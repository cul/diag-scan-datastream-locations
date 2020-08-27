import sys, os, re, sqlite3, fileinput, binascii
from itertools import takewhile
from pathlib import Path

INVENTORY_FILE_NAME = re.compile(r'([A-Za-z0-9_]+_fileInventory(_\d{1,2}){3})\.txt')

SIGN_CHECK_BIT = 1 << 63
SIGN_DIFF = 1 << 64

def usage():
	print("indexInventory.py PATH_OR_DIR")

def createOrUpdateInventory(conn, inventoryDir, date):
	cursor = conn.cursor()
	cursor.execute('''
		INSERT INTO inventories(path,date) VALUES(?,?)
  		ON CONFLICT(path) DO UPDATE SET date=excluded.date;
	''', (inventoryDir, date))
	cursor.execute("SELECT id FROM inventories where inventories.path=:path", { 'path' : inventoryDir })
	inventoryId = cursor.fetchone()
	# delete prior entries
	cursor.execute('''
		DELETE FROM entries WHERE entries.inventoryId=?
	''', (inventoryId))
	conn.commit()
	return inventoryId[0]

def indexInventory(conn, inventoryPath):
	fileName = os.path.basename(inventoryPath)
	m = INVENTORY_FILE_NAME.match(fileName)
	if m:
		segments = m[1].split('_')
		inventoryDir = "/%s/" % '/'.join(segments[0:-4])
		date = '-'.join(segments[-3:])
		print("indexing {} as {} from {}".format(fileName, inventoryDir, date))
		inventoryId = createOrUpdateInventory(conn, inventoryDir, date)
		cursor = conn.cursor()
		for line in fileinput.input(inventoryPath):
			row = line.strip()
			# entries: id (auto), crcId, checksum[0..6], inventoryId, path
			checksum = row[0:32]
			# crc32 has collisions
			# calculating a signed int (max range of sqlite integer) from first 16 checksum hex characters
			# may still need to verify from sum in entries
			crc = int(row[0:16], 16)
			if crc & SIGN_CHECK_BIT:
				crc -= SIGN_DIFF
			# skip the \s\s\.\/ after checksum
			path = row[36:]
			values = (crc, checksum, inventoryId, path)
			cursor.execute('INSERT INTO entries(crcId,checksum,inventoryId,path) VALUES(?, ?, ?, ?)', values)
	else:
		print("skipping unexpected file name %s" % fileName, file=sys.stderr)


def inventoryPaths(pathOrDir):
	test = lambda x: INVENTORY_FILE_NAME.match(os.path.basename(str(x)))
	if os.path.isfile(pathOrDir):
		return takewhile(test, [pathOrDir])
	else:
		return takewhile(test, Path(pathOrDir).glob('*.txt'))

def ensureInventoryTable(cursor):
	cursor.execute('''SELECT name FROM sqlite_master WHERE type='table' AND name='inventories';''')
	if cursor.fetchone() == None:
		cursor.execute(
			'''
				CREATE TABLE inventories
				(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, path varchar(255) NOT NULL UNIQUE, date varchar(8) NOT NULL);
			'''
		)

def ensureCrcsTable(cursor):
	cursor.execute('''SELECT name FROM sqlite_master WHERE type='table' AND name='crcs';''')
	if cursor.fetchone() == None:
		cursor.executescript(
			'''
				CREATE TABLE crcs
				(id INTEGER PRIMARY KEY NOT NULL, count INTEGER default 1);
				CREATE INDEX [IFK_crc_count] ON "crcs" ([count]);
			'''
		)

def ensureEntriesTable(cursor):
	cursor.execute('''SELECT name FROM sqlite_master WHERE type='table' AND name='entries';''')
	if cursor.fetchone() == None:
		# entries: id (auto), crcId, checksum[0..6], inventory, path, index(crcId), fk(crcId)
		cursor.executescript(
			'''
				CREATE TABLE entries
				(
					id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, crcId INTEGER NOT NULL, checksum varchar(32) NOT NULL,
					inventoryId INTEGER NOT NULL, path varchar(255),
					FOREIGN KEY (crcId) REFERENCES crcs ([id]) ON DELETE NO ACTION ON UPDATE NO ACTION,
					FOREIGN KEY (inventoryId) REFERENCES inventories ([id]) ON DELETE NO ACTION ON UPDATE NO ACTION
				);
				CREATE INDEX IFK_crc_id ON entries([crcId]);
				CREATE INDEX IFK_inv_id ON entries([inventoryId]);
				CREATE TRIGGER inc_crc_count AFTER INSERT ON "entries" WHEN true
				BEGIN
					INSERT INTO crcs(id) VALUES(new.crcId) ON CONFLICT(id) DO UPDATE SET count=count+1;
				END;
				CREATE TRIGGER dec_crc_count AFTER DELETE ON "entries" 
				BEGIN
					UPDATE crcs SET count=count-1 WHERE crcs.id=old.crcId;
				END;
			'''
		)

def ensureIndex():
	conn = sqlite3.connect('file:db/inventory.db?mode=rwc', uri=True)
	cursor = conn.cursor()
	cursor.execute('PRAGMA recursive_triggers = ON;')
	ensureInventoryTable(cursor)
	ensureCrcsTable(cursor)
	ensureEntriesTable(cursor)
	conn.commit()
	return conn
if __name__ == "__main__":
	if len(sys.argv) < 2:
		print("no path or directory given to find inventory files", file=sys.stderr)
		usage()
		sys.exit(1)
	conn = ensureIndex()
	for inventoryPath in inventoryPaths(sys.argv[1]):
		print(inventoryPath)
		indexInventory(conn, inventoryPath)
	conn.commit()
	conn.close()