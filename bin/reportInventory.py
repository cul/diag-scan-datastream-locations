import sys, os, sqlite3, csv

conn = sqlite3.connect('file:db/inventory.db?mode=ro', uri=True)
cursor = conn.cursor()
with open('duplicates.csv', 'w', newline='') as csvfile:
	writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
	writer.writerow(['checksum','inv_root','path','count','crc'])
	for row in cursor.execute('''
		SELECT entries.checksum, inventories.path, entries.path, crcs.count, crcs.id
		FROM entries INNER JOIN crcs ON crcs.id = entries.crcId
		INNER JOIN inventories ON inventories.id = entries.inventoryId
		WHERE crcs.count > 1
		ORDER BY crcs.count DESC
		'''):
		writer.writerow(row)

