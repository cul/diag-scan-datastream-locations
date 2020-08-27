# diag-scan-datastream-locations
## bin/scan.py
Legacy FCR Object Scan for External Datastream Content Locations with a path fragment

from the root object store directory, foxml is at /ROOT/YYYY/MMDD/HH/MM/ESCAPED_PID

where escaped pid is a pid with the colon replaced by an underscore

root element:
```
<foxml:digitalObject VERSION="1.1" PID="nsprefix:123456789"
xmlns:foxml="info:fedora/fedora-system:def/foxml#"
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
xsi:schemaLocation="info:fedora/fedora-system:def/foxml# http://www.fedora.info/definitions/1/0/foxml1-
1.xsd">
```

the xpath for a relevant datastream node is:
```
 /foxml:digitalObject/foxml:datastream[@CONTROL_GROUP='E']/foxml:datastreamVersion/foxml:contentLocation[contains(@REF, PATH_FRAGMENT)]
```

for each matching contentLocation node:
 @REF is the canonical path
 foxml:datastreamVersion@CREATED is the timestamp (parent['CREATED'])
 foxml:datastream@ID is the datastream ID that should be updated (parent.parent['ID'])
 foxml:digitalObject@PID is the digital object ID (parent.parent.parent['PID'])

we want to identify:
PID, DSID, PATH, TIMESTAMP for the latest/current timestamp if the current datastreamVersion is in fstore

scanning script will pipe output from `grep -r -l PATH_FRAGMENT OBJ_STORE_DIR | python bin/scan.py PATH_FRAGMENT` and send CSV data on matches to stdout, which can be sent to files as client chooses.
## bin/indexInventory.py
This script builds a sqlite db from md5 inventory files. The files are expected to be formatted:
- with content that is a checksum and a relative path to the inventory root separated by two whitespaces
- with a file name that is the inventory root, with / characters replaced by underscores, and ending in `_fileInventory_MM_DD_YY.txt`

To try to accelerate queries after the database is built, a CRC (integer from the first 8 bytes of the hex-decoded checksum) is calculated and counts of its occurence are updated with database triggers. The database is stored at `db/inventory.db`.

## bin/reportInventory.py
This script reports out on the sqlite database at `db/inventory.db`. It counts CRCs occuring more than once, and prints a CSV of the associated files and checksums to `duplicates.csv`. Because the CRC is not as unique as the checksum, it's possible that there are rare duplicates among CRCs for distinct checksums - but not likely.
