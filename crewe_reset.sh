#!/bin/sh
tiploc=CREWE
sql=crewe.sql
csv=docker/CREWE.csv
json=docker/static/train_service.json
db=DFROC2M/DFROC2M.db

echo "Clearing Log"
rm log.log
echo "Clearing cif_record.db..."
python3 cif_record.py
echo "Getting all CIF..."
python3 get_cif.py -r
echo "Removing main folders..."
rm -rf DF*
echo "Process CIF(s)"
python3 cif_extract.py -s
echo "Create today's schedules"
python3 today.py -X
echo "Create Line Up SQL"
python3 line_up.py -t $tiploc -f $sql
echo "Create CSV"
sqlite3  $db < $sql -separator ',' > $csv
echo "Create JSON"
python3 create_json.py -t $tiploc -o $json -d $db
echo "Start Docker"
cd docker
python3 docker.py
