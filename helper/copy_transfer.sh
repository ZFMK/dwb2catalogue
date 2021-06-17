#!/bin/bash

TARGET_DB='zfmk_coll_db'
DBUSER='root'
NOW=$(date +%Y%m%d%H)
LOG_FILE="backup-db_$NOW.log"
LOCAL_PATH="/tmp"
DEFAULTS_EXTRA_FILE="/home/bquast/.my.cnf"

echo "Copy last transfer-DB from Gaia to local database, renaming to ${TARGET_DB} "


echo "Get database for transfer:"
result="$(mysql --defaults-extra-file=$DEFAULTS_EXTRA_FILE -Bse "SELECT SCHEMA_NAME AS DB FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE 'Transfer2ZFMKColl_%' ORDER BY SCHEMA_NAME DESC LIMIT 0,1;" 2>&1 )"
if [ $? -ne 0 ]
then
	echo $result
	exit 1
else
	SOURCE_DB_NAME=$result
	unset result
fi

echo "Dumping datascheme of ${SOURCE_DB_NAME}:"
result=$(mysqldump --defaults-extra-file=$DEFAULTS_EXTRA_FILE --databases ${SOURCE_DB_NAME} --opt --max_allowed_packet=1G --quote-names --no-data --routines --no-tablespaces > ${LOCAL_PATH}/${SOURCE_DB_NAME}_SCHEME.sql 2>&1 )
if [ $? -ne 0 ]
then
	echo $result
	exit 1
else
	unset result
fi

echo "Dumping data of ${SOURCE_DB_NAME}:"
result=$(mysqldump --defaults-extra-file=$DEFAULTS_EXTRA_FILE --databases ${SOURCE_DB_NAME} --opt --max_allowed_packet=1G --quote-names --no-create-info --no-tablespaces > ${LOCAL_PATH}/${SOURCE_DB_NAME}_DATA.sql 2>&1 )
if [ $? -ne 0 ]
then
	echo $result
	exit 1
fi


echo "Replace ${SOURCE_DB_NAME} with ${TARGET_DB}:"
sed -i -e "s/${SOURCE_DB_NAME}/${TARGET_DB}/g" ${LOCAL_PATH}/${SOURCE_DB_NAME}_SCHEME.sql

sed -i -e "s/${SOURCE_DB_NAME}/${TARGET_DB}/g" ${LOCAL_PATH}/${SOURCE_DB_NAME}_DATA.sql

echo "Import data into local MYSQL system:"
mysql --defaults-extra-file=$DEFAULTS_EXTRA_FILE < ${LOCAL_PATH}/${SOURCE_DB_NAME}_SCHEME.sql
mysql --defaults-extra-file=$DEFAULTS_EXTRA_FILE < ${LOCAL_PATH}/${SOURCE_DB_NAME}_DATA.sql

rm ${LOCAL_PATH}/${SOURCE_DB_NAME}_SCHEME.sql ${LOCAL_PATH}/${SOURCE_DB_NAME}_DATA.sql

exit 0
