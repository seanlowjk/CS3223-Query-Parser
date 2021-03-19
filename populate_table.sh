# Usage Notes: 
# bash populate_table.sh TABLENAME NUM_TUPLES 

java RandomDB $1 $2 
java ConvertTxtToTbl $1
echo $1 "table is populated"
