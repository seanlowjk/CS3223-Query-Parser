# bash run_tests.sh 10 query0.sql query0.out query0.txt 500 50

num_records=$1
sql_query_file=$2
parser_ouput_file=$3
psql_output_file=$4

buffer_size=$5
num_pages=$6

echo "==========================================="
echo "Step 0: Preprocess"
echo "==========================================="
touch AIRCRAFTS.sql CERTIFIED.sql EMPLOYEES.sql FLIGHTS.sql SCHEDULE.sql

echo "Step 1: Populate Tables"
echo "==========================================="
bash populate_table.sh AIRCRAFTS $num_records > /dev/null
bash populate_table.sh CERTIFIED $num_records > /dev/null
bash populate_table.sh EMPLOYEES $num_records > /dev/null
bash populate_table.sh FLIGHTS $num_records > /dev/null
bash populate_table.sh SCHEDULE $num_records > /dev/null

echo "Step 2: Populate SQL Records"
echo "==========================================="
bash psql_equiv.sh AIRCRAFTS > /dev/null
bash psql_equiv.sh CERTIFIED > /dev/null
bash psql_equiv.sh EMPLOYEES > /dev/null
bash psql_equiv.sh FLIGHTS > /dev/null
bash psql_equiv.sh SCHEDULE > /dev/null

echo "Step 3: Insert VALUES into PostgreSQL"
echo "==========================================="
psql -f DATABASE.sql > /dev/null
psql -f AIRCRAFTS.sql > /dev/null
psql -f CERTIFIED.sql > /dev/null
psql -f EMPLOYEES.sql > /dev/null
psql -f FLIGHTS.sql > /dev/null
psql -f SCHEDULE.sql > /dev/null

echo "Step 4: Run psql query first"
echo "==========================================="
bash run_psql_query.sh $sql_query_file $psql_output_file

echo "Step 5: Run java QueryMain"
echo "==========================================="
java QueryMain $sql_query_file $parser_ouput_file $buffer_size $num_pages 1
tail -n +2 "$parser_ouput_file" > "$parser_ouput_file.tmp" && mv "$parser_ouput_file.tmp" "$parser_ouput_file"

echo "==========================================="
echo "Step 6: Check Diff"
echo "==========================================="
diff $parser_ouput_file $psql_output_file

rm AIRCRAFTS.sql CERTIFIED.sql EMPLOYEES.sql FLIGHTS.sql SCHEDULE.sql
