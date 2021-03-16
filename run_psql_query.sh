input_file=$1
output_file=$2
psql -f $1 -t | sed 's/|/ /g' | sed 's/ \+ /\t/g' | sed 's/^\t//g' | sed 's/^ //g' | sed 's/$/\t/g' | sed '$ d' > $2
