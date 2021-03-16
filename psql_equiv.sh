input="$COMPONENT/$1.txt"
output="$COMPONENT/$1.sql"

rm $output

echo -n "INSERT INTO $1 VALUES " >> $output
while IFS= read -r line
do
  count=0
  for word in $line 
    do
    if [[ $count -eq 0 ]]
    then 
      echo -n "(" >> $output
    else 
      echo -n ", " >> $output
    fi 
    ((count=count+1))
    echo -n "'$word'" >> $output
  done 
  echo ")," >> $output
done < "$input"
sed -i '$ s/,$/;/' $output 
