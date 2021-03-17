input="$COMPONENT/$1.txt"
output="$COMPONENT/$1.sql"
outputtext="INSERT INTO $1 VALUES "

rm $output

while IFS= read -r line
do
  count=0
  for word in $line
    do
    if [[ $count -eq 0 ]]
    then
      outputtext="$outputtext("
    else
    outputtext="$outputtext, "
    fi
    ((count=count+1))
    outputtext="$outputtext'$word'"
  done
  outputtext="$outputtext),"
done < "$input"
echo $outputtext > $output
sed -i '$ s/,$/;/' $output
