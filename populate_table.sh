count=0

for var in "$@"
do
    if [[ $count -lt 1 ]]
    then 
        count=$var
    else 
        echo $var "table is populated"
        java RandomDB $var $count 
        java ConvertTxtToTbl $var
    fi 
done

if [[ $count -eq 0 ]]
then 
    echo "Please supply in a number of records greater than 0!"
fi
