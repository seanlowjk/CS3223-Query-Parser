source queryenv 

cd $COMPONENT/src/qp/parser
java java_cup.Main parser.cup

cd $COMPONENT/lib/JLex/
javac Main.java

cd $COMPONENT/src/qp/parser
java JLex.Main scaner.lex

rm Scaner.java 
cp scaner.lex.java Scaner.java 
rm scaner.lex.java 
sed -i 's/Yylex/Scaner/g' Scaner.java 

cd $COMPONENT 
