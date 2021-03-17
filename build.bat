javac -d classes -classpath lib -sourcepath src src/qp/utils/*.java
javac -d classes -classpath lib -sourcepath src src/qp/parser/*.java
javac -d classes -classpath lib -sourcepath src src/qp/operators/*.java
javac -d classes -classpath lib -sourcepath src src/qp/optimizer/*.java

javac -d classes -classpath lib -sourcepath src src/QueryMain.java
javac -d classes -classpath lib -sourcepath src src/ConvertTxtToTbl.java 
javac -d classes -classpath lib -sourcepath src src/RandomDB.java 