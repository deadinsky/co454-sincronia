#!/bin/bash

# Original Author: Wojciech Golab, 2016-2019
# Modified By: Thomas Dedinsky

if [ -f /usr/lib/jvm/default-java/bin/javac ]; then
    JAVA_HOME=/usr/lib/jvm/default-java
elif [ -f /usr/lib/jvm/java-11-openjdk-amd64/bin/javac ]; then
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/
elif [ -f /usr/lib/jvm/java-8-openjdk-amd64/bin/javac ]; then
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
elif [ -f /usr/lib/jvm/java-openjdk/bin/javac ]; then
    JAVA_HOME=/usr/lib/jvm/java-openjdk
else
    echo "Unable to find java compiler"
    exit 1
fi

JAVA=$JAVA_HOME/bin/java
JAVA_CC=$JAVA_HOME/bin/javac
THRIFT_CC=/usr/local/bin/thrift

echo --- Cleaning
rm -f *.jar
rm -f *.class
rm -fr gen-java

echo --- Compiling Thrift IDL
$THRIFT_CC --version &> /dev/null
ret=$?
if [ $ret -ne 0 ]; then
    exit 1
fi
$THRIFT_CC --version
$THRIFT_CC --gen java:generated_annotations=suppress sincronia.thrift


echo --- Compiling Java
$JAVA_CC -version
$JAVA_CC gen-java/*.java -cp .:"lib/*"
$JAVA_CC *.java -cp .:gen-java/:"lib/*"

echo --- Done, now run your code.
echo     Examples:
echo $JAVA '-cp .:gen-java/:"lib/*" FENode 10123'
echo $JAVA '-cp .:gen-java/:"lib/*" BENode localhost 10123 10124'
echo $JAVA '-cp .:gen-java/:"lib/*" Client localhost 10123 schedule.txt'
