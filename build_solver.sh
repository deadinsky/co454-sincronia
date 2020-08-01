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

echo --- Cleaning
rm -f *.jar
rm -f *.class
rm -fr gen-java

echo --- Compiling Java
$JAVA_CC -version
mv LocalJobClass/Job.java Job.java
$JAVA_CC BruteForceSolver.java CalculateSchedules.java Job.java -cp .:gen-java/:"lib/*"
mv Job.java LocalJobClass/

echo --- Done, now run your code.
echo     Examples:
echo $JAVA '-cp .:gen-java/:"lib/*" BruteForceSolver schedule.txt'
