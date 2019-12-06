#!/bin/sh
if [ -z "$JAVA_HOME" ] ; then
  if [[ "$OSTYPE" == "darwin"* ]]; then
    if [ -d "/System/Library/Frameworks/JavaVM.framework/Home" ] ; then
      export JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Home
    else
      export JAVA_HOME=`/usr/libexec/java_home`
    fi
  fi
fi
if [ -z "$JAVA_HOME" ] ; then
  echo "Error: JAVA_HOME is not defined."
fi

if [ "$1" = "clean" ] ; then rm -rf data lib logs target ; fi

if [ "$1" != "test" && "$2" != "test" ] ; then exit(0) ; fi

if [ ! -d "data" ] ; then mkdir data ; fi
if [ ! -d "lib" ] ; then mkdir lib ; fi
if [ ! -d "logs" ] ; then mkdir logs ; fi
if [ ! -d "target/classes" ] ; then mkdir -p target/classes ; fi
if [ ! -d "target/test-classes" ] ; then mkdir -p target/test-classes ; fi

mvn dependency:copy-dependencies -DoutputDirectory=./lib

export CLASSPATH=.:./target/classes:./target/test-classes\
:./lib/logback-classic-1.1.7.jar:./lib/logback-core-1.1.7.jar:./lib/slf4j-api-1.7.21.jar\
:./lib/sqlite-jdbc-3.28.0.jar

"$JAVA_HOME/bin/javac" -sourcepath src/main/java -d target/classes src/main/java/org/sqlite/server/*.java
"$JAVA_HOME/bin/javac" -sourcepath src/test/java -d target/test-classes src/test/java/org/sqlite/*.java
"$JAVA_HOME/bin/javac" -sourcepath src/test/java -d target/test-classes src/test/java/org/sqlite/server/jdbc/pg/*.java

"$JAVA_HOME/bin/java" -Xmx256m org.sqlite.TestAll
