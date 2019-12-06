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

if [ "$1" != "test" && "$2" != "test" ] ; then exit 0 ; fi

if [ ! -d "data" ] ; then mkdir data ; fi
if [ ! -d "lib" ] ; then mkdir lib ; fi
if [ ! -d "logs" ] ; then mkdir logs ; fi
if [ ! -d "target/classes" ] ; then mkdir -p target/classes ; fi
if [ ! -d "target/test-classes" ] ; then mkdir -p target/test-classes ; fi

mvn dependency:copy-dependencies -DoutputDirectory=./lib

export CLASSPATH=./target/classes:./target/test-classes\
:./lib/logback-classic-1.1.7.jar:./lib/logback-core-1.1.7.jar:./lib/slf4j-api-1.7.21.jar\
:./lib/sqlite-jdbc-3.28.0.jar:./lib/antlr-2.7.6.jar:./lib/commons-collections-3.1.jar\
:./lib/postgresql-42.2.5.jre7.jar:./lib/javassist-3.12.0.GA.jar:./lib/jta-1.1.jar\
:./lib/hibernate-jpa-2.0-api-1.0.1.Final.jar:./lib/junit-4.12.jar:./lib/hibernate-core-3.6.10.Final.jar\
:./lib/dom4j-1.6.1.jar:./lib/tomcat-juli-8.5.29.jar:./lib/hibernate-commons-annotations-3.2.0.Final.jar\
:./lib/tomcat-jdbc-8.5.29.jar

"$JAVA_HOME/bin/javac" -sourcepath src/main/java -d target/classes src/main/java/org/sqlite/server/*.java
"$JAVA_HOME/bin/javac" -sourcepath src/test/java -d target/test-classes src/test/java/org/sqlite/*.java
"$JAVA_HOME/bin/javac" -sourcepath src/test/java -d target/test-classes src/test/java/org/sqlite/server/jdbc/pg/*.java
cp -f ./src/main/resources/* ./target/classes/

"$JAVA_HOME/bin/java" -Xmx256m org.sqlite.TestAll
