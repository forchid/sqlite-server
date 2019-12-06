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

if [ "$SQLITED_HOME" = "" ] ; then
    BIN_DIR=`dirname "$PRG"`
    export SQLITED_HOME=`dirname "$BIN_DIR"`
fi
if [ -z "$SQLITED_HOME" ] ; then
  echo "Error: SQLITED_HOME is not defined."
fi

if [ "$1" = "clean" ] ; then 
  rm -rf "$SQLITED_HOME/temp" "$SQLITED_HOME/lib" "$SQLITED_HOME/logs" "$SQLITED_HOME/target"
fi

if [ "$1" != "test" && "$2" != "test" ] ; then exit 0 ; fi

if [ ! -d "$SQLITED_HOME/temp" ] ; then mkdir "$SQLITED_HOME/temp" ; fi
if [ ! -d "$SQLITED_HOME/lib" ] ; then mkdir "$SQLITED_HOME/lib" ; fi
if [ ! -d "$SQLITED_HOME/logs" ] ; then mkdir "$SQLITED_HOME/logs" ; fi
if [ ! -d "$SQLITED_HOME/target/classes" ] ; then mkdir -p "$SQLITED_HOME/target/classes" ; fi
if [ ! -d "$SQLITED_HOME/target/test-classes" ] ; then mkdir -p "$SQLITED_HOME/target/test-classes" ; fi

mvn dependency:copy-dependencies -DoutputDirectory="$SQLITED_HOME/lib"

export CLASSPATH=$SQLITED_HOME/conf:$SQLITED_HOME/target/classes:$SQLITED_HOME/target/test-classes\
:$SQLITED_HOME/lib/logback-classic-1.1.7.jar:$SQLITED_HOME/lib/logback-core-1.1.7.jar:$SQLITED_HOME/lib/slf4j-api-1.7.21.jar\
:$SQLITED_HOME/lib/sqlite-jdbc-3.28.0.jar:$SQLITED_HOME/lib/antlr-2.7.6.jar:$SQLITED_HOME/lib/commons-collections-3.1.jar\
:$SQLITED_HOME/lib/postgresql-42.2.5.jre7.jar:$SQLITED_HOME/lib/javassist-3.12.0.GA.jar:$SQLITED_HOME/lib/jta-1.1.jar\
:$SQLITED_HOME/lib/hibernate-jpa-2.0-api-1.0.1.Final.jar:$SQLITED_HOME/lib/junit-4.12.jar:$SQLITED_HOME/lib/hibernate-core-3.6.10.Final.jar\
:$SQLITED_HOME/lib/dom4j-1.6.1.jar:$SQLITED_HOME/lib/tomcat-juli-8.5.29.jar:$SQLITED_HOME/lib/hibernate-commons-annotations-3.2.0.Final.jar\
:$SQLITED_HOME/lib/tomcat-jdbc-8.5.29.jar

"$JAVA_HOME/bin/javac" -sourcepath "$SQLITED_HOME/src/main/java" -d "$SQLITED_HOME/target/classes" "$SQLITED_HOME/src/main/java/org/sqlite/server/*.java"
"$JAVA_HOME/bin/javac" -sourcepath "$SQLITED_HOME/src/test/java" -d "$SQLITED_HOME/target/test-classes" "$SQLITED_HOME/src/test/java/org/sqlite/*.java"
"$JAVA_HOME/bin/javac" -sourcepath "$SQLITED_HOME/src/test/java" -d "$SQLITED_HOME/target/test-classes" "$SQLITED_HOME/src/test/java/org/sqlite/server/jdbc/pg/*.java"
cp -f "$SQLITED_HOME/src/main/resources/*" "$SQLITED_HOME/target/classes/"

"$JAVA_HOME/bin/java" -Xmx256m org.sqlite.TestAll
