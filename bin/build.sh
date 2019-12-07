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
  exit 1
fi

if [ "$SQLITED_HOME" = "" ] ; then
    BIN_DIR=`dirname "$PRG"`
    export SQLITED_HOME=`dirname "$BIN_DIR"`
fi
if [ -z "$SQLITED_HOME" ] ; then
  echo "Error: SQLITED_HOME is not defined."
  exit 1
fi

for i in "$@"
do
  if [ "$i" = "clean" ] ; then
    echo "clean: remove temp, lib, logs and target directories"
    rm -rf "$SQLITED_HOME/temp" "$SQLITED_HOME/lib" "$SQLITED_HOME/logs" "$SQLITED_HOME/target"
  fi
done
if [ ! -d "$SQLITED_HOME/temp" ] ; then mkdir "$SQLITED_HOME/temp" ; fi
if [ ! -d "$SQLITED_HOME/lib" ] ; then mkdir "$SQLITED_HOME/lib" ; fi
if [ ! -d "$SQLITED_HOME/logs" ] ; then mkdir "$SQLITED_HOME/logs" ; fi
if [ ! -d "$SQLITED_HOME/target" ] ; then mkdir "$SQLITED_HOME/target" ; fi

mvn compile

for i in "$@"
do
  if [ "$i" = "test" ] ; then
    mvn dependency:copy-dependencies -DoutputDirectory="$SQLITED_HOME"/lib
    export CLASSPATH="$SQLITED_HOME"/target/classes:"$SQLITED_HOME"/target/test-classes
    java -Xmx128m -Djava.ext.dirs="$SQLITED_HOME"/lib org.sqlite.TestAll
  fi
done
