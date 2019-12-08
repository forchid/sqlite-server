#!/bin/sh

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# -----------------------------------------------------------------------------
# Build Script for the SQLite Server
# -----------------------------------------------------------------------------

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

BIN_DIR=`dirname "$PRG"`
export SQLITED_HOME=`dirname "$BIN_DIR"`
export SQLITED_HOME=`readlink -f "$SQLITED_HOME"`

CLEAN_ARG=
TEST_ARG=
JAR_ARG=
for i in "$@"
do
  if [ "$i" = "jar" ] ; then export JAR_ARG="$i" ; fi
  if [ "$i" = "test" ] ; then export TEST_ARG="$i" ; fi
  if [ "$i" = "clean" ] ; then export CLEAN_ARG="$i" ; fi
done

if [ "$CLEAN_ARG" = "clean" ] ; then
  echo "clean: remove temp, test-lib, logs and target directories"
  rm -rf "$SQLITED_HOME/temp" "$SQLITED_HOME/test-lib" "$SQLITED_HOME/logs" "$SQLITED_HOME/target"
fi

if [ "$TEST_ARG" = "test" ] ; then
  echo "Test: run all test cases"
  rm -rf "$SQLITED_HOME/temp"
  if [ ! -d "$SQLITED_HOME/temp" ] ; then mkdir "$SQLITED_HOME/temp" ; fi
  if [ ! -d "$SQLITED_HOME/test-lib" ] ; then mkdir "$SQLITED_HOME/test-lib" ; fi
  if [ ! -d "$SQLITED_HOME/logs" ] ; then mkdir "$SQLITED_HOME/logs" ; fi
  if [ ! -d "$SQLITED_HOME/target" ] ; then mkdir "$SQLITED_HOME/target" ; fi
  
  mvn -f "$SQLITED_HOME"/pom.xml package -Dmaven.test.skip=true
  mvn -f "$SQLITED_HOME"/pom.xml dependency:copy-dependencies -DincludeScope=compile -DoutputDirectory="$SQLITED_HOME"/lib
  cp "$SQLITED_HOME"/target/*.jar "$SQLITED_HOME"/lib
  "$SQLITED_HOME"/bin/initdb.sh -D "$SQLITED_HOME"/temp -p 123456 -d test
  if [ $? != 0 ] ; then
    echo "Test initdb failed!"
    exit 1
  fi
  echo "Test initdb ok"
  
  mvn -f "$SQLITED_HOME"/pom.xml compile test-compile
  mvn -f "$SQLITED_HOME"/pom.xml dependency:copy-dependencies -DoutputDirectory="$SQLITED_HOME"/test-lib
  CLASSPATH="$SQLITED_HOME"/target/classes:"$SQLITED_HOME"/target/test-classes
  for jar in "$SQLITED_HOME"/test-lib/*.jar ; do
    CLASSPATH=$CLASSPATH:$jar
  done
  java -Xmx256m -classpath "$CLASSPATH" -DSQLITED_HOME="$SQLITED_HOME" org.sqlite.TestAll
  if [ $? != 0 ] ; then
    echo "Test all test cases failed!"
    exit 1
  fi
  echo "Test all test cases ok"
fi

if [ "$JAR_ARG" != "" ] ; then
  echo "jar: package sqlite server"
  if [ -d "$SQLITED_HOME/lib" ] ; then rm -rf "$SQLITED_HOME"/lib ; fi
  if [ ! -d "$SQLITED_HOME/lib" ] ; then mkdir "$SQLITED_HOME"/lib ; fi
  mvn -f "$SQLITED_HOME"/pom.xml package -Dmaven.test.skip=true
  mvn -f "$SQLITED_HOME"/pom.xml dependency:copy-dependencies -DincludeScope=compile -DoutputDirectory="$SQLITED_HOME"/lib
  cp "$SQLITED_HOME"/target/*.jar "$SQLITED_HOME"/lib
  if [ $? != 0 ] ; then
    echo "Package sqlite server failed!"
    exit 1
  fi
  echo "Package sqlite server ok"
fi
