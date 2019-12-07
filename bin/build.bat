@echo off

rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem
rem     http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.

rem ---------------------------------------------------------------------------
rem Build script for the SQLite Server
rem ---------------------------------------------------------------------------
setlocal

set CURDIR=%CD%

if not exist "%JAVA_HOME%" (
  echo "Error: JAVA_HOME is not defined."
  goto end
)

if "%SQLITED_HOME%" == "" (
    cd /d %~dp0
    cd ..
    set SQLITED_HOME=%CD%
    cd /d "%CURDIR%"
)
if not exist "%SQLITED_HOME%" (
  echo "Error: SQLITED_HOME is not defined."
  goto end
)

set JAR_ARG=
set TEST_ARG=
set CLEAN_ARG=
:arg_loop
if "%~1" == "jar" (
  set "JAR_ARG=%~1"
  shift
)
if "%~1" == "test" (
  set "TEST_ARG=%~1"
  shift
)
if "%~1" == "clean" (
  set "CLEAN_ARG=%~1"
)
shift
if not "%~1" == "" (
  goto arg_loop
)

if not "%CLEAN_ARG%" == "" (
  echo clean: remove temp, lib, logs and target directories
  del /S /Q "%SQLITED_HOME%\temp" "%SQLITED_HOME%\lib" "%SQLITED_HOME%\logs" "%SQLITED_HOME%\target"
)

if not exist "%SQLITED_HOME%\temp" ( md "%SQLITED_HOME%\temp" )
if not exist "%SQLITED_HOME%\lib"  ( md "%SQLITED_HOME%\lib" )
if not exist "%SQLITED_HOME%\logs" ( md "%SQLITED_HOME%\logs" )
if not exist "%SQLITED_HOME%\target" ( md "%SQLITED_HOME%\target" )

if not "%TEST_ARG%" == "" (
  echo test: all test cases
  mvn compile test-compile
  mvn dependency:copy-dependencies -DoutputDirectory="%SQLITED_HOME%\lib"
  set CLASSPATH=%SQLITED_HOME%\target\classes;%SQLITED_HOME%\target\test-classes;%SQLITED_HOME%\lib\*
  java -Xmx256m org.sqlite.TestAll
)

if not "%JAR_ARG%" == "" (
  echo jar: package sqlite server
  if exist "%SQLITED_HOME%\lib" ( del /S /Q "%SQLITED_HOME%\lib" )
  if not exist "%SQLITED_HOME%\lib" md "%SQLITED_HOME%\lib"
  mvn package -Dmaven.test.skip=true
  mvn dependency:copy-dependencies -DincludeScope=compile -DoutputDirectory="%SQLITED_HOME%\lib"
  copy "%SQLITED_HOME%\target\sqlite-server-0.3.29.jar" "%SQLITED_HOME%\lib"
)

:end

endlocal
