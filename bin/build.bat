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
  echo Error: JAVA_HOME is not defined.
  exit /b 1
)

if "%SQLITED_HOME%" == "" (
    cd /d %~dp0
    cd ..
    set SQLITED_HOME=%CD%
    cd /d "%CURDIR%"
)
if not exist "%SQLITED_HOME%" (
  echo Error: SQLITED_HOME is not defined.
  exit /b 1
)

set JAR_ARG=
set TEST_ARG=
set CLEAN_ARG=
:setArgs
if "%~1" == "jar" (
  set "JAR_ARG=%~1"
)
if "%~1" == "test" (
  set "TEST_ARG=%~1"
)
if "%~1" == "clean" (
  set "CLEAN_ARG=%~1"
)
shift
if not "%~1" == "" (
  goto setArgs
)

if not "%CLEAN_ARG%" == "" goto clean
:execTest
if not "%TEST_ARG%" == "" goto test
:execJar
if not "%JAR_ARG%" == "" goto jar
:execEnd
goto end

:clean
echo Clean: remove temp, test-lib, logs and target directories
del /S /Q "%SQLITED_HOME%\temp" "%SQLITED_HOME%\test-lib" "%SQLITED_HOME%\logs" "%SQLITED_HOME%\target"
goto execTest

:test
echo Test: run all test cases
del /S /Q "%SQLITED_HOME%\temp"
if not exist "%SQLITED_HOME%\temp" md "%SQLITED_HOME%\temp"
if not exist "%SQLITED_HOME%\test-lib" md "%SQLITED_HOME%\test-lib"
if not exist "%SQLITED_HOME%\logs" md "%SQLITED_HOME%\logs"
if not exist "%SQLITED_HOME%\target" md "%SQLITED_HOME%\target"
call mvn compile test-compile
call mvn dependency:copy-dependencies -DoutputDirectory="%SQLITED_HOME%\test-lib"
set CLASSPATH=%SQLITED_HOME%\target\classes;%SQLITED_HOME%\target\test-classes;%SQLITED_HOME%\test-lib\*
call "%SQLITED_HOME%\bin\initdb.bat" -D "%SQLITED_HOME%\temp" -p 123456 -d test
if %ERRORLEVEL% NEQ 0 exit /b 1
echo Test initdb ok
call java -Xmx256m org.sqlite.TestAll
if %ERRORLEVEL% NEQ 0 exit /b 1
echo Test all ok
goto execJar

:jar
echo Jar: package sqlite server
if exist "%SQLITED_HOME%\lib" del /S /Q "%SQLITED_HOME%\lib"
if not exist "%SQLITED_HOME%\lib" md "%SQLITED_HOME%\lib"
call mvn package -Dmaven.test.skip=true
call mvn dependency:copy-dependencies -DincludeScope=compile -DoutputDirectory="%SQLITED_HOME%\lib"
xcopy "%SQLITED_HOME%"\target\*.jar "%SQLITED_HOME%\lib"
goto execEnd

:end
endlocal
