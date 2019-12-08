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
rem Sqlited script for the SQLite Server
rem ---------------------------------------------------------------------------
setlocal

rem Guess SQLITED_HOME if not defined
set "CURRENT_DIR=%cd%"
if not "%SQLITED_HOME%" == "" goto gotHome
set "SQLITED_HOME=%CURRENT_DIR%"
if exist "%SQLITED_HOME%\bin\sqlited.bat" goto okHome
cd ..
set "SQLITED_HOME=%cd%"
cd /d "%CURRENT_DIR%"
:gotHome
if exist "%SQLITED_HOME%\bin\sqlited.bat" goto okHome
echo The SQLITED_HOME environment variable is not defined correctly
echo This environment variable is needed to run this program
goto end

:okHome
set CLASSPATH=.;%SQLITED_HOME%\lib\*;%SQLITED_HOME%\conf
java %JAVA_OPTS% org.sqlite.server.SQLiteServer %*

:end

endlocal