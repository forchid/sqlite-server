@echo off

setlocal

set BINDIR=%~dp0
set CLASSPATH=%BINDIR%\target\classes;^
%BINDIR%\target\test-classes;^
%BINDIR%\lib\*;

rem mvn dependency:copy-dependencies -DoutputDirectory="%BINDIR%\lib"

java %*

endlocal