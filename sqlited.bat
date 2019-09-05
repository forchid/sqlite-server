@echo off

setlocal

call %~dp0\runit.bat org.sqlite.server.SQLiteServer %*

endlocal