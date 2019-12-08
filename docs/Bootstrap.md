# Bootstrap SQLite Server
Before bootstrap a SQLite server, need to initialize the data directory of the server instance.
An example, initialize a "data" directory, and creating a "test" database in this directory:
```shell
$./bin/initdb.sh -D ./data -p 123456 -d test
```
Then do bootstrap the SQLite Server instance:
```shell
./bin/startup.sh -D ./data &
```
Issue "psql" command, connect to the SQLite server:
```shell
$psql -U root -h localhost -p 3272 test

```

## initdb options
```shell
$./bin/initdb.sh --help
Usage: java org.sqlite.server.SQLiteServer [OPTIONS]
  --help|-h|-?                  Show this message
  --data-dir|-D   <path>        SQLite server data dir, default sqlite3Data in user home
  --user|-U       <user>        Superuser's name, default root
  --password|-p   <password>    Superuser's password, must be provided in non-trust auth
  --db|-d         <dbName>      Initialized database, default as the user name
  --host|-H       <host>        Superuser's login host, IP, or '%', default localhost
  --trace|-T                    Trace SQLite server execution
  --trace-error                 Trace error information of SQLite server execution
  --journal-mode  <mode>        SQLite journal mode, default WAL
  --synchronous|-S<sync>        SQLite synchronous mode, default NORMAL
  --protocol      <pg>          SQLite server protocol, default pg
  --auth-method|-A<authMethod>  Available auth methods(md5, password and trust authentication), default 'md5'
```

## boot options
```shell
$./bin/startup.sh --help
Usage: java org.sqlite.server.SQLiteServer [OPTIONS]
  --help|-h|-?                  Show this message
  --data-dir|-D   <path>        SQLite server data dir, default sqlite3Data in user home
  --host|-H       <host>        SQLite server listen host or IP, default localhost
  --port|-P       <number>      SQLite server listen port, default 3272
  --max-conns     <number>      Max client connections limit, default 50 per SQLite server worker
  --worker-count  <number>      SQLite worker number, default CPU cores and max 128
  --trace|-T                    Trace SQLite server execution
  --trace-error                 Trace error information of SQLite server execution
  --busy-timeout  <millis>      SQL statement busy timeout, default 50000ms
  --journal-mode  <mode>        SQLite journal mode, default WAL
  --synchronous|-S<sync>        SQLite synchronous mode, default NORMAL
  --protocol      <pg>          SQLite server protocol, default pg
```

