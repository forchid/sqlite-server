# SQLite Server
A [SQLite](https://www.sqlite.org/index.html) server based on the client/server architecture and org.xerial [sqlite-jdbc](https://github.com/xerial/sqlite-jdbc) project.
+ Implement a subset(PgServer) of [postgreSQL frontend/backend protocol](https://www.postgresql.org/docs/8.2/protocol.html) for supporting [pgjdbc](https://github.com/pgjdbc/pgjdbc) , psql, or ODBC
+ Support md5(default) and password authentication method in PgServer
+ High performance(insert 50,000 ~ 100,000+ rows per second in [wal & normal](https://www.sqlite.org/pragma.html#pragma_journal_mode) mode)

# Examples
1. Standalone SQLite server

Console 1 Start SQLite server
```shell
$java -Xmx128m org.sqlite.server.SQLiteServer boot -p 123456
2019-09-03 20:30:16.703 [SQLite server 0.3.27] INFO  SQLiteServer - Ready for connections on localhost:3272
```
Console 2 Connect to SQLite server then execute query
```shell
$psql -U root -p 3272 test.db
The user root's password:
psql (11.0, Server 8.2.23)
Input "help" for help information.

test.db=> \timing on
Timing on
test.db=> select count(*) from accounts;
 count(*)
----------
 32011001
(Rows 1)


Time: 338.081 ms
test.db=> select balance, count(*) from accounts where balance > 1000 group by balance limit 2;
 balance | count(*)
---------+----------
    1001 |      321
    1002 |      321
(Rows 2)


Time: 9592.378 ms (00:09.592)
test.db=>
```

2. Embedded SQLite server
```java
SQLiteServer server = new SQLiteServer();
server.bootAsync("boot", "-p", "123456");
```
