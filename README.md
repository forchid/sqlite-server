# SQLite Server
A SQLite server based on the client/server architecture and org.xerial [sqlite-jdbc](https://github.com/xerial/sqlite-jdbc) project
- Implement a subset of postgreSQL c/s protocol for supporting pgjdbc
- Support simple user/password authentication
- High performance(insert 30,000 ~ 50,000+ rows per second in wal & normal mode)
