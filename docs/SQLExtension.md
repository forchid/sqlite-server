# SQLite SQL Extension
On the basis of SQLite, add some SQL statements and functions for administrative purposes, includes
those statements of user, privilege and database management, connection management, schema management
and transaction management.

## User, privilege and database management
+ User management statements
```SQL
1. ALTER USER user@host [WITH] {SUPERUSER|NOSUPERUSER | IDENTIFIED BY 'password' 
| IDENTIFIED WITH PG [{MD5|PASSWORD|TRUST}]}
2. CREATE USER 'user'@'host' [WITH] SUPERUSER|NOSUPERUSER 
| IDENTIFIED BY 'password' | IDENTIFIED WITH PG {MD5|PASSWORD|TRUST}
3. DROP USER user@host \[IDENTIFIED WITH PG\] \[, user@host \[IDENTIFIED WITH PG\]...\]
4. "SHOW USERS [WHERE 'pattern']", only for superuser
```

+ Privilege management
```SQL
1. GRANT {ALL \[PRIVILEGES\] \[, SELECT | INSERT | UPDATE | DELETE | CREATE | ALTER | DROP | PRAGMA 
| VACUUM | ATTACH\]} ON [DATABASE | SCHEMA] dbname [, ...] TO {'user'@'host' [, ...]}
2. REVOKE {ALL \[PRIVILEGES\] \[, SELECT | INSERT | UPDATE | DELETE | CREATE | ALTER | DROP | PRAGMA 
| VACUUM | ATTACH\]} ON \[DATABASE | SCHEMA\] dbname \[, ...\] FROM 'user'@'host' \[, ...\]
3. SHOW GRANTS [FOR {'user'[@'host'|'%'] | CURRENT_USER[()]}]
```

+ Database management
```SQL
1. CREATE {DATABASE | SCHEMA} [IF NOT EXISTS] dbname [{LOCATION | DIRECTORY} 'data-dir']
2. "DROP {DATABASE | SCHEMA} [IF EXISTS] dbname", requires superuser privilege
3. SHOW [ALL] DATABASES
```

## Connection management
```SQL
1. SHOW [FULL] PROCESSLIST
2. KILL [connection | query] processor_id
```

## Schema management
```SQL
1. SHOW TABLES \[{FROM | IN} schema_name\] \[LIKE 'pattern'\]
2. SHOW [EXTENDED] {COLUMNS | FIELDS} {FROM | IN} [schema_name.]table_name [{FROM | IN} schema_name]
3. "SHOW {INDEX|INDEXES} [{FROM|IN} [schema_name.]table_name [{FROM|IN} schema_name]]|[WHERE 'pattern']", or
 "SHOW {INDEX|INDEXES} [EXTENDED] COLUMNS {FROM|IN} [schema_name.]index_name [{FROM|IN} schema_name]"
4. SHOW CREATE TABLE [schema_name.]tbl_name [{FROM | IN} schema_name]
5. SHOW CREATE INDEX [schema_name.]index_name [{FROM | IN} schema_name]
```

## Transaction management
```SQL
1. "SET {TRANSACTION | SESSION CHARACTERISTICS AS TRANSACTION} transaction_mode [, ...]", "transaction_mode" 
includes "read only", "read write"
2. "BEGIN [DEFERRED | IMMEDIATE | EXCLUSIVE] TRANSACTION transaction_mode", and 
"START TRANSACTION transaction_mode"
3. SET TRANSACTION transaction_mode
4. SELECT ... FROM ... FOR UPDATE
```

## Extension of function
```SQL
user(), current_user(), version(), server_version(), database(), current_database(), start_time(), sysdate(),
clock_timestamp(), sleep(N)
```

## Extension of DML
```SQL
1. "INSERT RETURNING" statement of PostgreSQL style: "INSERT INTO... {VALUES()... | SELECT ...} RETURNING ..."
2. TRUNCATE \[TABLE\] \[schema_name.\]tbl_name
```
