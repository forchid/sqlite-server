/**
 * Copyright 2019 little-pan. A SQLite server based on the C/S architecture.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.sqlite.server;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.sqlite.SQLiteConnection;
import org.sqlite.server.util.IoUtils;
import org.sqlite.server.util.MD5Utils;

/**SQLite server meta database for user and database management.
 * 
 * @author little-pan
 * @since 2019-09-08
 *
 */
public class SQLiteMetadb implements AutoCloseable {
    
    protected static final String CREATE_TABLE_USER = 
            "create table if not exists user("
            + "user varchar(32) not null,"
            + "host varchar(60) not null,"
            + "password varchar(50),"
            + "protocol varchar(50) not null,"
            + "auth_method varchar(50) not null,"
            + "sa integer not null,"
            + "primary key(user, host))";
    
    protected static final String INSERT_USER =
            "insert into user(user, password, host, protocol, auth_method, sa)"
            + "values(?, ?, ?, ?, ?, ?)";
    
    protected static final String CREATE_TABLE_DB =
            "create table if not exits db("
            + "user varchara(32) not null,"
            + "host varchar(60) not null,"
            + "db varchar(250) not null,"
            + "primary key(user, host, db))";
    
    protected static final String INSERT_DB =
            "insert into db(user, host, db) values(?, ?, ?)";
    
    protected final File file;
    protected final SQLiteConnection conn;
    
    public SQLiteMetadb(File metaFile, SQLiteConnection metaConn) {
        this.file = metaFile;
        this.conn = metaConn;
    }

    @Override
    public void close() {
        IoUtils.close(this.conn);
    }
    
    public SQLiteUser createUser(SQLiteUser user) throws SQLException {
        String opass = user.getPassword();
        if (opass != null) {
            String npass = MD5Utils.encode(opass + user.getUser());
            user.setPassword(npass);
        }
        
        synchronized (this.conn) {
            this.conn.setAutoCommit(false);
            boolean failed = true;
            try (Statement stmt = this.conn.createStatement()) {
                stmt.executeUpdate(CREATE_TABLE_USER);
                
                try (PreparedStatement ps = this.conn.prepareStatement(INSERT_USER)) {
                    int i = 0;
                    ps.setString(++i, user.getUser());
                    ps.setString(++i, user.getPassword());
                    ps.setString(++i, user.getHost());
                    ps.setString(++i, user.getProtocol());
                    ps.setString(++i, user.getAuthMethod());
                    ps.setInt(++i, user.getSa());
                    ps.executeUpdate();
                }
                
                if (user.getDb() != null) {
                    SQLiteDb db = new SQLiteDb(user.getUser(), user.getHost(), user.getDb());
                    createDb(stmt, db);
                }
                
                this.conn.commit();
                return user;
            } finally {
                if (failed) {
                    user.setPassword(opass);
                    this.conn.rollback();
                }
            }
        }
    }
    
    public SQLiteDb createDb(SQLiteDb db) throws SQLException {
        synchronized (this.conn) {
            this.conn.setAutoCommit(false);
            boolean failed = true;
            try (Statement stmt = this.conn.createStatement()) {
                
                createDb(stmt, db);
                
                this.conn.commit();
                failed = false;
                return db;
            } finally {
                if (failed) {
                    this.conn.rollback();
                }
            }
        }
    }
    
    protected SQLiteDb createDb(Statement stmt, SQLiteDb db) throws SQLException {
        stmt.executeUpdate(CREATE_TABLE_DB);
        try (PreparedStatement ps = this.conn.prepareStatement(INSERT_DB)) {
            int i = 0;
            ps.setString(++i, db.getUser());
            ps.setString(++i, db.getHost());
            ps.setString(++i, db.getDb());
            ps.executeUpdate();
        }
        
        return db;
    }

}
