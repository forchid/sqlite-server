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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Deque;

import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteConnection;
import org.sqlite.SQLiteConfig.Encoding;
import org.sqlite.SQLiteConfig.JournalMode;
import org.sqlite.server.util.ConvertUtils;
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
            + "host varchar(60) not null,"
            + "user varchar(32) not null,"
            + "password varchar(50),"
            + "protocol varchar(50) not null,"
            + "auth_method varchar(50) not null,"
            + "sa integer not null,"
            + "primary key(host, user, protocol))";
    
    protected static final String INSERT_USER =
            "insert into user(host, user, password, protocol, auth_method, sa)"
            + "values(?, ?, ?, ?, ?, ?)";
    
    protected static final String CREATE_TABLE_DB =
            "create table if not exits db("
            + "host varchar(60) not null,"
            + "db varchar(250) not null,"
            + "user varchara(32) not null,"
            + "primary key(host, db, user))";
    
    protected static final String INSERT_DB =
            "insert into db(host, db, user) values(?, ?, ?)";
    
    protected final Deque<SQLiteConnection> connPool;
    protected final int maxPoolSize;
    protected int poolSize;
    
    protected final File file;
    
    public SQLiteMetadb(File metaFile, int maxPoolSize) {
        this.file = metaFile;
        this.connPool = new ArrayDeque<>(maxPoolSize);
        this.maxPoolSize = maxPoolSize;
    }
    
    public SQLiteMetadb(File metaFile) {
        this(metaFile, 4);
    }
    
    public boolean isInitialized() {
        if (this.file.exists() && this.file.isFile()) {
            try {
                int n = selectUserCount();
                return (n > 0);
            } catch (SQLException e) {
                String error = "Access " + this.file.getName() + " fatal";
                throw new IllegalStateException(error, e);
            }
        }
        
        return false;
    }
    
    public SQLiteConnection getConnection() throws SQLException {
        synchronized(this.connPool) {
            for (;;) {
                SQLiteConnection conn = this.connPool.poll();
                if (conn != null) {
                    return conn;
                }
                if (this.poolSize >= this.maxPoolSize) {
                    try {
                        this.connPool.wait();
                    } catch (InterruptedException e) {}
                    continue;
                }
                
                // Do connect
                boolean success = false;
                try {
                    SQLiteConfig config = new SQLiteConfig();
                    config.setJournalMode(JournalMode.DELETE);
                    config.setBusyTimeout(50000);
                    config.setEncoding(Encoding.UTF8);
                    conn = (SQLiteConnection)config.createConnection("jdbc:sqlite:"+this.file);
                    success = true;
                    return conn;
                } finally {
                    if (success) {
                        ++this.poolSize;
                    }
                }
                
            }
        }
    }
    
    public void release(SQLiteConnection conn, boolean close) {
        if (conn == null) {
            return;
        }
        if (close) {
            IoUtils.close(conn);
        }
        
        synchronized(this.connPool) {
            try {
                boolean closed = conn.isClosed();
                if (closed) {
                    --this.poolSize;
                    return;
                }
            } catch (SQLException e) {
                IoUtils.close(conn);
                --this.poolSize;
                return;
            }
            
            if (this.connPool.offer(conn)) {
                this.connPool.notifyAll();
                return;
            }
            // full? close it
        }
        
        IoUtils.close(conn);
        throw new IllegalStateException("Connection pool full");
    }
    
    public int getPoolSize() {
        synchronized(this.connPool) {
            return this.poolSize;
        }
    }
    
    public int getActiveCount() {
        synchronized(this.connPool) {
            return (this.poolSize - this.connPool.size());
        }
    }
    
    public int getMaxPoolSize() {
        return this.maxPoolSize;
    }

    @Override
    public void close() {
        synchronized(this.connPool) {
            for (;;) {
                SQLiteConnection conn = this.connPool.poll();
                if (conn == null) {
                    break;
                }
                IoUtils.close(conn);
                --this.poolSize;
            }
        }
    }
    
    public SQLiteUser createUser(SQLiteUser user) throws SQLException {
        String opass = user.getPassword();
        if (opass != null) {
            byte[] res = MD5Utils.encode(opass + user.getUser());
            user.setPassword(ConvertUtils.bytesToHexString(res));
        }
        
        SQLiteConnection conn = getConnection();
        boolean failed = true;
        try {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(CREATE_TABLE_USER);
                
                try (PreparedStatement ps = conn.prepareStatement(INSERT_USER)) {
                    int i = 0;
                    ps.setString(++i, user.getHost());
                    ps.setString(++i, user.getUser());
                    ps.setString(++i, user.getPassword());
                    ps.setString(++i, user.getProtocol());
                    ps.setString(++i, user.getAuthMethod());
                    ps.setInt(++i, user.getSa());
                    ps.executeUpdate();
                }
                
                if (user.getDb() != null) {
                    SQLiteDb db = new SQLiteDb(user.getUser(), user.getHost(), user.getDb());
                    createDb(conn, stmt, db);
                }
                
                conn.commit();
                failed = false;
                return user;
            } finally {
                if (failed) {
                    user.setPassword(opass);
                    conn.rollback();
                    failed = false;
                }
            }
        } finally {
            release(conn, failed);
        }
    }
    
    public SQLiteDb createDb(SQLiteDb db) throws SQLException {
        SQLiteConnection conn = getConnection();
        boolean failed = true;
        try {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                createDb(conn, stmt, db);
                conn.commit();
                failed = false;
                return db;
            } finally {
                if (failed) {
                    conn.rollback();
                    failed = false;
                }
            }
        } finally {
            release(conn, failed);
        }
    }
    
    protected SQLiteDb createDb(SQLiteConnection conn, Statement stmt, SQLiteDb db) 
            throws SQLException {
        stmt.executeUpdate(CREATE_TABLE_DB);
        try (PreparedStatement ps = conn.prepareStatement(INSERT_DB)) {
            int i = 0;
            ps.setString(++i, db.getHost());
            ps.setString(++i, db.getDb());
            ps.setString(++i, db.getUser());
            ps.executeUpdate();
        }
        
        return db;
    }
    
    public int selectUserCount() throws SQLException {
        SQLiteConnection conn = getConnection();
        boolean failed = true;
        try {
            int n = 0;
            try (Statement stmt = conn.createStatement()) {
                String sql = 
                        "select * from sqlite_master "
                        + "where type = 'table' and name = 'user'";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    if (!rs.next()) {
                        failed = false;
                        return 0;
                    }
                }
                
                sql = "select count(*) from user";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    if (rs.next()) {
                        n = rs.getInt(1);
                    }
                }
                
                failed = false;
                return n;
            }
        } finally {
            release(conn, failed);
        }
    }
    
    public int selectUserCount(String host, String ip, String protocol) throws SQLException {
        SQLiteConnection conn = getConnection();
        boolean failed = true;
        try {
            String sql = 
                    "select count(*) from user u "
                    + "where (? like u.host or ? like u.host) "
                    + "and u.protocol = ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int i = 0, n = 0;
                ps.setString(++i, host);
                ps.setString(++i, ip);
                ps.setString(++i, protocol);
                
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        n = rs.getInt(1);
                    }
                }
                
                failed = false;
                return n;
            }
        } finally {
            release(conn, failed);
        }
    }
    
    public SQLiteUser selectUser(String host, String ip, String protocol, String user, String db) 
            throws SQLException {
        SQLiteUser sqliteUser = null;
        SQLiteConnection conn = getConnection();
        boolean failed = true;
        try {
            String sql = 
                    "select host, user, password, protocol, auth_method, sa from user u "
                    + "where (? like u.host or ? like u.host) "
                    + "and u.protocol = ? and u.user = ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int i = 0;
                ps.setString(++i, host);
                ps.setString(++i, ip);
                ps.setString(++i, protocol);
                ps.setString(++i, user);
                
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        failed = false;
                        return null;
                    }
                    i = 0;
                    sqliteUser = new SQLiteUser();
                    sqliteUser.setHost(rs.getString(++i));
                    sqliteUser.setUser(rs.getString(++i));
                    sqliteUser.setPassword(rs.getString(++i));
                    sqliteUser.setProtocol(rs.getString(++i));
                    sqliteUser.setAuthMethod(rs.getString(++i));
                    sqliteUser.setSa(rs.getInt(++i));
                }
            }
            if (sqliteUser.getSa() == SQLiteUser.SUPER) {
                failed = false;
                return sqliteUser;
            }
            
            // check DB
            sql = "select d.host, d.user, d.db from db d "
                    + "where d.host = ? and d.user = ? and d.db = ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int i = 0;
                ps.setString(++i, host);
                ps.setString(++i, user);
                ps.setString(++i, db);
                
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        failed = false;
                        return null;
                    }
                }
            }
            sqliteUser.setDb(db);
            failed = false;
            return sqliteUser;
        } finally {
            release(conn, failed);
        }
    }
    
}
