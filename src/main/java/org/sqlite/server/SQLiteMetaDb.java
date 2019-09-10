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
import static java.lang.String.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteConnection;
import org.sqlite.server.meta.Db;
import org.sqlite.server.meta.User;
import org.sqlite.SQLiteConfig.Encoding;
import org.sqlite.SQLiteConfig.JournalMode;
import org.sqlite.util.IoUtils;

/**SQLite server meta database for user and database management.
 * 
 * @author little-pan
 * @since 2019-09-08
 *
 */
public class SQLiteMetaDb implements AutoCloseable {
    static final Logger log = LoggerFactory.getLogger(SQLiteMetaDb.class);
    
    protected static final String CREATE_TABLE_USER = 
            "create table if not exists user("
            + "host varchar(60) not null,"
            + "user varchar(32) not null,"
            + "password varchar(50),"
            + "protocol varchar(50) not null,"
            + "auth_method varchar(50) not null,"
            + "sa integer not null,"
            + "primary key(protocol, user, host))";
    
    protected static final String INSERT_USER =
            "insert into user(host, user, password, protocol, auth_method, sa)"
            + "values(?, ?, ?, ?, ?, ?)";
    
    protected static final String CREATE_TABLE_DB =
            "create table if not exists db("
            + "host varchar(60) not null,"
            + "db varchar(64) not null,"
            + "user varchara(32) not null,"
            + "primary key(host, db, user))";
    
    protected static final String INSERT_DB =
            "insert into db(host, db, user) values(?, ?, ?)";
    
    private volatile boolean open = true;
    protected final SQLiteServer server;
    
    protected final Deque<SQLiteConnection> connPool;
    protected final int maxPoolSize;
    protected int poolSize;
    
    protected final File file;
    
    /** IP -> hosts cache */
    protected final ConcurrentMap<String, Set<String>> hostsCache;
    /** host -> resolved time */
    protected final ConcurrentMap<String, Resolution> reslvCache;
    
    public SQLiteMetaDb(SQLiteServer server, File metaFile, int maxPoolSize) {
        this.server = server;
        this.file = metaFile;
        this.connPool = new ArrayDeque<>(maxPoolSize);
        this.maxPoolSize = maxPoolSize;
        this.hostsCache = new ConcurrentHashMap<>();
        this.reslvCache = new ConcurrentHashMap<>();
    }
    
    public SQLiteMetaDb(SQLiteServer server, File metaFile) {
        this(server, metaFile, 8);
    }
    
    public boolean isInited() {
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
        if (!isOpen()) {
            throw new IllegalStateException("MetaDb has been closed");
        }
        
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
        if (close || !this.isOpen()) {
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
    
    public boolean isOpen() {
        return (this.open);
    }

    @Override
    public void close() {
        this.open = true;
        
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
    
    public User createUser(User user) throws SQLException {
        final String opass = user.getPassword();
        if (opass != null) {
            // Generate store password
            SQLiteAuthMethod auth = this.server.newAuthMethod(user.getAuthMethod());
            String storePassword = auth.genStorePassword(user.getUser(), opass);
            user.setPassword(storePassword);
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
                    Db db = new Db(user.getUser(), user.getHost(), user.getDb());
                    createDb(conn, stmt, db, user.isSa());
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
    
    public Db createDb(Db db, boolean createDbFile) throws SQLException {
        SQLiteConnection conn = getConnection();
        boolean failed = true;
        try {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                createDb(conn, stmt, db, createDbFile);
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
    
    protected Db createDb(SQLiteConnection conn, Statement stmt, Db db, boolean createDbFile) 
            throws SQLException {
        if (createDbFile) {
            File dbFile = new File(server.getDataDir(), db.getDb());
            if (!server.inDataDir(db.getDb())) {
                throw new SQLException("Database name isn't a relative file name");
            }
            stmt.executeUpdate(format("attach database '%s' as %s", dbFile, db.getDb()));
            stmt.executeUpdate(format("detach database %s", db.getDb()));
        }
        
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
            }
            
            failed = false;
            return n;
        } finally {
            release(conn, failed);
        }
    }
    
    public User selectUser(String host, String protocol, String user, String db) 
            throws SQLException {
        List<User> users= new ArrayList<>();
        User sqliteUser = null;
        
        SQLiteConnection conn = getConnection();
        boolean failed = true;
        try {
            String sql = 
                    "select host, user, password, protocol, auth_method, sa from user u "
                    + "where u.protocol = ? and u.user = ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int i = 0;
                ps.setString(++i, protocol);
                ps.setString(++i, user);
                
                try (ResultSet rs = ps.executeQuery()) {
                    for (; rs.next(); ) {
                        User u = new User();
                        i = 0;
                        u.setHost(rs.getString(++i));
                        u.setUser(rs.getString(++i));
                        u.setPassword(rs.getString(++i));
                        u.setProtocol(rs.getString(++i));
                        u.setAuthMethod(rs.getString(++i));
                        u.setSa(rs.getInt(++i));
                        users.add(u);
                    }
                }
            }
            
            failed = false;
        } finally {
            release(conn, failed);
        }
        
        // check hosts
        // Case-1
        for (User u: users) {
            String h = u.getHost();
            if (h.equals(host)) {
                sqliteUser = u;
                break;
            }
        }
        // Case-2
        if (sqliteUser == null) {
            Set<String> cached = this.hostsCache.get(host);
            List<User> candidates = new ArrayList<>();
            for (User u: users) {
                String h = u.getHost();
                for (; ; ) {
                    if (cached != null) {
                        boolean contains;
                        synchronized (cached) {
                            contains = cached.contains(h);
                        }
                        if (contains) {
                            candidates.add(u);
                            break;
                        }
                    }
                    
                    if (resolveHost(h)) {
                        cached = this.hostsCache.get(host);
                        if (cached != null) {
                            continue;
                        }
                    }
                    
                    break;
                }
            }
            if (candidates.size() > 0) {
                Collections.sort(candidates, new Comparator<User>() {
                    @Override
                    public int compare(User a, User b) {
                        return a.getHost().compareTo(b.getHost());
                    }
                });
                sqliteUser = candidates.get(0);
            }
        }
        // Case-3
        if (sqliteUser == null) {
            for (User u: users) {
                String h = u.getHost();
                if ("%".equals(h)) {
                    sqliteUser = u;
                    break;
                }
            }
        }
        
        // check DB
        if (sqliteUser == null) {
            return null;
        }
        sqliteUser.setDb(db);
        if (sqliteUser.isSa()) {
            return sqliteUser;
        }
        
        conn = getConnection();
        failed = true;
        try {
            String sql = "select d.host, d.user, d.db from db d "
                    + "where d.host = ? and d.user = ? and d.db = ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int i = 0;
                ps.setString(++i, sqliteUser.getHost());
                ps.setString(++i, user);
                ps.setString(++i, db);
                
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        failed = false;
                        return null;
                    }
                }
            }
            failed = false;
            return sqliteUser;
        } finally {
            release(conn, failed);
        }
    }
    
    public int selectHostCount(String host, String protocol) throws SQLException {
        Set<String> hosts = new HashSet<>();
        
        SQLiteConnection conn = getConnection();
        boolean failed = true;
        try {
            String sql = "select u.host from user u where u.protocol = ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int i = 0;
                ps.setString(++i, protocol);
                try (ResultSet rs = ps.executeQuery()) {
                    for (; rs.next(); ) {
                        i = 0;
                        String h = rs.getString(++i);
                        hosts.add(h);
                    }
                }
            }
            failed = false;
        } finally {
            release(conn, failed);
        }
        
        int n = 0;
        if (hosts.contains(host)) {
            // Case-1 exact matching
            ++n;
        }
        Set<String> cached = this.hostsCache.get(host);
        for (String h : hosts) {
            if ("%".equals(h)) {
                // Case-2 fuzzy matching
                ++n;
                continue;
            }
            
            for (;;) {
                if (cached != null) {
                    boolean contains;
                    synchronized(cached) {
                        // Case-3.1 domain matching in resolved
                        contains = cached.contains(h);
                    }
                    if (contains) {
                        ++n;
                        break;
                    }
                }
                
                if (resolveHost(h)) {
                    cached = this.hostsCache.get(host);
                    if (cached != null) {
                        continue;
                    }
                }
                
                break;
            }
        }
        
        return n;
    }
    
    protected boolean resolveHost(String h) {
        Resolution reslv = this.reslvCache.get(h);
        if (reslv != null && !reslv.isExpired()) {
            return false;
        }
        
        try {
            for (InetAddress addr: InetAddress.getAllByName(h)) {
                String ip = addr.getHostAddress();
                Set<String> newCache = new HashSet<>();
                Set<String> oldCache = this.hostsCache.putIfAbsent(ip, newCache);
                if (oldCache != null) {
                    newCache = oldCache;
                }
                synchronized(newCache) {
                    newCache.add(h);
                }
            }
            this.reslvCache.put(h, new Resolution(true));
            return true;
        } catch (UnknownHostException e) {
            log.warn("Can't resolve the host " + h, e);
            this.reslvCache.put(h, new Resolution(false));
            return false;
        }
    }
    
    public void flushHosts() {
        this.reslvCache.clear();
        this.hostsCache.clear();
    }
    
    /** Host resolution. */
    static class Resolution {
        private boolean success;
        private long atime;
        
        Resolution (boolean success) {
            this.success = success;
            this.atime = System.currentTimeMillis();
        }
        
        synchronized Resolution update(boolean success) {
            this.success = success;
            this.atime = System.currentTimeMillis();
            return this;
        }
        
        synchronized boolean isExpired() {
            long interval = 180000L; // 3min when error
            if (isSuccess()) {
                interval *= 10L;     //30min when success
            }
            return (System.currentTimeMillis() - this.atime > interval);
        }
        
        synchronized boolean isSuccess() {
            return this.success;
        }
    }
    
}
