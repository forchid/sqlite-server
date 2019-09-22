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

import static java.lang.String.format;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteConfig.Encoding;
import org.sqlite.SQLiteConfig.JournalMode;
import org.sqlite.SQLiteConfig.SynchronousMode;
import org.sqlite.server.sql.meta.Catalog;
import org.sqlite.server.sql.meta.Db;
import org.sqlite.server.sql.meta.User;
import org.sqlite.SQLiteConnection;
import org.sqlite.util.IoUtils;
import org.sqlite.util.SecurityUtils;

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
            + "sa integer not null default 0,"
            + "primary key(protocol, user, host))";
    
    protected static final String INSERT_USER =
            "insert into user(host, user, password, protocol, auth_method, sa)"
            + "values(?, ?, ?, ?, ?, ?)";
    
    protected static final String CREATE_TABLE_DB =
            "create table if not exists db("
            + "host varchar(60) not null,"
            + "db varchar(64) not null,"
            + "user varchara(32) not null,"
            + "all_priv integer not null default 0,"
            + "select_priv integer not null default 0,"
            + "insert_priv integer not null default 0,"
            + "update_priv integer not null default 0,"
            + "delete_priv integer not null default 0,"
            + "create_priv integer not null default 0,"
            + "alter_priv integer not null default 0,"
            + "drop_priv integer not null default 0,"
            + "pragma_priv integer not null default 0,"
            + "vacuum_priv integer not null default 0,"
            + "attach_priv integer not null default 0,"
            + "primary key(host, db, user))";
    
    protected static final String INSERT_DB =
            "insert into db(host, db, user, "
            + "all_priv, select_priv, insert_priv, update_priv, delete_priv, "
            + "create_priv, alter_priv, drop_priv, pragma_priv, vacuum_priv, "
            + "attach_priv) "
            + "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    protected static final String SELECT_DB =
            "select host, db, user, "
            + "all_priv, select_priv, insert_priv, update_priv, delete_priv, "
            + "create_priv, alter_priv, drop_priv, pragma_priv, vacuum_priv, "
            + "attach_priv "
            + "from db";
    
    protected static final String CREATE_TABLE_CATALOG = 
            "create table if not exists catalog("
            + "db varchar(64) not null,"
            + "dir varchar(256),"
            + "size integer not null default 0,"
            + "primary key(db))";
    protected static final String INSERT_CATALOG = 
            "insert into catalog(db, dir, size)values(?, ?, ?)";
    
    private volatile boolean open = true;
    protected final SQLiteServer server;
    protected final File file;
    
    /** IP -> hosts cache */
    protected final ConcurrentMap<String, Set<String>> hostsCache;
    /** host -> resolved time */
    protected final ConcurrentMap<String, Resolution> reslvCache;
    /** user@host/db -> Db */
    protected final ConcurrentMap<String, Db> privCache;
    private final AtomicBoolean privLoading = new AtomicBoolean();
    private volatile boolean flushPrivs = true;
    /** db -> catalog */
    protected final ConcurrentMap<String, Catalog> catalogCache;
    
    public SQLiteMetaDb(SQLiteServer server, File metaFile) {
        this.server = server;
        this.file = metaFile;
        this.hostsCache = new ConcurrentHashMap<>();
        this.reslvCache = new ConcurrentHashMap<>();
        this.privCache = new ConcurrentHashMap<>();
        this.catalogCache = new ConcurrentHashMap<>();
    }
    
    public boolean isInited() {
        if (this.file.exists() && this.file.isFile()) {
            try (SQLiteConnection conn = newConnection()) {
                int n = selectUserCount(conn);
                return (n > 0);
            } catch (SQLException e) {
                String error = "Access " + this.file.getName() + " fatal";
                throw new IllegalStateException(error, e);
            }
        }
        
        return false;
    }
    
    public SQLiteConnection newConnection() throws SQLException {
        if (!isOpen()) {
            throw new IllegalStateException("MetaDb has been closed");
        }
        
        SQLiteConfig config = new SQLiteConfig();
        config.setJournalMode(JournalMode.WAL);
        config.setSynchronous(SynchronousMode.NORMAL);
        config.setBusyTimeout(50000);
        config.setEncoding(Encoding.UTF8);
        return (SQLiteConnection)config.createConnection("jdbc:sqlite:"+this.file);
    }
    
    public String attachTo(SQLiteConnection conn) throws SQLException {
        String schema = genSchemaName();
        String sql = format("attach database '%s' as %s", this.file, schema);
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
        return schema;
    }
    
    public void detachFrom(SQLiteConnection conn, String schema) throws SQLException {
        String sql = format("detach database %s", schema);
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }
    
    private String genSchemaName() {
        return ("meta_"+SecurityUtils.nextHexs(10));
    }
    
    public boolean isOpen() {
        return (this.open);
    }

    @Override
    public void close() {
        this.open = true;
    }
    
    public void initdb(User superuser) throws SQLException {
        SQLiteConnection conn = newConnection();
        try {
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            
            createUser(conn, stmt, superuser);
            Db db = new Db(superuser);
            db.setAllPriv(1);
            createDb(conn, stmt, db);
            
            stmt.close();
            conn.commit();
        } finally {
            conn.close();
        }
    }
    
    protected Catalog createCatalog(SQLiteConnection conn, Statement stmt, Catalog catalog) 
            throws SQLException {
        PreparedStatement ps;
        int i;
        
        stmt.executeUpdate(CREATE_TABLE_CATALOG);
        ps = conn.prepareStatement(INSERT_CATALOG);
        i = 0;
        ps.setString(++i, catalog.getDb());
        ps.setString(++i, catalog.getDir());
        ps.setLong(++i, catalog.getSize());
        ps.executeUpdate();
        
        return catalog;
    }
    
    protected User createUser(SQLiteConnection conn, Statement stmt, User user) throws SQLException {
        final String opass = user.getPassword();
        if (opass != null) {
            // Generate store password
            String proto = user.getProtocol();
            SQLiteAuthMethod auth = this.server.newAuthMethod(proto, user.getAuthMethod());
            String storePassword = auth.genStorePassword(user.getUser(), opass);
            user.setPassword(storePassword);
        }
        
        boolean failed = true;
        try {
            PreparedStatement ps;
            int i = 0;
            
            stmt.executeUpdate(CREATE_TABLE_USER);
            ps = conn.prepareStatement(INSERT_USER);
            ps.setString(++i, user.getHost());
            ps.setString(++i, user.getUser());
            ps.setString(++i, user.getPassword());
            ps.setString(++i, user.getProtocol());
            ps.setString(++i, user.getAuthMethod());
            ps.setInt(++i, user.getSa());
            ps.executeUpdate();
            
            failed = false;
            return user;
        } finally {
            if (failed) {
                user.setPassword(opass);
            }
        }
    }
    
    protected Db createDb(SQLiteConnection conn, Statement stmt, Db db) throws SQLException {
        PreparedStatement ps;
        int i;
        
        // check
        final File dbFile = new File(server.getDataDir(), db.getDb());
        if (!server.inDataDir(db.getDb())) {
            throw new SQLException("db name isn't a file name");
        }
        
        stmt.executeUpdate(CREATE_TABLE_DB);
        ps = conn.prepareStatement(INSERT_DB);
        i = 0;
        ps.setString(++i, db.getHost());
        ps.setString(++i, db.getDb());
        ps.setString(++i, db.getUser());
        ps.setInt(++i, db.getAllPriv());
        ps.setInt(++i, db.getSelectPriv());
        ps.setInt(++i, db.getInsertPriv());
        ps.setInt(++i, db.getUpdatePriv());
        ps.setInt(++i, db.getDeletePriv());
        ps.setInt(++i, db.getCreatePriv());
        ps.setInt(++i, db.getAlterPriv());
        ps.setInt(++i, db.getDropPriv());
        ps.setInt(++i, db.getPragmaPriv());
        ps.setInt(++i, db.getVacuumPriv());
        ps.setInt(++i, db.getAttachPriv());
        ps.executeUpdate();
        
        Catalog catalog = new Catalog(db.getDb());
        createCatalog(conn, stmt, catalog);
        // Do create
        String schema = db.getDb();
        stmt.executeUpdate(format("attach database '%s' as '%s'", dbFile, schema));
        stmt.executeUpdate(format("detach database '%s'", schema));
        
        return db;
    }
    
    public Db selectDb(User user) throws SQLException {
        return (selectDb(user.getHost(), user.getUser(), user.getDb()));
    }
    
    public Db selectDb(String host, String user, String db) throws SQLException {
        if (tryLoadPrivs()) {
            String key = privCacheKey(host, user, db);
            this.server.trace(log, "privCache key: {}", key);
            Db d = this.privCache.get(key);
            this.server.trace(log, "privCache db: {}", d);
            return d;
        }
        
        return null;
    }
    
    /**
     * @return this metaDb file name
     */
    public String getDbName() {
        return (this.file.getName());
    }
    
    public boolean hasPrivilege(String host, String user, String db, String command, String dataDir) 
            throws SQLException {
        if (!hasPrivilege(host, user, db, command)) {
            return false;
        }
        
        Catalog catalog = selectCatalog(db);
        if (catalog == null) {
            return false;
        }
        
        String dir = catalog.getDir();
        if (dir == null) {
            if (dataDir == null) {
                return true;
            }
            return false;
        }
        
        return (dir.equals(dataDir));
    }
    
    public boolean hasPrivilege(String host, String user, String db, String command) 
            throws SQLException {
        Db d = selectDb(host, user, db);
        if (d == null) {
            return false;
        }
        
        return (d.hasPriv(command));
    }
    
    public boolean hasPrivilege(User user, String command) throws SQLException {
        if (user.isSa()) {
            return true;
        }
        
        Db d = selectDb(user);
        if (d == null) {
            return false;
        }
        
        return (d.hasPriv(command));
    }
    
    protected int selectUserCount(SQLiteConnection conn) throws SQLException {
        int n = 0;
        try (Statement stmt = conn.createStatement()) {
            if (!tableExists(stmt, "user")) {
                return 0;
            }
            
            String sql = "select count(*) from user";
            try (ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    n = rs.getInt(1);
                }
            }
        }
        
        return n;
    }
    
    public Catalog selectCatalog(String db) throws SQLException {
        Catalog catalog = this.catalogCache.get(db);
        if (catalog != null) {
            return catalog;
        }
        
        this.server.trace(log, "Load '{}' catalog", db);
        try (SQLiteConnection conn = newConnection()) {
            String sql = "select db, dir, size from catalog where db = ?";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, db);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                catalog = new Catalog(rs.getString(1), rs.getString(2));
                catalog.setSize(rs.getLong(3));
                this.server.trace(log, "Load '{}' catalog OK", db);
            }
        }
        
        if (catalog != null) {
            this.catalogCache.put(db, catalog);
        }
        return catalog;
    }
    
    public User selectUser(String host, String protocol, String user, String db) 
            throws SQLException {
        List<User> users= new ArrayList<>();
        User sqliteUser = null;
        
        try (SQLiteConnection conn = newConnection()) {
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
        
        boolean failed = true;
        SQLiteConnection conn = newConnection();
        try {
            try {
                conn.setAutoCommit(false);
                Statement stmt = conn.createStatement();
                if (!tableExists(stmt, "db")) {
                    sqliteUser = null;
                } else {
                    String sql = "select d.host, d.user, d.db from db d "
                            + "where d.host = ? and d.user = ? and d.db = ?";
                    PreparedStatement ps = conn.prepareStatement(sql);
                    int i = 0;
                    ps.setString(++i, sqliteUser.getHost());
                    ps.setString(++i, user);
                    ps.setString(++i, db);
                    ResultSet rs = ps.executeQuery();
                    if (!rs.next()) {
                        sqliteUser = null;
                    }
                }
                
                conn.commit();
                failed = false;
                return sqliteUser;
            } finally {
                if (failed) {
                    conn.rollback();
                }
            }
        } finally {
            IoUtils.close(conn);
        }
    }
    
    public int selectHostCount(String host, String protocol) throws SQLException {
        Set<String> hosts = new HashSet<>();
        
        try (SQLiteConnection conn = newConnection()) {
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
    
    public List<Catalog> statisticsCatalogs(File dataDir) throws SQLException {
        List<Catalog> catalogs = new ArrayList<>();
        try (SQLiteConnection conn = newConnection()) {
            Statement s = conn.createStatement();
            String sql = "select db, dir from catalog order by db asc";
            ResultSet rs = s.executeQuery(sql);
            for (; rs.next(); ) {
                Catalog c = new Catalog();
                c.setDb(rs.getString(1));
                c.setDir(rs.getString(2));
                catalogs.add(c);
            }
        }
        
        // Do statistics
        for (Catalog c : catalogs) {
            String dir = c.getDir();
            File dirFile = dataDir;
            if (dir != null) {
                dirFile = new File(dir);
            }
            File dbFile = new File(dirFile, c.getDb());
            long size = dbFile.length();
            File walFile= new File(dirFile, c.getDb()+"-wal");
            if (walFile.isFile()) {
                size += walFile.length();
            }
            c.setSize(size);
        }
        
        // Store
        try (SQLiteConnection conn = newConnection()) {
            String sql = "update catalog set size = ? where db = ?";
            PreparedStatement ps = conn.prepareStatement(sql);
            for (Catalog c: catalogs) {
                ps.setLong(1, c.getSize());
                ps.setString(2, c.getDb());
                ps.addBatch();
            }
            ps.executeBatch();
        }
        
        return catalogs;
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
    
    public void flushCatalogs() {
        this.catalogCache.clear();
    }
    
    public void flushHosts() {
        this.reslvCache.clear();
        this.hostsCache.clear();
    }
    
    public void flushPrivileges() {
        this.flushPrivs = true;
    }
    
    protected boolean tryLoadPrivs() throws SQLException {
        if (!this.flushPrivs) {
            return true;
        }
        
        for (; !this.privLoading.compareAndSet(false, true);) {
            // Spin lock
        }
        Connection conn = null;
        try {
            this.server.trace(log, "load privileges");
            // Double check
            if (!this.flushPrivs) {
                return true;
            }
            
            conn = newConnection();
            Statement stmt = conn.createStatement();
            if (!tableExists(stmt, "db")) {
                this.server.trace(log, "`db` table not exists in metaDb");
                return false;
            }
            
            ResultSet rs = stmt.executeQuery(SELECT_DB);
            Map<String, Db> privs = new HashMap<>();
            for (; rs.next(); ) {
                final Db db = new Db();
                final String h, u, d, k;
                int i = 0;
                db.setHost(h = rs.getString(++i));
                db.setDb(d = rs.getString(++i));
                db.setUser(u = rs.getString(++i));
                db.setAllPriv(rs.getInt(++i));
                db.setSelectPriv(rs.getInt(++i));
                db.setInsertPriv(rs.getInt(++i));
                db.setUpdatePriv(rs.getInt(++i));
                db.setDeletePriv(rs.getInt(++i));
                db.setCreatePriv(rs.getInt(++i));
                db.setAlterPriv(rs.getInt(++i));
                db.setDropPriv(rs.getInt(++i));
                db.setPragmaPriv(rs.getInt(++i));
                db.setVacuumPriv(rs.getInt(++i));
                db.setAttachPriv(rs.getInt(++i));
                k = privCacheKey(h, u, d);
                privs.put(k, db);
                this.server.trace(log, "privCache put(<{}, {}...>)", k, db);
            }
            this.privCache.clear();
            this.privCache.putAll(privs);
            this.flushPrivs = false;
            this.server.trace(log, "load privileges OK");
            return true;
        } finally {
            this.privLoading.set(false);
            IoUtils.close(conn);
        }
    }
    
    protected String privCacheKey(String host, String user, String db) {
        return (format("%s@%s/%s", user, host, db));
    }
    
    protected boolean tableExists(Statement stmt, String name) throws SQLException {
        String sql = tableInfoSQL(name);
        try (ResultSet rs = stmt.executeQuery(sql)) {
            return (rs.next());
        }
    }
    
    protected String tableInfoSQL(String name) {
        return (format("select * from sqlite_master "
                + "where type = 'table' and name = '%s'", name));
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
