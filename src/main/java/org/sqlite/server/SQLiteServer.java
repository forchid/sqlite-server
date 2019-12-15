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

import static org.sqlite.server.util.StringUtils.toLowerEnglish;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.JDBC;
import org.sqlite.SQLiteConfig.Encoding;
import org.sqlite.SQLiteConfig.JournalMode;
import org.sqlite.SQLiteConfig.SynchronousMode;
import org.sqlite.SQLiteConnection;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.server.func.StringResultFunc;
import org.sqlite.server.func.TimestampFunc;
import org.sqlite.server.func.VersionFunc;
import org.sqlite.server.pg.PgServer;
import org.sqlite.server.sql.meta.Catalog;
import org.sqlite.server.sql.meta.User;
import org.sqlite.server.util.IoUtils;
import org.sqlite.sql.SQLContext;
import org.sqlite.server.util.StringUtils;

import static java.lang.String.*;
import static org.sqlite.SQLiteConfig.Pragma;

/**The SQLite server that abstracts various server's protocol, based on TCP/IP, 
 * and can be started and stopped.
 * 
 * @author little-pan
 * @since 2019-09-01
 *
 */
public abstract class SQLiteServer implements AutoCloseable {
    
    public static final String SQLITED_HOME = initSQLitedHome();
    
    public static final String NAME = "SQLite server";
    public static final String VERSION = "0.3.29";
    public static final String USER_DEFAULT = "root";
    public static final String METADB_NAME  = "sqlite3.meta";
    public static final String HOST_DEFAULT = "localhost";
    public static final int AUTH_TIMEOUT_DEFAULT = 15000;
    public static final int PORT_DEFAULT = 3272;
    public static final int MAX_CONNS_DEFAULT = 50;
    public static final int MAX_WORKER_COUNT  = 128;
    public static final int OPEN_TIMEOUT_DEFAULT = 30000;
    public static final long MAX_ALLOWED_PACKET_DEFAULT = 16L << 20;
    public static final int SLEEP_TIMEOUT_DEFAULT = 300000;
    public static final int SLEEP_IN_TX_TIMEOUT_DEFAULT = 30000;
    // SQLite settings
    public static final int BUSY_TIMEOUT_DEFAULT = 50000;
    public static final JournalMode JOURNAL_MODE_DEFAULT = JournalMode.WAL;
    public static final SynchronousMode SYNCHRONOUS_DEFAULT = SynchronousMode.NORMAL;
    
    // command list
    public static final String CMD_INITDB = "initdb";
    public static final String CMD_BOOT   = "boot";
    public static final String CMD_HELP   = "help";
    
    private static final Logger log = LoggerFactory.getLogger(SQLiteServer.class);
    
    protected SQLiteMetaDb metaDb;
    protected String command;
    
    private String username = USER_DEFAULT;
    private String password;
    private String dbName;
    protected String authMethod;
    
    protected String host = HOST_DEFAULT;
    protected int port = PORT_DEFAULT;
    protected int maxConns = MAX_CONNS_DEFAULT;
    private int maxPid;
    protected int openTimeout = OPEN_TIMEOUT_DEFAULT;
    protected int busyTimeout = BUSY_TIMEOUT_DEFAULT;
    protected int authTimeout = AUTH_TIMEOUT_DEFAULT;
    protected int sleepTimeout = SLEEP_TIMEOUT_DEFAULT;
    protected int sleepInTxTimeout = SLEEP_IN_TX_TIMEOUT_DEFAULT;
    protected JournalMode journalMode = JOURNAL_MODE_DEFAULT;
    protected SynchronousMode synchronous = SYNCHRONOUS_DEFAULT;
    private final ConcurrentMap<String, SQLContext> dbWriteLocks;
    protected File dataDir = new File(System.getProperty("user.home"), "sqlite3Data");
    protected boolean trace;
    protected boolean traceError;
    
    protected final String protocol;
    protected Selector selector;
    protected ServerSocketChannel serverSocket;
    protected SQLiteWorker[] workers;
    protected int workerCount = Runtime.getRuntime().availableProcessors();
    protected int workerId;
    
    private String startTime;
    private long startMillis;
    private long startNanos;
    protected StringResultFunc startTimeFunc;
    protected VersionFunc versionFunc;
    protected StringResultFunc serverVersionFunc;
    protected TimestampFunc clockTimestampFunc;
    protected TimestampFunc sysdateFunc;
    
    // Resource limit since v0.3.29 2019-12-14
    protected long maxAllowedPacket = MAX_ALLOWED_PACKET_DEFAULT;
    
    // Life-cycle states
    private final AtomicBoolean inited = new AtomicBoolean(false);
    private volatile boolean stopped;
    
    public static void main(String args[]) {
        if (args.length == 0) {
            String message = getHelp(SQLiteServer.class, NAME, VERSION);
            doHelp(1, message);
            return;
        }
        
        main(SQLiteServer.create(args), args);
    }
    
    public static void main(SQLiteServer server, String ... args) {
        try {
            if (args.length == 0) {
                throw new IllegalArgumentException("No command specified");
            }
            
            String command = args[0];
            switch (command) {
            case CMD_INITDB:
                server.initdb(args);
                break;
            case CMD_BOOT:
                server.boot(args);
                break;
            case CMD_HELP:
                server.help(0);
                break;
            default:
                throw new IllegalArgumentException("Unknown command: " + command);
            }
        } catch (Exception e) {
            if (server.isTrace()) {
                server.traceError(log, NAME + " fatal", e);
            } else if (e instanceof IllegalArgumentException) {
                String command = server.command;
                if (command == null) {
                    server.help(1);
                } else {
                    server.help(1, command);
                }
            } else {
                System.err.println("[ERROR] " + e.getMessage() 
                    + (e.getCause() != null? "(" + e.getCause().getMessage() + ")": ""));
            }
        } finally {
            IoUtils.close(server);
        }
    }
    
    public static SQLiteServer create(String ... args) {
        for (int i = 0, argc = args.length; i < argc; ++i) {
            String arg = args[i];
            if ("--protocol".equals(arg)) {
                if ("pg".equals(arg)) {
                    return new PgServer();
                } else {
                    throw new IllegalArgumentException("Unknown protocol: " + arg);
                }
            }
        }
        
        return new PgServer();
    }
    
    protected SQLiteServer(String protocol) {
        this.protocol = protocol;
        this.dbWriteLocks = new ConcurrentHashMap<>();
    }
    
    protected String[] wrapArgs(String command, String ... args) {
        if ((args.length == 0) || 
                (!command.equals(args[0]) && !CMD_HELP.equals(args[0]))) {
            String[] newArgs = new String[args.length + 1];
            newArgs[0] = command;
            System.arraycopy(args, 0, newArgs, 1, args.length);
            args = newArgs;
        }
        
        return args;
    }
    
    public void initdb(String ... args) {
        args = wrapArgs(CMD_INITDB, args);
        init(args);
        
        String authMethod = getAuthMethod();
        if (authMethod == null) {
            throw new IllegalArgumentException("No auth method provided");
        }
        
        // Create super user
        File dataDir = getDataDir();
        try {
            trace(log, "initdb in {}", dataDir);
            if (!inDataDir(this.dbName)) {
                throw new IllegalArgumentException("db name isn't a file name");
            }
            
            User sa = User.createSuperuser(this.username, this.password);
            sa.setAuthMethod(authMethod);
            sa.setHost(this.host);
            sa.setProtocol(this.protocol);
            sa.setDb(this.dbName);
            this.metaDb.initdb(sa);
        } catch (SQLException e) {
            traceError(log, "initdb fatal", e);
            if (isUniqueViolated(e)) {
                throw new IllegalStateException("Data dir has been initialized: " + dataDir);
            }
            throw new IllegalStateException("initdb fatal: " + e.getMessage(), e);
        }
    }
    
    /** Whether busy or locked.
     * 
     * @param e the SQL exception
     * @return true if busy or locked, otherwise false
     */
    public boolean isBlocked(SQLException e) {
        for (; e != null;) {
            int errorCode = e.getErrorCode();
            switch (errorCode) {
            case 5: // SQLITE_BUSY
            case 6: // SQLITE_LOCKED
            case 261: // SQLITE_BUSY_RECOVERY
            case 262: // SQLITE_LOCKED_SHAREDCACHE
            case 517: // SQLITE_BUSY_SNAPSHOT
                return true;
            default:
                trace(log, "sql errorCode: {}", errorCode);
                Throwable cause = e.getCause();
                if (cause instanceof SQLException) {
                    e = (SQLException)cause;
                } else {
                    e = null;
                }
                break;
            }
        }
        
        return false;
    }
    
    public boolean isCanceled(SQLException e) {
        return (e.getErrorCode() == SQLiteErrorCode.SQLITE_INTERRUPT.code);
    }
    
    public boolean isUniqueViolated(SQLException cause) {
        int errorCode = cause.getErrorCode();
        
        if (SQLiteErrorCode.SQLITE_CONSTRAINT.code == errorCode) {
            String errorMessage = cause.getMessage();
            return (errorMessage != null && errorMessage.contains("UNIQUE"));
        }
        
        return (SQLiteErrorCode.SQLITE_CONSTRAINT_UNIQUE.code == errorCode 
                || SQLiteErrorCode.SQLITE_CONSTRAINT_PRIMARYKEY.code == errorCode);
    }
    
    protected void initDataDir() {
        try {
            File baseDir = this.dataDir = this.dataDir.getCanonicalFile();
            if (!baseDir.isDirectory() && !baseDir.mkdirs()) {
                throw new IllegalStateException("Can't mkdirs data dir: " + baseDir);
            }
            
            // init metaDb
            File metaFile = new File(baseDir, METADB_NAME);
            this.metaDb = new SQLiteMetaDb(this, metaFile);
            if (CMD_BOOT.equals(this.command) && !this.metaDb.isInited()) {
                throw new IllegalStateException("Data dir hasn't been initialized: " + baseDir);
            }
        } catch (IOException cause) {
            throw new IllegalStateException(cause);
        }
    }
    
    public void boot(String... args) {
        try {
            args = wrapArgs(CMD_BOOT, args);
            init(args);
            start();
            listen();
        } finally {
            stop();
        }
    }
    
    public SQLiteServer bootAsync(String... args) {
        return bootAsync(true, args);
    }
    
    public SQLiteServer bootAsync(final boolean daemon, String... args) {
        return bootAsync(new Executor() {
            @Override
            public void execute(Runnable command) {
                Thread runner = new Thread(command);
                runner.setDaemon(daemon);
                runner.start();
            }
        }, args);
    }
    
    public SQLiteServer bootAsync(Executor executor, String... args) {
        boolean failed = true;
        try {
            args = wrapArgs(CMD_BOOT, args);
            init(args);
            start();
            executor.execute(new Runnable(){
                @Override
                public void run() {
                    try {
                        listen();
                    } finally {
                        stop();
                    }
                }
            });
            failed = false;
            return this;
        } finally {
            if (failed) {
                stop();
            }
        }
    }
    
    public boolean isInited() {
        return (this.inited.get());
    }
    
    public void init(String... args) {
        if (!this.inited.compareAndSet(false, true)) {
            throw new IllegalStateException(getName() + " has been initialized");
        }
        if (this.isStopped()) {
            throw new IllegalStateException(getName() + " has been stopped");
        }
        
        boolean help = false;
        int i = 0;
        
        // Check command
        if(args == null || args.length < 1) {
            throw new IllegalArgumentException("No command specified");
        }
        String command = this.command = args[i++];
        
        // Parse args
        for (int argc = args.length; i < argc; i++) {
            String a = args[i];
            if ("--auth-timeout".equals(a)) {
                this.authTimeout = Integer.decode(args[++i]);
            } else if ("--busy-timeout".equals(a)) {
                int busyTimeout = Integer.decode(args[++i]);
                if (busyTimeout < 0) {
                    throw new IllegalArgumentException(a+" " + busyTimeout);
                }
                this.busyTimeout = busyTimeout;
            } else if ("--trace".equals(a) || "-T".equals(a)) {
                this.trace = true;
            } else if ("--trace-error".equals(a)) {
                this.traceError = true;
            } else if ("--user".equals(a) || "-U".equals(a)) {
                this.username = args[++i];
            } else if ("--password".equals(a) || "-p".equals(a)) {
                this.password = args[++i];
            } else if ("--host".equals(a) || "-H".equals(a)) {
                this.host = args[++i];
            } else if ("--db".equals(a) || "-d".equals(a)) {
                this.dbName = args[++i];
            } else if ("--open-timeout".equals(a)) {
                this.openTimeout = Integer.decode(args[++i]);
            } else if ("--port".equals(a) || "-P".equals(a)) {
                this.port = Integer.decode(args[++i]);
            } else if ("--data-dir".equals(a) || "-D".equals(a)) {
                this.dataDir = new File(args[++i]);
            } else if ("--journal-mode".equals(a)) {
                String mode = StringUtils.toUpperEnglish(args[++i]);
                this.journalMode = JournalMode.valueOf(mode);
            } else if ("--max-conns".equals(a)) {
                this.maxConns = Integer.decode(args[++i]);
            } else if ("--sleep-timeout".equals(a)) {
                this.sleepTimeout = Integer.decode(args[++i]);
            } else if ("--sleep-in-tx-timeout".equals(a)) {
                this.sleepInTxTimeout = Integer.decode(args[++i]);
            } else if ("--synchronous".equals(a) || "-S".equals(a)) {
                String mode = StringUtils.toUpperEnglish(args[++i]);
                this.synchronous = SynchronousMode.valueOf(mode);
            } else if ("--worker-count".equals(a)) {
                int n = Math.max(1, Integer.decode(args[++i]));
                this.workerCount = Math.min(MAX_WORKER_COUNT, n);
            } else if ("--auth-method".equals(a) || "-A".equals(a)) {
                this.authMethod = toLowerEnglish(args[++i]);
            } else if ("--max-allowed-packet".equals(a)) {
                this.maxAllowedPacket = Long.decode(args[++i]);
            } else if ("--help".equals(a) || "-h".equals(a) || "-?".equals(a)) {
                help = true;
            }
        }
        if (this.dbName == null) {
            this.dbName = this.username;
        }
        
        trace(log, "command {}", command);
        switch (command) {
        case CMD_INITDB:
        case CMD_BOOT:
            break;
        case CMD_HELP:
            help(0);
            break;
        default:
            this.command = null;
            throw new IllegalArgumentException("Unknown command: " + command);
        }
        
        if (help) {
            help(0, this.command);
        }
        
        initDataDir();
    }
    
    public void start() throws NetworkException {
        String name = getName();
        TimestampFunc clockTimestampFunc;
        // start timestamp
        this.startMillis = System.currentTimeMillis();
        this.startNanos = System.nanoTime();
        clockTimestampFunc = new TimestampFunc(this, "clock_timestamp", 6);
        this.startTime = clockTimestampFunc.getTimestamp();
        log.info("SQLITED_HOME '{}'", SQLITED_HOME);
        
        if (!isInited()) {
            throw new IllegalStateException(name + " hasn't been initialized");
        }
        if (isStopped()) {
            throw new IllegalStateException(name + " has been stopped");
        }
        
        boolean failed = true;
        try {
            InetAddress addr = InetAddress.getByName(getHost());
            InetSocketAddress local = new InetSocketAddress(addr, getPort());
            int backlog = Math.min(getMaxConns() * this.workerCount, 1000);
            this.serverSocket = ServerSocketChannel.open();
            this.serverSocket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            this.serverSocket.bind(local, backlog);
            this.serverSocket.configureBlocking(false);
            this.selector = Selector.open();
            this.serverSocket.register(this.selector, SelectionKey.OP_ACCEPT);
            
            // server workers
            startWorkers();
            
            // version functions
            this.versionFunc = new VersionFunc(this);
            this.serverVersionFunc = new StringResultFunc(getVersion());
            // timestamp functions
            this.clockTimestampFunc = clockTimestampFunc;
            this.startTimeFunc = new StringResultFunc(this.startTime);
            this.sysdateFunc = new TimestampFunc(this, "sysdate");
            
            failed = false;
        } catch (IOException e) {
            throw new NetworkException("Can't create server socket", e);
        } finally {
            if (failed) {
                stop();
            }
        }
    }
    
    protected void startWorkers() throws IOException {
        this.workers = new SQLiteWorker[this.workerCount];
        for (int i = 0, n = workers.length; i < n; ++i) {
            SQLiteWorker worker = new SQLiteWorker(this, i);
            this.workers[i] = worker;
            worker.start();
        }
    }
    
    protected SQLiteWorker nextWorker() {
        SQLiteWorker worker = this.workers[this.workerId];
        if (worker.isOpen() && !worker.isStopped()) {
            this.workerId = ++this.workerId % this.workers.length;
            return worker;
        }
        
        int i = (this.workerId + 1) % this.workers.length;
        for (; i != this.workerId; ) {
            worker = this.workers[i];
            if (worker.isOpen() && !worker.isStopped()) {
                this.workerId = i;
                return worker;
            }
            i = ++i % this.workers.length;
        }
        
        throw new IllegalStateException("No available worker");
    }
    
    public int getWorkerCount() {
        return this.workerCount;
    }
    
    public void dbIdle() {
        SQLiteWorker[] workers = this.workers;
        for (int i = 0, n = workers.length; i < n; ++i) {
            SQLiteWorker worker = workers[i];
            if (worker == null || !worker.isOpen()) {
                continue;
            }
            worker.dbIdle(false);
        }
    }
    
    public SQLiteProcessor getProcessor(int pid) {
        SQLiteWorker[] workers = this.workers;
        for (int i = 0, n = workers.length; i < n; ++i) {
            SQLiteWorker worker = workers[i];
            if (worker == null) {
                continue;
            }
            SQLiteProcessor p = worker.getProcessor(pid);
            if (p != null) {
                return p;
            }
        }
        return null;
    }
    
    public void listen() {
        String name = getName();
        if (!isInited()) {
            throw new IllegalStateException(name + " hasn't been initialized");
        }
        
        try {
            Thread.currentThread().setName(name);
            log.info("Ready for connections on {}:{}, version {}", getHost(), getPort(), getVersion());
            for (; !isStopped(); ) {
                int n = this.selector.select();
                if (n > 0) {
                    Iterator<SelectionKey> keys = this.selector.selectedKeys().iterator();
                    for (; keys.hasNext(); keys.remove()) {
                        SelectionKey key = keys.next();
                        if (key.isAcceptable()) {
                            ServerSocketChannel schan = (ServerSocketChannel)key.channel();
                            SocketChannel channel = schan.accept();
                            if (channel == null) {
                                continue;
                            }
                            SQLiteProcessor processor = null;
                            try {
                                trace(log, "Connection {}", channel);
                                // settings
                                channel.configureBlocking(false);
                                channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                                channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                                
                                int pid = nextPid();
                                processor = newProcessor(channel, pid);
                                SQLiteWorker worker = nextWorker();
                                if (!worker.offer(processor)) {
                                    IoUtils.close(processor);
                                    trace(log, "{} processor queue full, close {}", worker.getName(), processor.getName());
                                }
                            } catch (IOException | NetworkException e) {
                                IoUtils.close(channel);
                                log.error("Can't create processor", e);
                            }
                        } else {
                            key.cancel();
                        }
                    }// for-keys
                }
            }
        } catch (IOException e) {
            if (this.serverSocket.isOpen()) {
                log.error(name+" fatal", e);
            }
        } finally {
            doStop();
        }
    }
    
    public void stop() {
        this.stopped = true;
        // stop server
        IoUtils.close(this.serverSocket);
        IoUtils.close(this.selector);
        // stop workers
        stopWorkers();
    }
    
    protected void doStop() {
        // 1. Close this server
        IoUtils.close(this.serverSocket);
        IoUtils.close(this.selector);
        // 2. Stop all workers
        stopWorkers();
        // 3. Close metaDb
        IoUtils.close(this.metaDb);
        
        log.info("{} has stopped", this);
    }
    
    protected void stopWorkers() {
        SQLiteWorker[] workers = this.workers;
        if (workers == null) {
            return;
        }
        
        for (int i = 0, n = workers.length; i < n; ++i) {
            SQLiteWorker worker = workers[i];
            if (worker != null) {
                worker.stop();
            }
        }
    }
    
    public boolean isAllowed(InetSocketAddress remoteAddr) throws SQLException {
        String host = remoteAddr.getAddress().getHostAddress();
        int n = this.metaDb.selectHostCount(host, getProtocol());
        return (n > 0);
    }
    
    public User selectUser(InetSocketAddress remoteAddr, String user, String db) throws SQLException {
        String host = remoteAddr.getAddress().getHostAddress();
        return (this.metaDb.selectUser(host, getProtocol(), user, db));
    }
    
    @Override
    public void close() {
        this.stop();
    }
    
    public boolean isOpen() {
        return (!this.isStopped());
    }
    
    public SQLiteConnection newSQLiteConnection(String dbName) throws SQLException {
        String url = "jdbc:sqlite:"+dbName;
        if (!":memory:".equals(dbName) && !"".equals(dbName)/* temporary */) {
            Catalog catalog = this.metaDb.selectCatalog(dbName);
            if (catalog == null || !dbFileExists(dbName, catalog.getDir())) {
                trace(log, "Database '{}' {} not exists", dbName, (catalog == null? "catalog": "file"));
                SQLiteErrorCode error = SQLiteErrorCode.SQLITE_ERROR;
                throw new SQLException(error.message, "08001", error.code);
            }
            url = String.format("jdbc:sqlite:%s", getDbFile(dbName, catalog.getDir()));
        }
        trace(log, "SQLite connection {}", url);
        
        // Here shouldn't use SQLiteConfig.createConnection() to create connection:
        // 1. It maybe lead to busy issue for applying some PRAGMA commands.
        // 2. The DB resources can't be released when any SQL exception occurs.
        SQLiteConnection conn = JDBC.createConnection(url, new Properties());
        boolean failed = true;
        try {
            dbIdle();
            failed = false;
            return conn;
        } finally {
            if (failed) {
                conn.close();
            }
        }
    }
    
    /**
     * Maybe block so that shouldn't call it when creating it.
     */
    public void initConnection(SQLiteConnection connection) throws SQLException {
        initConnection(connection, 0/* non-blocking mode*/);
    }
    
    /**
     * Maybe block so that shouldn't call it when creating it.
     */
    public void initConnection(SQLiteConnection connection, int busyTimeout) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(format("pragma %s=%s", Pragma.BUSY_TIMEOUT, busyTimeout));
            stmt.execute(format("pragma %s=%s", Pragma.JOURNAL_MODE, this.journalMode));
            stmt.execute(format("pragma %s=%s", Pragma.SYNCHRONOUS, this.synchronous));
            stmt.execute(format("pragma %s=%s", Pragma.FOREIGN_KEYS, true));
            stmt.execute(format("pragma %s=%s", Pragma.ENCODING, Encoding.UTF8));
        }
    }
    
    public boolean isStopped() {
        return this.stopped;
    }
    
    public boolean isTrace() {
        return this.trace;
    }
    
    public boolean isTraceError() {
        return this.traceError;
    }
    
    public int getAuthTimeout() {
        return this.authTimeout;
    }
    
    public int getBusyTimeout() {
        return this.busyTimeout;
    }
    
    public int getOpenTimeout() {
        return this.openTimeout;
    }
    
    public String getName() {
        return NAME;
    }
    
    public int getPort() {
        return this.port;
    }
    
    public String getHost() {
        return this.host;
    }
    
    public String getProtocol() {
        return this.protocol;
    }
    
    public File getDataDir() {
        return this.dataDir;
    }
    
    public File getDbFile(String dbName, String dataDir) {
        File dirFile = this.dataDir;
        if (dataDir != null) {
            dirFile = new File(dataDir);
        }
        return new File(dirFile, dbName);
    }
    
    public File getDbFile(String dbName) {
        return new File(this.dataDir, dbName);
    }
    
    protected SQLiteMetaDb getMetaDb() {
        return this.metaDb;
    }
    
    public int getMaxConns() {
        return this.maxConns;
    }
    
    public long getMaxAllowedPacket() {
        return maxAllowedPacket;
    }
    
    public String getVersion() {
        return VERSION;
    }
    
    protected String getUsername() {
        return this.username;
    }
    
    protected String getPassword() {
        return this.password;
    }
    
    protected String getDbName() {
        return this.dbName;
    }
    
    public List<SQLiteProcessorState> getProcessorStates(SQLiteProcessor processor) {
        List<SQLiteProcessorState> states = new ArrayList<>();
        SQLiteWorker[] workers = this.workers;
        for (int i = 0, n = workers.length; i < n; ++i) {
            SQLiteWorker worker = workers[i];
            if (worker == null || !worker.isOpen()) {
                continue;
            }
            states.addAll(worker.getProcessorStates(processor));
        }
        return states;
    }
    
    public int getSleepTimeout() {
        return this.sleepTimeout;
    }
    
    public int getSleepInTxTimeout() {
        return this.sleepInTxTimeout;
    }
    
    public boolean inDataDir(String filename) {
        return inDataDir(filename, this.dataDir);
    }
    
    public boolean inDataDir(String filename, File dataDir) {
        File dbFile = new File(dataDir, filename);
        return (dbFile.getParentFile().equals(dataDir) 
                && dbFile.getName().equals(filename));
    }
    
    public boolean dbFileExists(String filename) {
        return (dbFileExists(filename, this.dataDir));
    }
    
    public boolean dbFileExists(String filename, File dataDir) {
        File dbFile = new File(dataDir, filename);
        return (inDataDir(filename, dataDir) && dbFile.isFile());
    }
    
    public boolean dbFileExists(String filename, String dataDir) {
        File dirFile = this.dataDir;
        if (dataDir != null) {
            dirFile = new File(dataDir);
        }
        File dbFile = new File(dirFile, filename);
        return (inDataDir(filename, dirFile) && dbFile.isFile());
    }
    
    public String getAuthMethod() {
        return this.authMethod;
    }
    
    /**
     * Default authentication method
     * @return the default authentication method
     */
    public abstract String getAuthDefault();
    
    /**
     * Authentication method list, separated by ','
     * @return Authentication method list
     */
    public abstract String getAuthMethods();
    
    public long getStartMillis() {
        return this.startMillis;
    }
    
    public long getStartNanos() {
        return this.startNanos;
    }
    
    public void flushCatalogs() {
        this.metaDb.flushCatalogs();
    }
    
    public void flushHosts() {
        this.metaDb.flushHosts();
    }
    
    public void flushPrivileges() {
        this.metaDb.flushPrivileges();
    }
    
    public boolean hasPrivilege(User user, String command) throws SQLException {
        return (this.metaDb.hasPrivilege(user, command));
    }
    
    public boolean hasPrivilege(String host, String user, String db, String command, String dataDir) 
            throws SQLException {
        return (this.metaDb.hasPrivilege(host, user, db, command, dataDir));
    }
    
    public void trace(Logger log, String message) {
        if (isTrace()) {
            log.info(message);
        }
    }
    
    public void trace(Logger log, String format, Object ... args) {
        if (isTrace()) {
            log.info(format, args);
        }
    }
    
    public void traceError(Logger log, String message, Throwable cause) {
        if (isTraceError()) {
            log.warn(message, cause);
        }
    }
    
    public void traceError(Logger log, String tag, String message, Throwable cause) {
        if (isTraceError()) {
            log.warn(tag + ": " + message, cause);
        }
    }
    
    protected abstract SQLiteProcessor newProcessor(SocketChannel channel, int id)
            throws NetworkException;
    
    public abstract SQLiteAuthMethod newAuthMethod(String protocol, String authMethod);
    
    public boolean tryDbWriteLock(SQLContext context) {
        String db = context.getDbName();
        SQLContext oldOne = this.dbWriteLocks.putIfAbsent(db, context);
        return (oldOne == null || oldOne == context);
    }
    
    public boolean dbWriteUnlock(SQLContext context) {
        String db = context.getDbName();
        if (db == null) {
            return false;
        }
        return this.dbWriteLocks.remove(db, context);
    }
    
    public boolean holdsDbWriteLock(SQLContext context) {
        String db = context.getDbName();
        return (this.dbWriteLocks.get(db) == context);
    }
    
    public boolean canHoldDbWriteLock(SQLContext context) {
        String db = context.getDbName();
        SQLContext holder = this.dbWriteLocks.get(db);
        return (holder == null || holder == context);
    }
    
    @Override
    public String toString() {
        return getName();
    }
    
    protected int nextPid() {
        int id = ++this.maxPid;
        if(id < 1){
            id = this.maxPid = 1;
        }
        
        return id;
    }
    
    protected void help(int status, String command) {
        help(status, command, null);
    }
    
    protected void help(int status, String command, String message) {
        if (message != null) {
            PrintStream out;
            if (status == 0) {
                out = System.out;
                message = "[INFO ] " + message;
            } else {
                out = System.err;
                message = "[ERROR] " + message;
            }
            out.println(message);
        }
        switch (command) {
        case CMD_INITDB:
            helpInitDb(status);
            break;
        case CMD_BOOT:
            helpBoot(status);
            // continue boot
            break;
        default:
            help(1);
            break;
        }
    }
    
    protected void help(int status) {
        doHelp(status, getHelp());
    }
    
    protected void helpInitDb(int status) {
        doHelp(status, getInitDbHelp());
    }
    
    protected void helpBoot(int status) {
        doHelp(status, getBootHelp());
    }
    
    protected String getHelp() {
        return getHelp(getClass(), getName(), getVersion());
    }
    
    protected String getInitDbHelp() {
        return getName() + " " + getVersion() + " since 2019\n" +
                "Usage: java "+getClass().getName()+" "+CMD_INITDB+" [OPTIONS]\n" +
                "  --auth-method|-A<authMethod>  \tAvailable auth methods("+getAuthMethods()+"), default "+getAuthDefault()+"\n"+
                "  --data-dir|-D   <path>        \tSQLite server data dir, default sqlite3Data in user home\n"+
                "  --db|-d         <dbName>      \tInitialized database, default as the user name\n"+
                "  --help|-h|-?                  \tShow this message\n" +
                "  --host|-H       <host>        \tSuperuser's login host, IP, or '%', default "+HOST_DEFAULT+"\n"+
                "  --journal-mode  <mode>        \tSQLite journal mode, default "+JOURNAL_MODE_DEFAULT+"\n"+
                "  --password|-p   <password>    \tSuperuser's password, must be provided in non-trust auth\n"+
                "  --protocol      <pg>          \tSQLite server protocol, default pg\n"+
                "  --synchronous|-S<sync>        \tSQLite synchronous mode, default "+SYNCHRONOUS_DEFAULT+ "\n"+
                "  --trace|-T                    \tTrace SQLite server execution\n" +
                "  --trace-error                 \tTrace error information of SQLite server execution\n"+
                "  --user|-U       <user>        \tSuperuser's name, default "+USER_DEFAULT;
    }
    
    protected String getBootHelp() {
        return getName() + " " + getVersion() + " since 2019\n" +
                "Usage: java "+getClass().getName()+" "+CMD_BOOT+" [OPTIONS]\n"+
                "  --busy-timeout  <millis>      \tSQL statement busy timeout, default "+BUSY_TIMEOUT_DEFAULT+"ms\n"+
                "  --data-dir|-D   <path>        \tSQLite server data dir, default sqlite3Data in user home\n"+
                "  --help|-h|-?                  \tShow this message\n" +
                "  --host|-H       <host>        \tSQLite server listen host or IP, default "+HOST_DEFAULT+"\n"+
                "  --journal-mode  <mode>        \tSQLite journal mode, default "+JOURNAL_MODE_DEFAULT+"\n"+
                "  --max-allowed-packet <number> \tMax allowed packet size, default " + MAX_ALLOWED_PACKET_DEFAULT+"B\n"+
                "  --max-conns     <number>      \tMax client connections limit, default "+MAX_CONNS_DEFAULT+"\n"+
                "  --open-timeout  <millis>      \tOpen SQLite database timeout, default "+OPEN_TIMEOUT_DEFAULT+"ms\n"+
                "  --port|-P       <number>      \tSQLite server listen port, default "+PORT_DEFAULT+"\n"+
                "  --protocol      <pg>          \tSQLite server protocol, default pg\n"+
                "  --trace|-T                    \tTrace SQLite server execution\n" +
                "  --trace-error                 \tTrace error information of SQLite server execution\n"+
                "  --sleep-timeout <millis>      \tProcess sleep timeout when idle, default "+SLEEP_TIMEOUT_DEFAULT+"ms\n"+
                "  --sleep-in-tx-timeout <millis>\tProcess sleep timeout in transaction, default "+SLEEP_IN_TX_TIMEOUT_DEFAULT+"ms\n"+
                "  --synchronous|-S<sync>        \tSQLite synchronous mode, default "+SYNCHRONOUS_DEFAULT+ "\n"+
                "  --worker-count  <number>      \tSQLite worker number, default CPU cores and max "+MAX_WORKER_COUNT;
    }
    
    protected static void doHelp(int status, String message) {
        PrintStream out = System.out;
        if (status > 0) {
            out = System.err;
        }
        
        out.println(message);
        System.exit(status);
    }

    protected static String getHelp(Class<? extends SQLiteServer> serverClazz, String name, String version) {
        return name + " " + version + " since 2019\n" +
                "Usage: java "+serverClazz.getName()+" <COMMAND> [OPTIONS]\n"+
                "COMMAND: \n"+
                "  boot    \tBootstap SQLite server\n" +
                "  initdb  \tInitialize SQLite server database\n" +
                "  help    \tShow this help message";
    }
    
    private static String initSQLitedHome() {
        String sqlitedHome = System.getProperty("SQLITED_HOME");
        if (sqlitedHome == null) {
            sqlitedHome = System.getProperty("user.dir");
            System.setProperty("SQLITED_HOME", sqlitedHome);
        }
        
        return sqlitedHome;
    }

}
