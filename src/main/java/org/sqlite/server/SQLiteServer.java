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
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteConnection;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteConfig.Encoding;
import org.sqlite.SQLiteConfig.JournalMode;
import org.sqlite.SQLiteConfig.SynchronousMode;
import org.sqlite.server.pg.PgServer;
import org.sqlite.server.util.IoUtils;
import static org.sqlite.server.util.StringUtils.*;

/**The SQLite server that abstracts various server's protocol, based on TCP/IP, 
 * and can be started and stopped.
 * 
 * @author little-pan
 * @since 2019-09-01
 *
 */
public class SQLiteServer implements AutoCloseable {
    
    static final Logger log = LoggerFactory.getLogger(SQLiteServer.class);
    
    public static final String VERSION = "0.3.27";
    public static final String USER_DEFAULT = "root";
    public static final String METADB_NAME  = "sqlite3.meta";
    public static final int PORT_DEFAULT = 3272;
    public static final int MAX_CONNS_DEFAULT = 151;
    
    // command list
    public static final String CMD_INITDB = "initdb";
    public static final String CMD_BOOT   = "boot";
    public static final String CMD_HELP   = "help";
    
    protected SQLiteMetadb metadb;
    private SQLiteServer base;
    protected String command;
    
    private String user = USER_DEFAULT;
    private String password;
    protected String authMethod;
    
    protected String host = "localhost";
    protected int port = PORT_DEFAULT;
    protected int maxConns = MAX_CONNS_DEFAULT;
    protected File dataDir = new File(System.getProperty("user.home"), "sqlite3Data");
    protected boolean trace;
    
    private final AtomicInteger processCount = new AtomicInteger(0);
    private int maxProcessId;
    
    protected String protocol = "pg";
    protected ServerSocket serverSocket;
    final ConcurrentMap<Integer, Processor> processors = new ConcurrentHashMap<>();
    private volatile boolean stopped;
    
    public static void main(String args[]) {
        main(new SQLiteServer(), args);
    }
    
    public static void main(SQLiteServer server, String ... args) {
        try {
            if (args == null || args.length == 0) {
                server.help(1);
                return;
            }
            
            String command = args[0];
            if (CMD_INITDB.equals(command)) {
                server.initdb(args);
                return;
            }
            
            server.boot(args);
        } catch (Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            server.help(1);
        } finally {
            IoUtils.close(server);
        }
    }
    
    public void initdb(String ... args) {
        init(args);
        
        String authMethod = getAuthMethod();
        if (authMethod == null) {
            help(1, this.command, "No auth method provided");
        }
        
        // Create super user
        try {
            SQLiteUser sa = new SQLiteUser(this.user, this.password, SQLiteUser.SUPER);
            sa.setAuthMethod(authMethod);
            sa.setHost(this.host);
            sa.setProtocol(this.protocol);
            this.metadb.createUser(sa);
        } catch (SQLException e) {
            SQLiteErrorCode existError = SQLiteErrorCode.SQLITE_CONSTRAINT;
            if (existError.code == e.getErrorCode()) {
                throw new IllegalStateException("Data dir had been initialized");
            }
            throw new IllegalStateException("Initdb fatal: " + e.getMessage(), e);
        }
    }
    
    protected void initDataDir() {
        Connection metaConn = null;
        boolean failed = true;
        try {
            this.dataDir = this.dataDir.getCanonicalFile();
            File baseDir = this.dataDir;
            if (!baseDir.isDirectory() && !baseDir.mkdirs()) {
                throw new IllegalStateException("Can't mkdirs data dir " + baseDir);
            }
            
            // init metaDb
            IoUtils.close(this.metadb);
            SQLiteConfig config = new SQLiteConfig();
            config.setJournalMode(JournalMode.DELETE);
            config.setBusyTimeout(50000);
            config.setEncoding(Encoding.UTF8);
            File metaFile = new File(baseDir, METADB_NAME);
            metaConn = config.createConnection("jdbc:sqlite:"+metaFile);
            this.metadb = new SQLiteMetadb(metaFile, (SQLiteConnection)metaConn);
            failed = false;
        } catch (IOException | SQLException cause) {
            throw new IllegalStateException(cause);
        } finally {
            if (failed) {
                IoUtils.close(metaConn);
            }
        }
    }
    
    public void boot(String... args) {
        try {
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
    
    public void init(String... args) {
        boolean isSelf = (getClass() == SQLiteServer.class);
        boolean help = false;
        int i = 0;
        
        // Check command
        if(args == null || args.length < 1) {
            help(1);
        }
        String command = this.command = args[i++];
        switch (command) {
        case CMD_INITDB:
        case CMD_BOOT:
            break;
        case CMD_HELP:
            help(0);
            break;
        default:
            help(1);
            break;
        }
        
        // Parse args
        for (int argc = args.length; i < argc; i++) {
            String a = args[i];
            if ("--trace".equals(a) || "-T".equals(a)) {
                trace = true;
            } else if ("--user".equals(a) || "-U".equals(a)) {
                user = args[++i];
            } else if ("--password".equals(a) || "-p".equals(a)) {
                password = args[++i];
            } else if ("--host".equals(a) || "-H".equals(a)) {
                host = args[++i];
            } else if ("--port".equals(a) || "-P".equals(a)) {
                port = Integer.decode(args[++i]);
            } else if ("--data-dir".equals(a) || "-D".equals(a)) {
                dataDir = new File(args[++i]);
            } else if ("--max-conns".equals(a)) {
                maxConns = Integer.decode(args[++i]);
            } else if ("--auth-method".equals(a) || "-A".equals(a)) {
                this.authMethod = toLowerEnglish(args[++i]);
            } else if ("--protocol".equals(a) && isSelf && this.base==null) {
                String proto = this.protocol = toLowerEnglish(args[++i]);
                if ("pg".equals(proto)) this.base = new PgServer();
                else help(1);
            } else if ("--help".equals(a) || "-h".equals(a) || "-?".equals(a)) {
                help = true;
            }
        }
        
        if (isSelf) {
            if (this.base == null) {
                this.base = new PgServer();
            }
            this.base.init(args);
        }
        
        if (help) {
            help(0, this.command);
        }
        
        initDataDir();
    }
    
    public void start() throws NetworkException {
        if (isStopped()) {
            throw new IllegalStateException("Server has been stopped");
        }
        
        try {
            InetAddress addr = InetAddress.getByName(getHost());
            this.serverSocket = new ServerSocket(getPort(), getMaxConns(), addr);
        } catch (IOException e) {
            throw new NetworkException("Can't create server socket", e);
        }
    }
    
    public void listen() {
        try {
            Thread.currentThread().setName(getName());
            
            log.info("Ready for connections on {}:{}, version {}", getHost(), getPort(), getVersion());
            while (!isStopped()) {
                Socket s = this.serverSocket.accept();
                try {
                    trace(log, "Connection {}", s);
                    
                    incrProcessCount();
                    if (this.processCount.get() > this.maxConns) {
                        IoUtils.close(s);
                        decrProcessCount();
                        
                        trace(log, "Exceeds maxConns limit({}), close connection", getMaxConns());
                        continue;
                    }
                    s.setTcpNoDelay(true);
                    s.setKeepAlive(true);
                    
                    int processId = nextProcessId();
                    Processor processor = newProcessor(s, processId);
                    if (processors.putIfAbsent(processId, processor) != null) {
                        IoUtils.close(s);
                        decrProcessCount();
                        log.error("Processor-{} exists, close the current", processId);
                        continue;
                    }
                    
                    processor.start();
                    continue;
                } catch (Throwable cause) {
                    IoUtils.close(s);
                    decrProcessCount();
                    
                    traceError(log, getName(), "can't create processor", cause);
                }
            }
        } catch (IOException e) {
            if (!this.serverSocket.isClosed()) {
                log.error(getName()+" fatal", e);
            }
        } finally {
            doStop();
        }
    }
    
    public void stop() {
        if (!isStopped()) {
            this.stopped = true;
            doStop();
        }
    }
    
    protected void doStop() {
        // 1. Close this server
        IoUtils.close(this.serverSocket);
        // 2. Stop all processors
        for(Processor p : this.processors.values()) {
            p.stop();
        }
        // 3. Close metaDb
        IoUtils.close(this.metadb);
    }
    
    @Override
    public void close() {
        this.stop();
    }
    
    public boolean isOpen() {
        return (!this.isStopped());
    }
    
    public Processor removeProcessor(Processor processor) {
        int id = processor.getId();
        
        if (getProcessor(id) == processor) {
            Processor proc = this.processors.remove(id);
            decrProcessCount();
            return proc;
        }
        return null;
    }
    
    public SQLiteConnection newSQLiteConnection(String databaseName) throws SQLException {
        String url = "jdbc:sqlite:"+databaseName;
        if (!":memory:".equals(databaseName) && !"".equals(databaseName)/* temporary */) {
            url = String.format("jdbc:sqlite:%s", getDbFile(databaseName));
        }
        trace(log, "SQLite connection {}", url);
        
        SQLiteConfig config = new SQLiteConfig();
        config.setJournalMode(JournalMode.WAL);
        config.setSynchronous(SynchronousMode.NORMAL);
        config.setBusyTimeout(50000);
        config.enforceForeignKeys(true);
        config.setEncoding(Encoding.UTF8);
        
        return (SQLiteConnection)config.createConnection(url);
    }
    
    public Processor getProcessor(int id) {
        return this.processors.get(id);
    }
    
    public boolean isStopped() {
        return this.stopped;
    }
    
    public boolean isTrace() {
        return this.trace;
    }
    
    public String getName() {
        return "SQLite server";
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
    
    public File getDbFile(String dbName) {
        return new File(getDataDir(), dbName);
    }
    
    public int getMaxConns() {
        return this.maxConns;
    }
    
    public String getVersion() {
        return VERSION;
    }
    
    public String getUser() {
        return this.user;
    }
    
    public String getPassword() {
        return this.password;
    }
    
    public String getAuthMethod() {
        if (this.base == null) {
            return this.authMethod;
        }
        
        return this.base.getAuthMethod();
    }
    
    /**
     * Default authentication method
     * @return the default authentication method, or "" if none
     */
    public String getAuthDefault() {
        if (this.base == null) {
            return "";
        }
        
        return this.base.getAuthDefault();
    }
    
    /**
     * Authentication method list, separated by ','
     * @return Authentication method list, or "" if none
     */
    public String getAuthMethods() {
        if (this.base == null) {
            return "";
        }
        
        return this.base.getAuthMethods();
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
        if (isTrace()) {
            log.warn(message, cause);
        }
    }
    
    public void traceError(Logger log, String tag, String message, Throwable cause) {
        if (isTrace()) {
            log.warn(tag + ": " + message, cause);
        }
    }
    
    protected Processor newProcessor(Socket s, int id) {
        if (this.base == null) {
            throw new UnsupportedOperationException("Not implemented");
        }
        
        return this.base.newProcessor(s, id);
    }
    
    protected int incrProcessCount() {
        return this.processCount.incrementAndGet();
    }
    
    protected int decrProcessCount() {
        return this.processCount.decrementAndGet();
    }
    
    protected int nextProcessId() {
        int id = ++this.maxProcessId;
        if(id < 1){
            id = this.maxProcessId = 1;
        }
        
        return id;
    }
    
    protected void doHelp(int status, String message) {
        PrintStream out = System.out;
        if (status > 0) {
            out = System.err;
        }
        
        out.println(message);
        System.exit(status);
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
        if (this.base != null) {
            return this.base.getHelp();
        }
        
        return getName() + " " + getVersion() + " since 2019\n" +
                "Usage: java "+getClass().getName()+" <COMMAND> [OPTIONS]\n"+
                "COMMAND: \n"+
                "  initdb  Initialize SQLite server database\n" +
                "  boot    Bootstap SQLite server\n" +
                "  help    Show this help message";
    }
    
    protected String getInitDbHelp() {
        if (this.base != null) {
            return this.base.getInitDbHelp();
        }
        
        return getName() + " " + getVersion() + " since 2019\n" +
                "Usage: java "+getClass().getName()+" "+CMD_INITDB+" [OPTIONS]\n" +
                "  --help|-h|-?                  Show this message\n" +
                "  --data-dir|-D   <path>        SQLite server data dir, default sqlite3Data in user home\n"+
                "  --user|-U       <user>        Superuser's name, default "+USER_DEFAULT+"\n"+
                "  --password|-p   <password>    Superuser's password, must be provided in non-trust auth\n"+
                "  --host|-H       <host>        Superuser's host, default localhost\n"+
                "  --protocol      <pg>          SQLite server protocol, default pg\n"+
                "  --auth-method|-A <authMethod> Available auth methods("+getAuthMethods()+"), default '"+getAuthDefault()+"'";
    }
    
    protected String getBootHelp() {
        if (this.base != null) {
            return this.base.getBootHelp();
        }
        
        
        return getName() + " " + getVersion() + " since 2019\n" +
                "Usage: java "+getClass().getName()+" "+CMD_BOOT+" [OPTIONS]\n"+
                "  --help|-h|-?                  Show this message\n" +
                "  --data-dir|-D   <path>        SQLite server data dir, default sqlite3Data in user home\n"+
                "  --host|-H       <host>        SQLite server listen host or IP, default localhost\n"+
                "  --port|-P       <number>      SQLite server listen port, default "+PORT_DEFAULT+"\n"+
                "  --max-conns     <number>      Max client connections limit, default "+MAX_CONNS_DEFAULT+"\n"+
                "  --trace|-T                    Trace SQLite server execution\n" +
                "  --protocol      <pg>          SQLite server protocol, default pg";
    }

}
