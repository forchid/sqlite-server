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
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteConnection;
import org.sqlite.SQLiteConfig.Encoding;
import org.sqlite.SQLiteConfig.JournalMode;
import org.sqlite.SQLiteConfig.SynchronousMode;
import org.sqlite.server.pg.PgServer;
import org.sqlite.server.util.IoUtils;
import org.sqlite.server.util.StringUtils;

/**The SQLite server that abstracts various server's protocol, based on TCP/IP, 
 * and can be started and stopped.
 * 
 * @author little-pan
 * @since 2019-09-01
 *
 */
public class SQLiteServer {
    
    static final Logger log = LoggerFactory.getLogger(SQLiteServer.class);
    
    public static final String VERSION = "0.3.27";
    public static final String USER_DEFAULT = "root";
    public static final int PORT_DEFAULT = 3272;
    public static final int MAX_CONNS_DEFAULT = 151;
    
    private SQLiteServer base;
    
    private String user = USER_DEFAULT;
    private String password;
    
    protected String host = "localhost";
    protected int port = PORT_DEFAULT;
    protected int maxConns = MAX_CONNS_DEFAULT;
    protected File dataDir = new File(System.getProperty("user.home"), "sqlite3Data");
    protected boolean trace;
    
    private final AtomicInteger processCount = new AtomicInteger(0);
    private int maxProcessId;
    
    protected ServerSocket serverSocket;
    final ConcurrentMap<Integer, Processor> processors = new ConcurrentHashMap<>();
    private volatile boolean stopped;
    
    public static void main(String args[]) {
        SQLiteServer server = new SQLiteServer();
        try {
            server.boot(args);
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            server.help(1);
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
        if(args == null) {
            return;
        }
        boolean isSelf = (getClass() == SQLiteServer.class);
        
        for (int i = 0, argc = args.length; i < argc; i++) {
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
            } else if ("--protocol".equals(a) && isSelf && this.base==null) {
                String proto = StringUtils.toLowerEnglish(args[++i]);
                if ("pg".equals(proto)) {
                    this.base = new PgServer();
                } else {
                    help(1);
                }
            } else if ("--help".equals(a) || "-h".equals(a) || "-?".equals(a)) {
                help(0);
            } 
        }
        
        // init dataDir
        try {
            this.dataDir = this.dataDir.getCanonicalFile();
            File baseDir = this.dataDir;
            if (!baseDir.isDirectory() && !baseDir.mkdirs()) {
                throw new IllegalStateException("Can't mkdirs data dir " + baseDir);
            }
        } catch (IOException cause) {
            throw new IllegalStateException(cause);
        }
        
        if (isSelf) {
            if (this.base == null) {
                this.base = new PgServer();
            }
            this.base.init(args);
        }
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
            
            log.info("Ready for connections on {}:{}", getHost(), getPort());
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
        }
    }
    
    public void stop() {
        if (!isStopped()) {
            this.stopped = true;
            
            // 1. Close this server
            IoUtils.close(this.serverSocket);
            
            // 2. Stop all processors
            for(Processor p : this.processors.values()) {
                p.stop();
            }
        }
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
        String url = "jdbc:sqlite::memory:";
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
        return "SQLite server " + VERSION;
    }
    
    public int getPort() {
        return this.port;
    }
    
    public String getHost() {
        return this.host;
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
    
    public void help(int status) {
        PrintStream out = System.out;
        String message;
        
        if (this.base == null) {
            message = getHelp();
        } else {
            message = this.base.getHelp();
        }
        
        if (status > 0) {
            out = System.err;
        }
        
        out.println(message);
        System.exit(status);
    }
    
    public String getHelp() {
        return "Usage: java "+getClass().getName()+" [OPTIONS]\n"+
                "OPTIONS: \n"+
                "  --help|-h|-?                  Show this message\n" +
                "  --data-dir|-D   <path>        Server data directory, default data in work dir\n"+
                "  --user|-U       <user>        User's name, default "+USER_DEFAULT+"\n"+
                "  --password|-p   <password>    User's password, must be provided in md5 auth\n"+
                "  --host|-H       <host>        Server listen host or IP, default localhost\n"+
                "  --port|-P       <number>      Server listen port, default "+PORT_DEFAULT+"\n"+
                "  --max-conns     <number>      Max connections limit, default "+MAX_CONNS_DEFAULT+"\n"+
                "  --trace|-T                    Trace the server execution\n" +
                "  --protocol      <pg>          The server protocol, default pg";
    }
    
}
