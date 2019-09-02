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
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.util.IoUtils;

/**The server that abstracts various protocol, based on TCP/IP, and 
 * can be started and stopped.
 * 
 * @author little-pan
 * @since 2019-09-01
 *
 */
public abstract class Server {
    static final Logger log = LoggerFactory.getLogger(Server.class);
    
    public static final String VERSION = "0.3.27";
    public static final int PORT_DEFAULT = 3272;
    public static final int MAX_CONNS_DEFAULT = 151;
    
    protected String host = "localhost";
    protected int port = PORT_DEFAULT;
    protected int maxConns = MAX_CONNS_DEFAULT;
    protected File dataDir = new File(System.getProperty("user.home"), "sqlite3Data");
    protected boolean trace;
    
    private final AtomicInteger processCount = new AtomicInteger(0);
    private int maxProcessId;
    
    protected ServerSocket serverSocket;
    protected final ConcurrentMap<Integer, Processor> processors = new ConcurrentHashMap<>();
    private volatile boolean stopped;
    
    public void init(String... args) {
        if(args == null) {
            return;
        }
        
        for (int i = 0, argc = args.length; i < argc; i++) {
            String a = args[i];
            if ("--trace".equals(a) || "-T".equals(a)) {
                trace = true;
            } else if ("--host".equals(a) || "-H".equals(a)) {
                host = args[++i];
            } else if ("--port".equals(a) || "-P".equals(a)) {
                port = Integer.decode(args[++i]);
            } else if ("--data-dir".equals(a) || "-D".equals(a)) {
                dataDir = new File(args[++i]);
            } else if ("--max-conns".equals(a)) {
                maxConns = Integer.decode(args[++i]);
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
            
            log.info("{}: ready for connections on {}:{}, version {}", 
                 getName(), getHost(), getPort(), getVersion());
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
            IoUtils.close(this.serverSocket);
            this.serverSocket = null;
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
    
    public Processor getProcessor(int id) {
        return this.processors.get(id);
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
    
    protected abstract Processor newProcessor(Socket s, int processId);
    
    protected int incrProcessCount(){
        return this.processCount.incrementAndGet();
    }
    
    protected int decrProcessCount(){
        return this.processCount.decrementAndGet();
    }
    
    protected int nextProcessId(){
        int id = ++this.maxProcessId;
        if(id < 1){
            id = this.maxProcessId = 1;
        }
        
        return id;
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
    
}
