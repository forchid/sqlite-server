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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConnection;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.server.meta.User;
import org.sqlite.sql.SQLStatement;
import org.sqlite.sql.meta.AlterUserStatement;
import org.sqlite.sql.meta.CreateUserStatement;
import org.sqlite.sql.meta.MetaStatement;
import org.sqlite.util.IoUtils;

/**
 * The SQLite server protocol handler.
 * 
 * @author little-pan
 * @since 2019-09-01
 *
 */
public abstract class SQLiteProcessor implements AutoCloseable {
    
    static final Logger log = LoggerFactory.getLogger(SQLiteProcessor.class);
    
    protected final InetSocketAddress remoteAddress;
    protected final SocketChannel channel;
    protected Selector selector;
    protected Deque<ByteBuffer> writeBufferQueue;
    protected Deque<WriteTask> writeTaskQueue;
    
    protected final int id;
    protected final String name;
    protected final SQLiteServer server;
    protected SQLiteWorker worker;
    protected SQLiteAuthMethod authMethod;
    protected User user;
    
    private volatile boolean open = true;
    private volatile boolean stopped;
    
    private SQLiteConnection connection;
    private String metaSchema = null;
    
    protected SQLiteProcessor(SocketChannel channel, int processId, SQLiteServer server)
            throws NetworkException {
        this.channel = channel;
        this.server = server;
        this.id = processId;
        this.name = String.format("proc-%d", processId);
        
        try {
            this.remoteAddress = (InetSocketAddress)channel.getRemoteAddress();
        } catch (IOException e) {
            throw new NetworkException(e);
        }
        
        this.writeTaskQueue = new ArrayDeque<>();
        this.writeBufferQueue = new ArrayDeque<>();
    }
    
    public int getId() {
        return this.id;
    }
    
    public String getName() {
        return this.name;
    }
    
    public SQLiteServer getServer() {
        return this.server;
    }
    
    protected SocketChannel getChannel() {
        return this.channel;
    }
    
    protected Selector getSelector() {
        return selector;
    }
    
    protected void setSelector(Selector selector) {
        this.selector = selector;
    }
    
    protected SQLiteWorker getWorker() {
        return this.worker;
    }
    
    protected void setWorker(SQLiteWorker worker) {
        this.worker = worker;
    }
    
    protected SQLiteMetaDb getMetaDb() {
        return getServer().getMetaDb();
    }
    
    protected String getMetaSchema() {
        return this.metaSchema;
    }
    
    protected void attachMetaDb(SQLiteConnection conn) throws SQLException {
        if (this.metaSchema == null) {
            this.metaSchema = getMetaDb().attachTo(conn);
            this.server.trace(log, "attach {}", this.metaSchema);
        }
    }
    
    protected void detachMetaDb(SQLiteConnection conn) throws SQLException {
        if (this.metaSchema == null || !conn.getAutoCommit()) {
            return;
        }
        getMetaDb().detachFrom(conn, this.metaSchema);
        this.server.trace(log, "detach {}", this.metaSchema);
        this.metaSchema = null;
    }
    
    protected String getSQL(SQLStatement stmt) throws SQLException {
        if (stmt.isMetaStatement()) {
            attachMetaDb(getConnection());
            String schema = getMetaSchema();
            MetaStatement metaStmt = (MetaStatement)stmt;
            switch (stmt.getCommand()) {
            case "ALTER USER":
                AlterUserStatement auStmt = (AlterUserStatement)stmt;
                if (auStmt.getPassword() != null && !auStmt.isPasswordSet()) {
                    String proto = auStmt.getProtocol(), method = auStmt.getAuthMethod();
                    SQLiteAuthMethod authMethod = this.server.newAuthMethod(proto, method);
                    String p = authMethod.genStorePassword(auStmt.getUser(), auStmt.getPassword());
                    auStmt.setPassword(p);
                    auStmt.setPasswordSet(true);
                }
                break;
            case "CREATE USER":
                CreateUserStatement cuStmt = (CreateUserStatement)stmt;
                if (cuStmt.getPassword() != null && !cuStmt.isPasswordSet()) {
                    String proto = cuStmt.getProtocol(), method = cuStmt.getAuthMethod();
                    SQLiteAuthMethod authMethod = this.server.newAuthMethod(proto, method);
                    String p = authMethod.genStorePassword(cuStmt.getUser(), cuStmt.getPassword());
                    cuStmt.setPassword(p);
                    cuStmt.setPasswordSet(true);
                }
                break;
            }
            return metaStmt.getMetaSQL(schema);
        }
        
        return stmt.getSQL();
    }
    
    public void setConnection(SQLiteConnection connection) {
        if (!isOpen()) {
            throw new IllegalStateException("Processor has been closed");
        }
        
        if (connection == null) {
            throw new NullPointerException("connection");
        }
        
        if (this.connection != null) {
            throw new IllegalStateException("connection has been set");
        }
        
        this.connection = connection;
    }
    
    public SQLiteConnection getConnection() {
        return this.connection;
    }
    
    public InetSocketAddress getRemoteAddress() {
        return this.remoteAddress;
    }
    
    public void cancelRequest() throws SQLException {
        SQLiteConnection conn = getConnection();
        if (conn != null && isOpen()) {
            conn.getDatabase().interrupt();
        }
    }
    
    protected void checkPerm(SQLStatement stmt) throws SQLException {
        if (stmt.isMetaStatement()) {
            MetaStatement metaStmt = (MetaStatement)stmt;
            if (metaStmt.needSa() && !this.user.isSa()) {
                throw convertError(SQLiteErrorCode.SQLITE_PERM);
            }
        }
    }
    
    protected SQLException convertError(SQLiteErrorCode error) {
        return convertError(error, null, null);
    }
    
    protected SQLException convertError(SQLiteErrorCode error, String message) {
        return convertError(error, message, null);
    }
    
    protected SQLException convertError(SQLiteErrorCode error, String message, String sqlState) {
        if (sqlState == null) {
            if (SQLiteErrorCode.SQLITE_ERROR.code == error.code) {
                sqlState = "42000";
            } else if (SQLiteErrorCode.SQLITE_PERM.code == error.code){
                sqlState = "42501";
            } else if (SQLiteErrorCode.SQLITE_INTERNAL.code == error.code) {
                sqlState = "58005";
            } else if (SQLiteErrorCode.SQLITE_IOERR.code == error.code) {
                sqlState = "58030";
            }
            if (sqlState == null) {
                sqlState = "HY000";
            }
        }
        
        if (message == null) {
            message = error.message;
        }
        return new SQLException(message, sqlState, error.code);
    }

    public void start() throws IOException, IllegalStateException {
        final String name = getName();
        if (isStopped()) {
            throw new IllegalStateException(name + " has been stopped");
        }
        
        SQLiteServer server = this.server;
        server.trace(log, "Connect: id {}", this.id);
        try {
            InetSocketAddress remoteAddr = this.remoteAddress;
            if (!server.isAllowed(remoteAddr)) {
                server.trace(log, "Host '{}' not allowed", remoteAddr.getHostName());
                deny(remoteAddr);
                return;
            }
            enableRead();
        } catch (SQLException e) {
            throw new IllegalStateException("Access metaDb fatal", e);
        }
    }
    
    protected boolean isRunning() throws SQLException {
        if (!isStopped()) {
            return true;
        }
        
        SQLiteConnection conn = getConnection();
        if (conn == null || conn.isClosed()) {
            return false;
        }
        return (!conn.getAutoCommit() /* Ongoing tx */);
    }
    
    protected abstract void deny(InetSocketAddress remote) throws IOException;
    
    protected void read() {
        try {
            process();
        } catch (IOException e) {
            this.server.traceError(log, "process error", e);
            stop();
            this.worker.close(this);
        }
    }
    
    protected void write() {
        try {
            flush();
        } catch (IOException |SQLException e) {
            this.server.traceError(log, "flush error", e);
            stop();
            this.worker.close(this);
        }
    }
    
    protected void flush() throws IOException, SQLException {
        SocketChannel ch = getChannel();
        int i = 0;
        
        disableRead();
        for (;;) {
            ByteBuffer buf = nextWriteBuffer();
            if (buf != null) {
                for (; buf.hasRemaining();) {
                    int n = ch.write(buf);
                    if (n == 0) {
                        break;
                    }
                    if (++i >= 1000) {
                        break;
                    }
                }
                if (buf.hasRemaining()) {
                    this.writeBufferQueue.offerFirst(buf);
                    return;
                }
                continue;
            }
            
            WriteTask task = nextWriteTask();
            if (task != null) {
                task.run();
                continue;
                
            }
            
            disableWrite();
            if (isRunning()) {
                enableRead();
            } else {
                this.worker.close(this);
            }
            break;
        }
    }
    
    protected ByteBuffer nextWriteBuffer() {
        return this.writeBufferQueue.poll();
    }
    
    protected void offerWriteBuffer(ByteBuffer writeBuffer) {
        this.writeBufferQueue.offer(writeBuffer);
    }
    
    protected WriteTask nextWriteTask() {
        return this.writeTaskQueue.poll();
    }
    
    protected void offerWriteTask(WriteTask task) {
        this.writeTaskQueue.offer(task);
    }
    
    protected void enableRead() throws IOException {
        SelectionKey key = this.channel.keyFor(this.selector);
        if (key == null) {
            this.channel.register(this.selector, SelectionKey.OP_READ, this);
        } else {
            key.interestOps(key.interestOps() | SelectionKey.OP_READ);
        }
    }
    
    protected void disableRead() throws IOException {
        SelectionKey key = this.channel.keyFor(this.selector);
        if (key != null) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
        }
    }
    
    protected void enableWrite() throws IOException {
        SelectionKey key = this.channel.keyFor(this.selector);
        if (key == null) {
            this.channel.register(this.selector, SelectionKey.OP_WRITE, this);
        } else {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        }
    }
    
    protected void disableWrite() throws IOException {
        SelectionKey key = this.channel.keyFor(this.selector);
        if (key != null) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        }
    }
    
    protected abstract void process() throws IOException;
    
    protected abstract void tooManyConns() throws IOException;
    
    protected abstract void interalError() throws IOException;
    
    public void stop() {
        this.stopped = true;
    }
    
    public boolean isStopped() {
        return this.stopped;
    }
    
    public boolean isOpen() {
        return this.open;
    }
    
    @Override
    public void close() {
        stop();
        if (!isOpen()) {
            return;
        }
        this.open = false;
        
        IoUtils.close(this.channel);
        IoUtils.close(this.connection);
        this.server.trace(log, "Close: id {}", this.id);
    }
    
    protected static abstract class WriteTask implements Runnable {
        
        final SQLiteProcessor proc;
        
        protected WriteTask(SQLiteProcessor proc) {
            this.proc = proc;
        }
        
        public void run() {
            try {
                write();
            } catch (IOException e) {
                this.proc.stop();
                this.proc.worker.close(this.proc);
            }
        }
        
        protected abstract void write() throws IOException;
    }
    
}
