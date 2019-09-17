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
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.Function;
import org.sqlite.SQLiteConnection;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.server.func.CurrentUserFunc;
import org.sqlite.server.func.StringResultFunc;
import org.sqlite.server.func.UserFunc;
import org.sqlite.server.func.VersionFunc;
import org.sqlite.server.meta.User;
import org.sqlite.sql.SQLStatement;
import org.sqlite.sql.TransactionStatement;
import org.sqlite.sql.meta.AlterUserStatement;
import org.sqlite.sql.meta.CreateUserStatement;
import org.sqlite.sql.meta.DropUserStatement;
import org.sqlite.sql.meta.GrantStatement;
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
    
    // Read settings
    static final int initReadBuffer = Integer.getInteger("org.sqlite.server.procssor.initReadBuffer", 1<<12);
    static final int maxReadBuffer  = Integer.getInteger("org.sqlite.server.procssor.maxReadBuffer",  1<<16);
    // Write settings
    static final int maxWriteTimes  = Integer.getInteger("org.sqlite.server.procssor.maxWriteTimes",  1<<10);
    static final int maxWriteQueue  = Integer.getInteger("org.sqlite.server.procssor.maxWriteQueue",  1<<10);
    static final int maxWriteBuffer = Integer.getInteger("org.sqlite.server.procssor.maxWriteBuffer", 1<<12);
    
    protected final InetSocketAddress remoteAddress;
    protected final SocketChannel channel;
    protected Selector selector;
    protected ByteBuffer readBuffer;
    protected Deque<ByteBuffer> writeQueue;
    protected WriteTask writeTask;
    
    protected final int id;
    protected final String name;
    protected final SQLiteServer server;
    protected SQLiteWorker worker;
    protected SQLiteAuthMethod authMethod;
    protected String databaseName;
    protected User user;
    
    private volatile boolean open = true;
    private volatile boolean stopped;
    
    private SQLiteConnection connection;
    private String metaSchema = null;
    protected final Stack<TransactionStatement> savepointStack;
    
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
        
        this.writeQueue = new ArrayDeque<>();
        this.savepointStack = new Stack<>();
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
    
    public void setConnection(SQLiteConnection connection) throws SQLException {
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
        // init
        String host = getRemoteAddress().getHostName();
        String version = this.server.getVersion();
        StringResultFunc strResFunc;
        
        strResFunc = new VersionFunc(this.server);
        Function.create(connection, strResFunc.getName(), strResFunc);
        strResFunc = new StringResultFunc("server_version", version);
        Function.create(connection, strResFunc.getName(), strResFunc);
        
        strResFunc = new UserFunc(this.user, host);
        Function.create(connection, strResFunc.getName(), strResFunc);
        strResFunc = new CurrentUserFunc(this.user);
        Function.create(connection, strResFunc.getName(), strResFunc);
        
        strResFunc = new StringResultFunc("database", this.databaseName);
        Function.create(connection, strResFunc.getName(), strResFunc);
        strResFunc = new StringResultFunc("current_database", this.databaseName);
        Function.create(connection, strResFunc.getName(), strResFunc);
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
        final User user = this.user;
        if (user.isSa() || stmt.isTransaction() || stmt.isEmpty()) {
            return;
        }
        
        if (stmt.isMetaStatement()) {
            MetaStatement metaStmt = (MetaStatement)stmt;
            if (metaStmt.needSa()) {
                // Modify my user information myself?
                switch (stmt.getCommand()) {
                case "ALTER USER":
                    AlterUserStatement as = (AlterUserStatement)stmt;
                    if (as.isUser(user.getHost(), user.getUser(), user.getProtocol())) {
                        // Pass
                        return;
                    }
                    break;
                case "DROP USER":
                    DropUserStatement ds = (DropUserStatement)stmt;
                    if (ds.exists(user.getHost(), user.getUser(), user.getProtocol())) {
                        // Pass
                        return;
                    }
                default:
                    throw convertError(SQLiteErrorCode.SQLITE_PERM);
                }
                // no SA
            }
        } else {
            String command = stmt.getCommand();
            if (this.server.hasPrivilege(user, command)) {
                return;
            }
            
            throw convertError(SQLiteErrorCode.SQLITE_PERM);
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
            if (SQLiteErrorCode.SQLITE_AUTH.code            == error.code) {
                sqlState = "28000";
            } else if (SQLiteErrorCode.SQLITE_ERROR.code    == error.code) {
                sqlState = "42000";
            } else if (SQLiteErrorCode.SQLITE_PERM.code     == error.code) {
                sqlState = "42501";
            } else if (SQLiteErrorCode.SQLITE_INTERNAL.code == error.code) {
                sqlState = "58005";
            } else if (SQLiteErrorCode.SQLITE_IOERR.code    == error.code) {
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
            this.worker.close(this);
        }
    }
    
    protected ByteBuffer getReadBuffer(final int minSize) {
        final ByteBuffer buf = this.readBuffer;
        if (buf == null) {
            int cap = Math.max(minSize, initReadBuffer);
            return (this.readBuffer = ByteBuffer.allocate(cap));
        }
        
        final int cap = buf.capacity(), pos = buf.position();
        int freeSize = cap - pos;
        if (freeSize >= minSize) {
            return buf;
        }
        
        final int lim = buf.limit();
        freeSize = Math.max(freeSize + cap, minSize);
        ByteBuffer newBuffer = ByteBuffer.allocate(pos + freeSize);
        buf.flip();
        newBuffer.put(buf);
        newBuffer.limit(lim);
        return (this.readBuffer = newBuffer);
    }
    
    protected int resetReadBuffer() {
        final ByteBuffer buf = this.readBuffer;
        if (buf == null) {
            return 0;
        }
        
        final int cap = buf.capacity();
        int rem = buf.remaining();
        if (rem == 0) {
            if (cap > maxReadBuffer) {
                this.readBuffer = null;
            } else {
                buf.clear();
            }
            return 0;
        }
        
        buf.compact();
        if (buf.position() <= initReadBuffer && cap > maxReadBuffer) {
            ByteBuffer newBuffer = ByteBuffer.allocate(initReadBuffer);
            buf.flip();
            newBuffer.put(buf);
            this.readBuffer = newBuffer;
        }
        
        return (this.readBuffer.position());
    }
    
    protected void write() {
        try {
            flush();
        } catch (IOException |SQLException e) {
            this.server.traceError(log, "flush error", e);
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
                    if (++i >= maxWriteTimes) {
                        break;
                    }
                }
                if (buf.hasRemaining()) {
                    this.writeQueue.offerFirst(buf);
                    return;
                }
                continue;
            }
            
            WriteTask task = this.writeTask;
            if (task != null) {
                task.run();
                continue;
            }
            
            disableWrite();
            if (isRunning()) {
                // Go on
                enableRead();
                ByteBuffer rb = this.readBuffer;
                if (rb != null && rb.position() > 0) {
                    read();
                }
            } else {
                this.worker.close(this);
            }
            break;
        }
    }
    
    protected boolean canFlush() {
        ByteBuffer buf = this.writeQueue.peek();
        if (buf == null) {
            return false;
        } else if (buf.remaining() >= maxWriteBuffer) {
            // Case-1
            return true;
        } else if (this.writeQueue.size() >= maxWriteQueue) {
            // Case-2
            return true;
        }
        
        return false;
    }
    
    protected ByteBuffer nextWriteBuffer() {
        return this.writeQueue.poll();
    }
    
    protected void offerWriteBuffer(ByteBuffer writeBuffer) {
        final ByteBuffer last = this.writeQueue.peekLast();
        final int minSize = writeBuffer.remaining();
        if (last == null || minSize > maxWriteBuffer) {
            this.writeQueue.offer(writeBuffer);
            return;
        }
        
        // Merge buffer: optimize write performance!
        final int lim = last.limit(), cap = last.capacity();
        int freeSize = cap - lim;
        if (freeSize >= minSize) {
            // Case-1 space enough
            last.position(lim).limit(cap);
            last.put(writeBuffer);
            last.flip();
            return;
        }
        // - Try to expand capacity
        freeSize = Math.max(freeSize + cap, minSize);
        final int newSize = lim + freeSize;
        if (newSize <= maxWriteBuffer) {
            // Case-c space enough after expanding
            final ByteBuffer newBuffer = ByteBuffer.allocate(newSize);
            newBuffer.put(last);
            newBuffer.put(writeBuffer);
            newBuffer.flip();
            this.writeQueue.pollLast();
            this.writeQueue.offer(newBuffer);
            return;
        }
        
        this.writeQueue.offer(writeBuffer);
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
    
    protected boolean tryBegin(SQLStatement sql) throws SQLException {
        boolean success = false;
        SQLiteConnection conn = getConnection();
        if (sql.isTransaction() && conn.getAutoCommit()) {
            TransactionStatement txSql = (TransactionStatement)sql;
            if (txSql.isBegin() || txSql.isSavepoint()) {
                server.trace(log, "tx: begin");
                this.savepointStack.clear();
                conn.getConnectionConfig().setAutoCommit(false);
                this.savepointStack.push(txSql);
                success = true;
            } 
        }
        if (server.isTrace()) {
            server.trace(log, "sqliteConn execute: autocommit {} ->", conn.getAutoCommit());
        }
        
        return success;
    }
    
    protected void tryFinish(SQLStatement sql) throws SQLException {
        SQLiteConnection conn = getConnection();
        if (sql.isTransaction()) {
            TransactionStatement txSql = (TransactionStatement)sql;
            String command = sql.getCommand();
            switch (command) {
            case "COMMIT":
            case "END":
            case "ROLLBACK":
                if (txSql.hasSavepoint()) {
                    break;
                }
                conn.getConnectionConfig().setAutoCommit(true);
                this.savepointStack.clear();
                server.trace(log, "tx: finish");
                break;
            case "RELEASE":
                boolean autoCommit = this.savepointStack.isEmpty();
                String savepoint = txSql.getSavepointName();
                for (; !this.savepointStack.isEmpty(); ) {
                    TransactionStatement spSql = this.savepointStack.peek();
                    if (spSql.isBegin()) {
                        break;
                    }
                    this.savepointStack.pop();
                    String target = spSql.getSavepointName();
                    server.trace(log, "tx: release savepoint {}", target);
                    if (target.equalsIgnoreCase(savepoint)) {
                        autoCommit = this.savepointStack.isEmpty();
                        break;
                    }
                }
                if (autoCommit) {
                    conn.getConnectionConfig().setAutoCommit(true);
                    server.trace(log, "tx: finish");
                }
                break;
            }
        }
        
        boolean autoCommit = conn.getAutoCommit();
        if (autoCommit) {
            detachMetaDb(conn);
            if (sql instanceof GrantStatement) {
                this.server.flushPrivileges();
            } else if (sql instanceof CreateUserStatement) {
                this.server.flushHosts();
            }
        }
        
        if (server.isTrace()) {
            server.trace(log, "sqliteConn execute: autocommit {} <-", autoCommit);
        }
    }
    
    protected void resetAutoCommit() {
        getConnection().getConnectionConfig()
        .setAutoCommit(true);
        this.savepointStack.clear();
    }
    
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
    
    public String toString() {
        return this.name;
    }
    
    protected static abstract class WriteTask implements Runnable {
        
        protected final SQLiteProcessor proc;
        
        protected WriteTask(SQLiteProcessor proc) {
            this.proc = proc;
        }
        
        public void run() {
            try {
                write();
            } catch (IOException e) {
                this.proc.worker.close(this.proc);
            }
        }
        
        protected abstract void write() throws IOException;
    }
    
}
