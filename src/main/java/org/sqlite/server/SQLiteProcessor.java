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

import java.net.Socket;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

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
public abstract class SQLiteProcessor implements Runnable, AutoCloseable {
    
    static final Logger log = LoggerFactory.getLogger(SQLiteProcessor.class);
    
    protected Socket socket;
    protected final int id;
    protected final String name;
    protected final SQLiteServer server;
    protected SQLiteAuthMethod authMethod;
    protected User user;
    
    private final AtomicBoolean open = new AtomicBoolean(true);
    private final AtomicBoolean stopped = new AtomicBoolean();
    
    protected SQLiteConnection connection;
    private String metaSchema = null;
    
    protected SQLiteProcessor(Socket socket, int processId, SQLiteServer server) {
        this.socket = socket;
        this.server = server;
        this.id = processId;
        this.name = String.format("%s processor-%d", server.getName(), processId);
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
        return convertError(error, null);
    }
    
    protected SQLException convertError(SQLiteErrorCode error, String message) {
        String sqlState = "HY000";
        if (SQLiteErrorCode.SQLITE_ERROR.code == error.code) {
            sqlState = "42000";
        } else if (SQLiteErrorCode.SQLITE_PERM.code == error.code){
            sqlState = "42501";
        }
        if (message == null) {
            message = error.message;
        }
        return new SQLException(message, sqlState, error.code);
    }

    public void start() {
        final String name = getName();
        if (isStopped()) {
            throw new IllegalStateException(name + " has been stopped");
        }
        
        Thread thread = new Thread(this, name);
        thread.start();
    }
    
    /** Stop this processor
     * @return true if has been stopped, otherwise false
     */
    public boolean stop() {
        if (!this.stopped.compareAndSet(false, true)) {
            return true;
        }
        this.server.close(this);
        return false;
    }
    
    public boolean isStopped() {
        return this.stopped.get();
    }
    
    public boolean isOpen() {
        return this.open.get();
    }
    
    @Override
    public void close() {
        if (!stop()) {
            return;
        }
        
        if (!this.open.compareAndSet(true, false)) {
            return;
        }
        
        IoUtils.close(this.socket);
        IoUtils.close(this.connection);
        this.server.trace(log, "Close");
    }

}
