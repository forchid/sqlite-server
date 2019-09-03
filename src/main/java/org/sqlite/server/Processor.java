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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConnection;
import org.sqlite.util.IoUtils;

/**
 * The server protocol handler.
 * 
 * @author little-pan
 * @since 2019-09-01
 *
 */
public abstract class Processor implements Runnable, AutoCloseable {
    
    static final Logger log = LoggerFactory.getLogger(Processor.class);
    
    protected Socket socket;
    protected final int id;
    protected final String name;
    protected final SQLiteServer server;
    
    private volatile boolean open = true;
    private volatile boolean stopped;
    protected Thread runner;
    protected SQLiteConnection connection;
    
    protected Processor(Socket socket, int processId, SQLiteServer server) {
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

    public void start() {
        if (isStopped()) {
            throw new IllegalStateException(getName() + " has been stopped");
        }
        
        Thread thread = new Thread(this, getName());
        this.runner = thread;
        thread.start();
    }
    
    public void stop() {
        if (isStopped()) {
            return;
        }
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
        if (!isOpen()) {
            return;
        }
        this.open = false;
        
        stop();
        IoUtils.close(this.socket);
        IoUtils.close(this.connection);
        this.server.removeProcessor(this);
        this.server.trace(log, "Close");
    }

}
