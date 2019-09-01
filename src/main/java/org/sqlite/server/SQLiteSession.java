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

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConnection;
import org.sqlite.util.IoUtils;

/**<p>
 * The SQLite server session.
 * </p>
 * 
 * @author little-pan
 * @since 2019-03-20
 *
 */
public class SQLiteSession implements AutoCloseable {
    
    static final Logger log = LoggerFactory.getLogger(SQLiteSession.class);
    
    protected final Processor processor;
    protected final Server server;
    protected SQLiteConnection connection;
    
    public SQLiteSession(Processor processor){
        this.processor = processor;
        this.server = processor.getServer();
    }
    
    public int getId(){
        return processor.getId();
    }
    
    public String getName(){
        return processor.getName();
    }
    
    public Processor getProcessor(){
        return this.processor;
    }
    
    public Server getServer() {
        return this.server;
    }
    
    public SQLiteConnection getConnection() {
        return this.connection;
    }
    
    public void setConnection(SQLiteConnection connection) {
        if (connection == null) {
            throw new NullPointerException("connection");
        }
        
        if (this.connection != null) {
            throw new IllegalStateException("connection has been set");
        }
        
        this.connection = connection;
    }
    
    public boolean isOpen() {
        SQLiteConnection conn = getConnection();
        try {
            return (conn != null && !conn.isClosed());
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public void close() {
        if (!isOpen()) {
            return;
        }
        
        IoUtils.close(getConnection());
    }
    
    public void interrupt() throws SQLException {
        SQLiteConnection conn = getConnection();
        if (conn != null && isOpen()) {
            conn.getDatabase().interrupt();
        }
    }

}
