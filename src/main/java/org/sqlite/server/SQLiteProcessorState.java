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

import java.net.InetSocketAddress;

import org.sqlite.server.sql.meta.User;
import org.sqlite.sql.SQLStatement;

/**
 * @author little-pan
 * @since 2019-10-19
 *
 */
public class SQLiteProcessorState implements AutoCloseable {
    
    protected final SQLiteProcessor processor;
    protected String command;
    protected long startTime;
    protected String state = "";
    protected String sql;
    
    public SQLiteProcessorState(SQLiteProcessor processor) {
        this.processor = processor;
        this.startTime = System.currentTimeMillis();
        this.state = "init";
    }
    
    public SQLiteProcessor getProcessor() {
        return this.processor;
    }
    
    public int getId() {
        return this.processor.getId();
    }
    
    public String getUser() {
        return this.processor.getUserName();
    }

    public String getHost() {
        InetSocketAddress remote = this.processor.getRemoteAddress();
        final User user = this.processor.getUser();
        if (user == null) {
            return (String.format("%s:%s", remote.getHostString(), remote.getPort()));
        }
        
        return (String.format("%s:%s", user.getHost(), remote.getPort()));
    }
    
    public String getDb() {
        return this.processor.getDbName();
    }
    
    public String getCommand() {
        return this.command;
    }
    
    public synchronized void setCommand(String command) {
        this.command = command;
    }
    
    public int getTime() {
        final long curTime = System.currentTimeMillis();
        return (int)((curTime - this.startTime) / 1000L);
    }
    
    public long getStartTime() {
        return startTime;
    }

    public synchronized void setStartTime(long startTime) {
        this.startTime = startTime;
    }
    
    public String getState() {
        return state;
    }
    
    public synchronized void setState(String state) {
        this.state = state;
    }

    public String getInfo(boolean full) {
        String sql = this.sql;
        if (sql == null) {
            return null;
        }
        
        String info = sql;
        if (info.length() <= 80 || full) {
            return info;
        } else {
            return info.substring(0, 80);
        }
    }
    
    public String getInfo() {
        return getInfo(false);
    }
    
    public synchronized void startQuery(SQLStatement sqlStatement) {
        startQuery(sqlStatement, "starting");
    }
    
    public synchronized void startQuery(SQLStatement sqlStatement, String state) {
        setCommand("Query");
        this.startTime = System.currentTimeMillis();
        this.state = state;
        this.sql = sqlStatement.toString();
    }
    
    public synchronized void startSleep() {
        setCommand("Sleep");
        this.startTime = System.currentTimeMillis();
        this.state = "";
        this.sql = null;
    }

    public SQLiteProcessorState copy() {
        SQLiteProcessorState copy = new SQLiteProcessorState(this.processor);
        synchronized(this) {
            copy.command = this.command;
            copy.startTime = this.startTime;
            copy.state = this.state;
            copy.sql = this.sql;
        }
        return copy;
    }
    
    public synchronized void stop() {
        setCommand(null);
        this.startTime = System.currentTimeMillis();
        this.state = "stopped";
        this.sql = null;
    }
    
    public boolean isOpen() {
        return this.processor.isOpen();
    }

    @Override
    public synchronized void close() {
        setCommand(null);
        this.startTime = System.currentTimeMillis();
        this.state = "closed";
        this.sql = null;
    }

}
