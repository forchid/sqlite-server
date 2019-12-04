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
import org.sqlite.server.util.locks.SpinLock;
import org.sqlite.sql.SQLStatement;

/**
 * @author little-pan
 * @since 2019-10-19
 *
 */
public class SQLiteProcessorState implements AutoCloseable {
    
    private final SpinLock lock = new SpinLock();
    
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
    
    public void setCommand(String command) {
        this.lock.lock();
        try {
            this.command = command;
        } finally {
            this.lock.unlock();
        }
    }
    
    public int getTime() {
        final long curTime = System.currentTimeMillis();
        return (int)((curTime - this.startTime) / 1000L);
    }
    
    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.lock.lock();
        try {
            this.startTime = startTime;
        } finally {
            this.lock.unlock();
        }
    }
    
    public String getState() {
        return state;
    }
    
    public void setState(String state) {
        this.lock.lock();
        try {
            this.state = state;
        } finally {
            this.lock.unlock();
        }
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
    
    public void startOpen() {
        this.lock.lock();
        try {
            this.command = "Open";
            this.startTime = System.currentTimeMillis();
            this.state = "open and init database";
            this.sql = null;
        } finally {
            this.lock.unlock();
        }
    }
    
    public void startQuery(SQLStatement sqlStatement) {
        startQuery(sqlStatement, "starting");
    }
    
    public void startQuery(SQLStatement sqlStatement, String state) {
        this.lock.lock();
        try {
            this.command = "Query";
            this.startTime = System.currentTimeMillis();
            this.state = state;
            this.sql = sqlStatement.toString();
        } finally {
            this.lock.unlock();
        }
    }
    
    public void startSleep() {
        this.lock.lock();
        try {
            this.command = "Sleep";
            this.startTime = System.currentTimeMillis();
            this.state = "";
            this.sql = null;
        } finally {
            this.lock.unlock();
        }
    }

    public SQLiteProcessorState copy() {
        SQLiteProcessorState copy = new SQLiteProcessorState(this.processor);
        this.lock.lock();
        try {
            copy.command = this.command;
            copy.startTime = this.startTime;
            copy.state = this.state;
            copy.sql = this.sql;
        } finally {
            this.lock.unlock();
        }
        
        return copy;
    }
    
    public void stop() {
        this.lock.lock();
        try {
            this.command = null;
            this.startTime = System.currentTimeMillis();
            this.state = "stopped";
            this.sql = null;
        } finally {
            this.lock.unlock();
        }
    }
    
    public boolean isOpen() {
        return this.processor.isOpen();
    }

    @Override
    public void close() {
        this.lock.lock();
        try {
            this.command = null;
            this.startTime = System.currentTimeMillis();
            this.state = "closed";
            this.sql = null;
        } finally {
            this.lock.unlock();
        }
    }

}
