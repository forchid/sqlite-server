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
    
    public static final int INIT = 0x0000, AUTH = 0x0001, OPEN = 0x0002, 
        QUERY  = 0x0004,  SLEEP  = 0x0008, SLEEP_IN_TX = 0x0010,
        STOPPED= 0x0020,  CLOSED = 0x0040, READ = 0x0080, WRITE= 0x0100;
    
    private final SpinLock lock = new SpinLock();
    
    protected final SQLiteProcessor processor;
    protected String command;
    protected long startTime;
    protected int state = INIT;
    protected String stateText = "";
    protected String sql;
    
    public SQLiteProcessorState(SQLiteProcessor processor) {
        this.processor = processor;
        this.startTime = System.currentTimeMillis();
        this.stateText = "init";
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
        if (user == null || "%".equals(user.getHost())) {
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
    
    public int getState() {
        this.lock.lock();
        try {
            return this.state;
        } finally {
            this.lock.unlock();
        }
    }
    
    public String getStateText() {
        return this.stateText;
    }
    
    public void setStateText(String stateText) {
        this.lock.lock();
        try {
            this.stateText = stateText;
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
    
    public void lock() {
        this.lock.lock();
    }
    
    public void unlock() {
        this.lock.unlock();
    }
    
    public void startAuth() {
        this.lock.lock();
        try {
            this.command = "Auth";
            this.startTime = System.currentTimeMillis();
            this.state = AUTH;
            this.stateText = "start authentication";
            this.sql = null;
        } finally {
            this.lock.unlock();
        }
    }
    
    public void startOpen() {
        this.lock.lock();
        try {
            this.command = "Open";
            this.startTime = System.currentTimeMillis();
            this.state = OPEN;
            this.stateText = "open and init database";
            this.sql = null;
        } finally {
            this.lock.unlock();
        }
    }
    
    public void startRead() {
        this.lock.lock();
        try {
            this.command = "Read";
            this.startTime = System.currentTimeMillis();
            this.state = READ;
            this.stateText = "read data from network";
            this.sql = null;
        } finally {
            this.lock.unlock();
        }
    }
    
    public void startWrite() {
        this.lock.lock();
        try {
            this.command = "Write";
            this.startTime = System.currentTimeMillis();
            this.state = WRITE;
            this.stateText = "flush data into network";
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
            this.state = QUERY;
            this.stateText = state;
            this.sql = sqlStatement.toString();
        } finally {
            this.lock.unlock();
        }
    }
    
    public void startSleep() {
        boolean inTx = false;
        if (this.processor.isOpen()) {
            if (this.processor.getConnection() != null) {
                inTx = !this.processor.isAutoCommit();
            }
        }
        
        this.lock.lock();
        try {
            this.command = "Sleep";
            this.startTime = System.currentTimeMillis();
            if (inTx) {
                this.state = SLEEP_IN_TX;
                this.stateText = "idle in transaction";
            } else {
                this.state = SLEEP;
                this.stateText = "idle";
            }
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
            copy.stateText = this.stateText;
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
            this.state = STOPPED;
            this.stateText = "stopped";
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
            this.state = CLOSED;
            this.stateText = "closed";
            this.sql = null;
        } finally {
            this.lock.unlock();
        }
    }

}
