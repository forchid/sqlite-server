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

/** SQLite busy or locked context for busy task
 * 
 * @author little-pan
 * @since 2019-09-23
 *
 */
public class SQLiteBusyContext {
    
    protected volatile boolean canceled;
    protected final boolean sleepable;
    
    protected final long startTime;
    protected final long timeoutTime;
    protected boolean onDbWriteLock;  // busy on
    
    public SQLiteBusyContext(long timeout) throws IllegalArgumentException {
        this(false, timeout);
    }
    
    public SQLiteBusyContext(boolean onDbWriteLock, long timeout) throws IllegalArgumentException {
        this(onDbWriteLock, timeout, false);
    }
    
    public SQLiteBusyContext(long timeout, boolean sleepable) throws IllegalArgumentException {
        this(false, timeout, sleepable);
    }
    
    public SQLiteBusyContext(boolean onDbWriteLock, long timeout, boolean sleepable) 
            throws IllegalArgumentException {
        if (timeout < 0L) {
            throw new IllegalArgumentException("timeout " + timeout);
        }
        
        this.startTime = System.currentTimeMillis();
        this.timeoutTime = this.startTime + timeout;
        this.sleepable = sleepable;
    }
    
    public boolean isCanceled() {
        return canceled;
    }

    public void setCanceled(boolean canceled) {
        this.canceled = canceled;
    }
    
    public boolean isOnDbWriteLock() {
        return onDbWriteLock;
    }
    
    public void setOnDbWriteLock(boolean onDbWriteLock) {
        this.onDbWriteLock = onDbWriteLock;
    }
    
    public long getStartTime() {
        return startTime;
    }
    
    public long getTimeoutTime() {
        return this.timeoutTime;
    }
    
    public boolean isSleepable() {
        return this.sleepable;
    }
    
    public boolean isTimeout() {
        long curMillis = System.currentTimeMillis();
        return (this.timeoutTime < curMillis);
    }
    
    public boolean isReady() {
        if (this.sleepable) {
            long curMillis = System.currentTimeMillis();
            return (this.timeoutTime <= curMillis);
        }
        
        return true;
    }
    
}
