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

import org.sqlite.sql.SQLStatement;

/** SQLite busy or locked context for busy task
 * 
 * @author little-pan
 * @since 2019-09-23
 *
 */
public class SQLiteBusyContext {
    
    protected SQLStatement statement;
    protected long startTime;
    protected long executeTime;
    
    public SQLiteBusyContext() {
        this(0L);
    }
    
    public SQLiteBusyContext(long executeTime) {
        this.executeTime = executeTime;
        this.startTime = System.currentTimeMillis();
    }
    
    public SQLStatement getStatement() {
        return statement;
    }
    
    public void setStatement(SQLStatement statement) {
        this.statement = statement;
    }
    
    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }
    
    public long getExecuteTime() {
        return this.executeTime;
    }

    public void setExecuteTime(long executeTime) {
        this.executeTime = executeTime;
    }
    
    public boolean isReady() {
        if (this.executeTime == 0L) {
            return true;
        }
        long curMillis = System.currentTimeMillis();
        return (this.executeTime <= curMillis);
    }
    
}
