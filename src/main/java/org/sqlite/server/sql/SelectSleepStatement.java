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
package org.sqlite.server.sql;

import java.sql.SQLException;

import org.sqlite.SQLiteErrorCode;
import org.sqlite.server.SQLiteBusyContext;
import org.sqlite.server.SQLiteProcessor;
import org.sqlite.sql.SQLStatement;

/**
 * @author little-pan
 * @since 2019-09-23
 *
 */
public class SelectSleepStatement extends SQLStatement {
    
    protected int second;
    
    public SelectSleepStatement(String sql) {
        super(sql, "SELECT", true);
    }
    
    public int getSecond() {
        return second;
    }
    
    public void setSecond(int second) {
        this.second = second;
    }
    
    @Override
    public SQLiteProcessor getContext() {
        return (SQLiteProcessor)this.context;
    }
    
    @Override
    public void preExecute(int maxRows) throws SQLException, IllegalStateException {
        SQLiteProcessor processor = getContext();
        SQLiteBusyContext busyContext = processor.getBusyContext();
        if (this.second == 0 || (busyContext != null && busyContext.isReady())) {
            super.preExecute(maxRows);
            return;
        }
        
        // Calculate the execute time
        if (busyContext == null) {
            busyContext = new SQLiteBusyContext(this.second * 1000L, true);
            processor.setBusyContext(busyContext);
        }
        SQLiteErrorCode error = SQLiteErrorCode.SQLITE_BUSY;
        throw new SQLException("Non-blocking execution", "57019", error.code);
    }
    
}
