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
package org.sqlite.server.func;

import java.sql.SQLException;

import org.sqlite.Function;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.server.SQLiteBusyContext;
import org.sqlite.server.SQLiteProcessor;

/** SLEEP(second) function
 * 
 * @author little-pan
 * @since 2019-09-23
 *
 */
public class SleepFunc extends Function {
    
    protected final SQLiteProcessor processor;
    
    public SleepFunc(SQLiteProcessor processor) {
        this.processor = processor;
    }

    @Override
    protected void xFunc() throws SQLException {
        int args = super.args();
        if (args != 1) {
            SQLiteErrorCode error = SQLiteErrorCode.SQLITE_ERROR;
            throw new SQLException("Incorrect parameter count in the call to 'sleep'", "42000", error.code);
        }
        
        int second = super.value_int(0);
        if (second < 0) {
            SQLiteErrorCode error = SQLiteErrorCode.SQLITE_ERROR;
            throw new SQLException("Incorrect arguments in the call to 'sleep'", "42000", error.code);
        }
        
        SQLiteBusyContext busyContext = this.processor.getBusyContext();
        if (second == 0 || (busyContext != null && busyContext.isReady())) {
            super.result(0);
            return;
        }
        if (busyContext == null) {
            throw new IllegalStateException("No busy context set");
        }
        
        SQLiteErrorCode error = SQLiteErrorCode.SQLITE_BUSY;
        throw new SQLException("Non-blocking execution", "57019", error.code);
    }

}
