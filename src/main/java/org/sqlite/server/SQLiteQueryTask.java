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

import java.io.IOException;
import java.sql.SQLException;

/** SQLite query task
 * 
 * @author little-pan
 * @since 2019-09-24
 *
 */
public abstract class SQLiteQueryTask extends SQLiteProcessorTask {
    
    protected volatile SQLiteBusyContext busyContext;
    
    protected SQLiteQueryTask(SQLiteProcessor proc) {
        super(proc);
    }
    
    public boolean handleBlocked(boolean timeout, SQLException cause) {
        int busyTimeout = proc.server.getBusyTimeout();
        return (handleBlocked(timeout, cause, busyTimeout));
    }
    
    public boolean handleBlocked(boolean timeout, SQLException cause, int busyTimeout) {
        SQLiteProcessor proc = this.proc;
        
        if (!timeout && proc.server.isBlocked(cause)) {
            SQLiteBusyContext busyContext = getBusyContext();
            if (busyContext == null) {
                busyTimeout = Math.max(0, busyTimeout);
                busyContext = new SQLiteBusyContext(busyTimeout);
                setBusyContext(busyContext);
            }
            proc.getWorker().busy(proc);
            this.async = true;
            return true;
        } else {
            return false;
        }
    }
    
    public SQLiteBusyContext getBusyContext() {
        return busyContext;
    }

    public void setBusyContext(SQLiteBusyContext busyContext) {
        this.busyContext = busyContext;
    }
    
    public boolean isReady() {
        return (getBusyContext().isReady());
    }
    
    public boolean isCanceled() {
        return (getBusyContext().isCanceled());
    }
    
    @Override
    public void finish() throws IOException {
        this.proc.queryTask = null;
        setBusyContext(null);
        super.finish();
    }
    
}
