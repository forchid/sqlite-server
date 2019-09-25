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
        setBusyContext(null);
        this.proc.queryTask = null;
        super.finish();
    }
    
}
