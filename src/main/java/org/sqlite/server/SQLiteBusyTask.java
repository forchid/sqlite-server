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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SQLite busy task that is executed by busy queue
 * 
 * @author little-pan
 * @since 2019-09-23
 *
 */
public abstract class SQLiteBusyTask implements Runnable {
    static final Logger log = LoggerFactory.getLogger(SQLiteBusyTask.class);
    
    protected final SQLiteProcessor proc;
    
    protected SQLiteBusyTask(SQLiteProcessor proc) {
        this.proc = proc;
    }
    
    @Override
    public void run() {
        try {
            execute();
        } catch (Exception e) {
            final SQLiteProcessor proc = this.proc;
            proc.traceError(log, "Execute error", e);
            proc.getWorker().close(proc);
        } catch (OutOfMemoryError e) {
            log.warn("No memory", e);
            this.proc.getWorker().close(proc);
        }
    }

    protected abstract void execute() throws IOException;
    
}
