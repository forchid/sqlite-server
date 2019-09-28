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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteErrorCode;
import static org.sqlite.util.ConvertUtils.*;

/** SQLite processor task that processes asynchronous logic such as busy, big result set write
 * in SQLite processor, and handle fatal error for protecting SQLiteWorker.
 * 
 * @author little-pan
 * @since 2019-09-23
 *
 */
public abstract class SQLiteProcessorTask implements Runnable {
    static final Logger log = LoggerFactory.getLogger(SQLiteProcessorTask.class);
    
    protected final SQLiteProcessor proc;
    protected boolean open = true;
    protected boolean async;
    
    protected SQLiteProcessorTask(SQLiteProcessor proc) {
        this.proc = proc;
    }
    
    @Override
    public void run() {
        boolean failed = true;
        try {
            if (isOpen()) {
                execute();
                failed = false;
                return;
            }
            
            throw new IllegalStateException("Processor task closed");
        } catch (Exception e) {
            final SQLiteProcessor proc = this.proc;
            proc.traceError(log, "Execute task error", e);
            proc.getWorker().close(proc);
        } catch (OutOfMemoryError e) {
            log.warn("No memory", e);
            this.proc.getWorker().close(this.proc);
        } finally {
            if (failed) {
                this.open = false;
            }
        }
    }

    protected abstract void execute() throws IOException;
    
    protected void finish() throws IOException {
        SQLiteProcessor proc = this.proc;
        
        this.open = false;
        if (isAsync()) {
            proc.process();
        }
    }
    
    protected void checkBusyState() throws SQLException {
        SQLiteBusyContext ctx = this.proc.getBusyContext();
        if (ctx != null && !ctx.isSleepable() && ctx.isTimeout()) {
            throw convertError(SQLiteErrorCode.SQLITE_BUSY);
        }
        
        if (ctx != null && ctx.isCanceled()) {
            throw convertError(SQLiteErrorCode.SQLITE_INTERRUPT);
        }
    }
    
    public boolean isAsync() {
        return this.async;
    }
    
    public boolean isOpen() {
        return this.open;
    }
    
}
