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
package org.sqlite.server.sql.local;

import java.sql.SQLException;

import org.sqlite.SQLiteErrorCode;
import org.sqlite.server.SQLiteProcessor;
import org.sqlite.server.SQLiteServer;
import org.sqlite.server.sql.meta.User;
import org.sqlite.util.ConvertUtils;

/** "KILL [connection | query] processor_id" statement.
 * 
 * @author little-pan
 * @since 2019-10-19
 *
 */
public class KillStatement extends LocalStatement {
    
    protected boolean killQuery;
    protected int processorId;
    protected SQLiteProcessor killed;
    
    public KillStatement(String sql) {
        super(sql, "KILL");
    }
    
    @Override
    protected void checkPermission() throws SQLException {
        SQLiteProcessor processor = getContext();
        SQLiteServer server = processor.getServer();
        final int pid = getProcessorId();
        SQLiteProcessor killed = server.getProcessor(pid);
        if (killed == null) {
            SQLiteErrorCode error = SQLiteErrorCode.SQLITE_ERROR;
            String message = format("Unknown processor id: %s", pid);
            throw ConvertUtils.convertError(error, message, "HY000");
        }
        final User me = processor.getUser();
        if (me == null || (!me.isSa() && !me.equals(killed.getUser()))) {
            throw ConvertUtils.convertError(SQLiteErrorCode.SQLITE_PERM);
        }
        
        this.killed = killed;
    }
    
    @Override
    public void complete(boolean success) {
        this.killed = null;
        super.complete(success);
    }
    
    @Override
    protected String getSQL(String localSchema) throws SQLException {
        return getUpdatableSQL(localSchema, true);
    }
    
    @Override
    protected void postExecute(boolean resultSet) throws SQLException {
        super.postExecute(resultSet);
        
        this.killed.cancelRequest(isKillQuery());
    }
    
    public boolean isKillQuery() {
        return killQuery;
    }

    public void setKillQuery(boolean killQuery) {
        this.killQuery = killQuery;
    }

    public int getProcessorId() {
        return processorId;
    }

    public void setProcessorId(int processorId) {
        this.processorId = processorId;
    }

}
