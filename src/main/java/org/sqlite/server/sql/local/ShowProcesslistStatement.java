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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.sqlite.server.SQLiteProcessor;
import org.sqlite.server.SQLiteProcessorState;
import org.sqlite.server.SQLiteServer;

/** "SHOW [FULL] PROCESSLIST" statement.
 * 
 * @author little-pan
 * @since 2019-10-19
 *
 */
public class ShowProcesslistStatement extends LocalStatement {
    
    public static final String TBL_NAME = "processor";
    
    protected boolean full;
    
    public ShowProcesslistStatement(String sql) {
        super(sql, "SHOW PROCESSLIST", true);
    }

    @Override
    protected String getSQL(String localSchema) throws SQLException {
        final String f = 
                "select Id, User, Host, db, Command, Time, State, Info "
                + "from '%s'.%s order by Id%s";
        return format(f, localSchema, TBL_NAME, (isFull()? "": " limit 100"));
    }
    
    @Override
    protected void init() throws SQLException {
        super.init();
        
        String localSchema = super.localDb.getSchemaName();
        String f, sql;
        // CREATE TABLE
        f = "create table if not exists '%s'.%s("
                + "`Id` integer not null, "
                + "`User` varchar(64) not null, "
                + "`Host` varchar(80) not null,"
                + "`Db` varchar(64),"
                + "`Command` varchar(64),"
                + "`Time` integer not null,"
                + "`State` varchar(80),"
                + "`Info` text)";
        sql = format(f, localSchema, TBL_NAME);
        execute(sql);
    }
    
    @Override
    public void preExecute(int maxRows) throws SQLException, IllegalStateException {
        super.preExecute(maxRows);
        
        String localSchema = super.localDb.getSchemaName(), f, sql;
        // DELETE old data
        f = "delete from '%s'.%s";
        sql = format(f, localSchema, TBL_NAME);
        execute(sql);
        
        // Collect processor state
        SQLiteProcessor processor = super.getContext();
        SQLiteServer server = processor.getServer();
        List<SQLiteProcessorState> states = server.getProcessorStates(processor);
        // INSERT new data for query
        f = "insert into '%s'.%s(`id`, `user`, `host`, `db`, `command`, `time`, `state`, `info`)"
                + "values(?, ?, ?, ?, ?, ?, ?, ?)";
        sql = format(f, localSchema, TBL_NAME);
        try (PreparedStatement ps = processor.getConnection().prepareStatement(sql)) {
            for (SQLiteProcessorState state: states) {
                int i = 0;
                ps.setInt(++i, state.getId());
                ps.setString(++i, state.getUser());
                ps.setString(++i, state.getHost());
                ps.setString(++i, state.getDb());
                ps.setString(++i, state.getCommand());
                ps.setInt(++i, state.getTime());
                ps.setString(++i, state.getState());
                ps.setString(++i, state.getInfo(isFull()));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }
    
    public boolean isFull() {
        return full;
    }

    public void setFull(boolean full) {
        this.full = full;
    }
    
}
