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
package org.sqlite.sql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.sqlite.server.util.IoUtils;

/** "INSERT INTO ... SELECT ..." statement
 * @author little-pan
 * @since 2019-09-28
 *
 */
public class InsertSelectStatement extends SQLStatement {
    
    protected SQLStatement selectStatement;

    public InsertSelectStatement(String sql) {
        super(sql, "INSERT");
    }
    
    @Override
    public PreparedStatement prepare() throws SQLException, IllegalStateException {
        PreparedStatement ps = super.prepare();
        
        if (this.selectStatement != null) {
            this.selectStatement.setContext(getContext());
            this.selectStatement.prepare();
        }
        return ps;
    }
    
    @Override
    public void preExecute(int maxRows) throws SQLException, IllegalStateException {
        super.preExecute(maxRows);
        
        if (!this.prepared && this.selectStatement != null) {
            this.selectStatement.setContext(getContext());
            this.selectStatement.preExecute(maxRows);
        }
    }

    public SQLStatement getSelectStatement() {
        return selectStatement;
    }
    
    public void setSelectStatement(SQLStatement selectStatement) {
        this.selectStatement = selectStatement;
    }
    
    @Override
    public void close() {
        IoUtils.close(this.selectStatement);
        super.close();
    }
    
}
