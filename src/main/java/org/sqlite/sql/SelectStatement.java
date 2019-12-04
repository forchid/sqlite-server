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

import static org.sqlite.server.util.ConvertUtils.convertError;

import java.sql.SQLException;

import org.sqlite.SQLiteErrorCode;

/** "SELECT...[FOR UPDATE]" statement
 * 
 * @author little-pan
 * @since 2019-09-29
 *
 */
public class SelectStatement extends SQLStatement {
    
    protected boolean forUpdate;
    
    public SelectStatement(String sql) {
        super(sql, "SELECT", true);
    }
    
    @Override
    protected void checkReadOnly() throws SQLException {
        if (isForUpdate() && inReadOnlyTx()) {
            String message = "Attempt to write a readonly transaction";
            throw convertError(SQLiteErrorCode.SQLITE_READONLY, message);
        } else {
            super.checkReadOnly();
        }
    }
    
    @Override
    protected boolean isWritable() {
        return (isForUpdate() && !inReadOnlyTx());
    }
    
    public boolean isForUpdate() {
        return forUpdate;
    }
    
    public void setForUpdate(boolean forUpdate) {
        this.forUpdate = forUpdate;
    }

}
