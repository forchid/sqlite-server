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
package org.sqlite.server.sql.meta;

import java.sql.SQLException;

import org.sqlite.server.MetaStatement;
import org.sqlite.server.SQLiteProcessor;
import org.sqlite.sql.SQLParseException;
import org.sqlite.sql.SQLParser;
import org.sqlite.sql.SQLStatement;

/** SHOW [ALL] DATABASES
 * 
 * @author little-pan
 * @since 2019-09-19
 *
 */
public class ShowDatabasesStatement extends MetaStatement {
    
    protected boolean sa;
    protected boolean all;
    
    protected String host;
    protected String user;
    
    public ShowDatabasesStatement(String sql) {
        super(sql, "SHOW DATABASES");
        this.query = true;
    }
    
    /**
     * @return the sa
     */
    public boolean isSa() {
        return sa;
    }

    /**
     * @param sa the sa to set
     */
    public void setSa(boolean sa) {
        this.sa = sa;
    }

    /**
     * @return the all
     */
    public boolean isAll() {
        return all;
    }

    /**
     * @param all the all to set
     */
    public void setAll(boolean all) {
        this.all = all;
    }
    
    public void setUser(User user) {
        this.host = user.getHost();
        this.user = user.getUser();
    }

    @Override
    public String getMetaSQL(String metaSchema) throws SQLParseException {
        User metaUser = getMetaUser();
        if (metaUser.isSa()) {
            setSa(true);
        } else {
            setUser(metaUser);
        }
        
        String sql;
        if (this.all) {
            String f = "select db, dir, size from '%s'.catalog order by db asc";
            sql = String.format(f, metaSchema);
        } else {
            if (this.sa) {
                sql = String.format("select db from '%s'.catalog order by db asc", metaSchema);
            } else {
                String f = "select db from '%s'.db where host = '%s' and user = '%s' order by db asc";
                sql = String.format(f, metaSchema, this.host, this.user);
            }
        }
        
        // Check
        try (SQLParser parser = new SQLParser(sql)) {
            SQLStatement stmt = parser.next();
            if ("SELECT".equals(stmt.getCommand()) && !parser.hasNext()) {
                return stmt.getSQL();
            }
        } catch (SQLParseException e) {}
        
        throw new SQLParseException(getSQL());
    }

    @Override
    public boolean isNeedSa() {
        return (this.all || this.sa);
    }
    
    @Override
    protected void preExecute(int maxRows) throws SQLException, IllegalStateException {
        super.preExecute(maxRows);
        SQLiteProcessor proc = getContext();
        proc.statisticsCatalogs();
    }

}
