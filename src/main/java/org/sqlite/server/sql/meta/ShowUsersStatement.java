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

import org.sqlite.sql.SQLParseException;
import org.sqlite.sql.SQLParser;
import org.sqlite.sql.SQLStatement;

/** "SHOW USERS [WHERE 'pattern']" statement, only for super administrator.
 * 
 * @author little-pan
 * @since 2019-12-01
 *
 */
public class ShowUsersStatement extends MetaStatement {
    
    protected String pattern;
    
    public ShowUsersStatement(String sql) {
        super(sql, "SHOW USERS", true);
    }

    @Override
    public String getMetaSQL(String metaSchema) throws SQLParseException {
        final String sql, f;
        
        if (this.pattern == null) {
            f = "select user, host, protocol from '%s'.user "
                    + "order by user asc, host asc, protocol asc";
            sql = format(f, metaSchema);
        } else {
            f = "select user, host, protocol from '%s'.user "
                    + "where user like '%s' "
                    + "order by user asc, host asc, protocol asc";
            sql = format(f, metaSchema, this.pattern);
        }
        
        // Check SQL
        try (SQLParser parser = new SQLParser(sql)) {
            SQLStatement stmt = parser.next();
            if ("SELECT".equals(stmt.getCommand()) && !parser.hasNext()) {
                return stmt.getSQL();
            }
        } catch (SQLParseException e) {}
        
        throw new SQLParseException(getSQL());
    }
    
    public String getPattern() {
        return pattern;
    }
    
    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

}
