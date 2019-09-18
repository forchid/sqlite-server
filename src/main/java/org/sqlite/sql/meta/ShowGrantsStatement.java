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
package org.sqlite.sql.meta;

import static java.lang.String.*;

import org.sqlite.sql.SQLParseException;
import org.sqlite.sql.SQLParser;
import org.sqlite.sql.SQLStatement;

/** SHOW GRANTS [FOR {'user'[@'host'|'%'] | CURRENT_USER[()]}]
 * 
 * @author little-pan
 * @since 2019-09-18
 *
 */
public class ShowGrantsStatement extends SQLStatement implements MetaStatement {
    
    protected String host = "%";
    protected String user;
    protected boolean currentUser;
    
    public ShowGrantsStatement(String sql) {
        super(sql, "SHOW GRANTS");
        this.query = true;
    }

    @Override
    public String getMetaSQL(String metaSchema) throws SQLParseException {
        if (this.user == null) {
            throw new SQLParseException("No 'user' specified");
        }
        
        String f = "select * from '%s'.db where host = '%s' and user = '%s'";
        String sql = format(f, metaSchema, this.host, this.user);
        try (SQLParser parser = new SQLParser(sql)) {
            SQLStatement stmt = parser.next();
            if ("SELECT".equals(stmt.getCommand()) && !parser.hasNext()) {
                return stmt.getSQL();
            }
        } catch (SQLParseException e) {}
        
        throw new SQLParseException(getSQL());
    }

    @Override
    public boolean needSa() {
        return (!currentUser);
    }
    
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public boolean isCurrentUser() {
        return currentUser;
    }

    public void setCurrentUser(boolean currentUser) {
        this.currentUser = currentUser;
    }
    
    public boolean isCurrentUser(String host, String user) {
        if (user == null) {
            return false;
        } else {
            return (host.equals(this.host) && user.equals(this.user));
        }
    }
    
}
