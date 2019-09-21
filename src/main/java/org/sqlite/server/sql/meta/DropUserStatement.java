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

import static java.lang.String.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.sqlite.SQLiteErrorCode;
import org.sqlite.server.MetaStatement;
import org.sqlite.sql.SQLParseException;
import org.sqlite.sql.SQLParser;
import org.sqlite.sql.SQLStatement;

/**The DROP USER statement:
 * DROP USER user@host [IDENTIFIED WITH PG] [, user@host [IDENTIFIED WITH PG]...]
 * 
 * @author little-pan
 * @since 2019-09-16
 *
 */
public class DropUserStatement extends MetaStatement {
    
    protected List<DroppedUser> users = new ArrayList<>(2);
    
    public DropUserStatement(String sql) {
        super(sql, "DROP USER");
    }

    @Override
    public String getMetaSQL(String metaSchema) throws SQLParseException {
        int size = this.users.size();
        if (size == 0) {
            throw new SQLParseException("No 'user'@'host' specified in DROP USER statement");
        }
        String f = "delete from '%s'.user where ";
        StringBuilder sb = new StringBuilder(format(f, metaSchema));
        for (int i = 0; i < size; ++i) {
            DroppedUser u = this.users.get(i);
            sb.append(i==0? "":" or ").append('(')
            .append("host = ").append('\'').append(u.host).append('\'').append(" and ")
            .append("user = ").append('\'').append(u.user).append('\'').append(" and ")
            .append("protocol = ").append('\'').append(u.protocol).append('\'')
            .append(')');
        }
        try (SQLParser parser = new SQLParser(sb.toString(), true)) {
            SQLStatement stmt = parser.next();
            if ("DELETE".equals(stmt.getCommand()) && !parser.hasNext()) {
                return (stmt.getSQL());
            }
        } catch (SQLParseException e) {
            // next throw
        }
        
        throw new SQLParseException(getSQL());
    }
    
    public void addUser(String host, String user, String protocol) {
        DroppedUser u = new DroppedUser(host, user, protocol);
        this.users.add(u);
    }
    
    public int getUserCount() {
        return this.users.size();
    }
    
    public boolean exists(String host, String user, String protocol) {
        DroppedUser du = new DroppedUser(host, user, protocol);
        return (this.users.contains(du));
    }
    
    @Override
    protected void checkPermission() throws SQLException {
        User user = getMetaUser();
        if (user.isSa() || !isNeedSa()) {
            return;
        }
        
        String host = user.getHost(), proto = user.getProtocol();
        if ((1 == getUserCount()) && exists(host, user.getUser(), proto)) {
            return;
        }
        
        throw convertError(SQLiteErrorCode.SQLITE_PERM);
    }
    
    static class DroppedUser {
        final String host;
        final String user;
        final String protocol;
        
        DroppedUser(String host, String user, String protocol) {
            this.host = host;
            this.user = user;
            this.protocol = protocol;
        }
        
        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            
            if (o instanceof DroppedUser) {
                DroppedUser u = (DroppedUser)o;
                return (this.host.equals(u.host) 
                        && this.user.equals(u.user) 
                        && this.protocol.equalsIgnoreCase(u.protocol));
            }
            
            return false;
        }
        
        @Override
        public int hashCode() {
            return (this.host.hashCode() 
                    ^ this.user.hashCode() ^ this.protocol.hashCode());
        }
        
    }
    
}
