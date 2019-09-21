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

import org.sqlite.SQLiteErrorCode;
import org.sqlite.server.MetaStatement;
import org.sqlite.server.SQLiteAuthMethod;
import org.sqlite.sql.SQLParseException;
import org.sqlite.sql.SQLParser;
import org.sqlite.sql.SQLStatement;

/**A statement that represents:
 * ALTER USER user@host [WITH]
 *   {SUPERUSER|NOSUPERUSER
 * | IDENTIFIED BY 'password'
 * | IDENTIFIED WITH PG [{MD5|PASSWORD|TRUST}]}
 * 
 * @author little-pan
 * @since 2019-09-12
 *
 */
public class AlterUserStatement extends MetaStatement {
    
    protected String user;
    protected String host;
    protected Boolean sa;
    protected String password;
    protected String protocol = PROTO_DEFAULT;
    protected String authMethod;
    
    private boolean passwordSet;

    public AlterUserStatement(String sql) {
        super(sql, "ALTER USER");
    }
    
    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Boolean isSa() {
        return sa;
    }

    public void setSa(Boolean sa) {
        this.sa = sa;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getAuthMethod() {
        return authMethod;
    }

    public void setAuthMethod(String authMethod) {
        this.authMethod = authMethod;
    }
    
    public boolean isPasswordSet() {
        return passwordSet;
    }

    public void setPasswordSet(boolean passwordSet) {
        this.passwordSet = passwordSet;
    }
    
    @Override
    protected void checkPermission() throws SQLException {
        User user = getMetaUser();
        if (user.isSa() || !isNeedSa()) {
            initPassword();
            return;
        }
        
        String host = user.getHost();
        String proto = user.getProtocol();
        if (isUser(host, user.getUser(), proto)) {
            initPassword();
            return;
        }
        
        throw convertError(SQLiteErrorCode.SQLITE_PERM);
    }
    
    protected void initPassword() {
        if (getPassword() != null && !isPasswordSet()) {
            String proto = getProtocol(), method = getAuthMethod();
            SQLiteAuthMethod authMethod = getContext().newAuthMethod(proto, method);
            String p = authMethod.genStorePassword(getUser(), getPassword());
            setPassword(p);
            setPasswordSet(true);
        }
    }

    @Override
    public String getMetaSQL(String metaSchema) {
        StringBuilder sb = new StringBuilder("update ")
            .append('\'').append(metaSchema).append('\'').append('.').append("user ")
            .append("set");
        boolean hasSet = false;
        if (this.sa != null) {
            sb.append(hasSet?',':"").append(" sa = ").append(User.convertSa(this.sa));
            hasSet = true;
        }
        if (this.password != null) {
            sb.append(hasSet?',':"").append(" password = ").append('\'').append(this.password).append('\'');
            hasSet = true;
        }
        if (this.authMethod != null) {
            sb.append(hasSet?',':"").append(" auth_method = ").append('\'').append(this.authMethod).append('\'');
            hasSet = true;
        }
        if (!hasSet) {
            throw new SQLParseException(getSQL());
        }
        
        sb.append(" where host = ").append('\'').append(this.host).append('\'')
        .append(" and user = ").append('\'').append(this.user).append('\'');
        if (this.protocol != null) {
            sb.append(" and protocol = ").append('\'').append(this.protocol).append('\'');
        }
        
        // check
        try (SQLParser parser = new SQLParser(sb.toString())) {
            SQLStatement stmt = parser.next();
            if ("UPDATE".equals(stmt.getCommand()) && !parser.hasNext()) {
                return stmt.getSQL();
            }
        } catch (SQLParseException e) {
            // next throw
        }
        
        throw new SQLParseException(getSQL());
    }
    
    public boolean isUser(String host, String user, String protocol) {
        return ((getHost().equals(host))
                && (getUser().equals(user))
                && (getProtocol().equalsIgnoreCase(protocol)));
    }
    
}
