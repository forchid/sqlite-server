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

import static java.lang.String.*;

/** A statement that represents:
 * CREATE USER 'user'@'host' [[WITH] 
 *   SUPERUSER|NOSUPERUSER 
 * | IDENTIFIED BY 'password' 
 * | IDENTIFIED WITH PG {MD5|PASSWORD|TRUST}]
 * 
 * @author little-pan
 * @since 2019-09-11
 *
 */
public class CreateUserStatement extends SQLStatement implements MetaStatement {
    
    public static final int SUPER = 1;
    public static final int USER  = 0;
    
    protected String user;
    protected String host;
    protected boolean sa;
    protected String password;
    protected String protocol = "pg";
    protected String authMethod = "md5";
    
    public CreateUserStatement(String sql) {
        super(sql, "CREATE USER");
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

    public boolean isSa() {
        return sa;
    }

    public void setSa(boolean sa) {
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

    @Override
    public String getMetaSQL(String metaSchema) {
        String password = this.password==null? "NULL": "'"+this.password+"'" ;
        int sa = isSa()? SUPER: USER;
        String f = "insert into '%s'.user(host, user, password, protocol, auth_method, sa)"
                + "values('%s', '%s', %s, '%s', '%s', %d)";
        String sql = format(f, metaSchema, 
                this.host, this.user, password, this.protocol, this.authMethod, sa);
        // Check
        try (SQLParser parser = new SQLParser(sql, true)) {
            try {
                SQLStatement stmt = parser.next();
                if ("INSERT".equals(stmt.getCommand()) && !parser.hasNext()) {
                    return stmt.getSQL();
                }
            } catch (SQLParseException e) {
                // re-throw: hide metaSchema name
                throw new SQLParseException(getSQL());
            }
            throw new SQLParseException(getSQL());
        }
    }

    @Override
    public boolean needSa() {
        return true;
    }
    
}
