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
import java.util.HashSet;
import java.util.Set;

import org.sqlite.server.MetaStatement;
import org.sqlite.server.SQLiteProcessor;
import org.sqlite.server.sql.meta.GrantStatement.Grantee;
import org.sqlite.sql.SQLParseException;
import org.sqlite.sql.SQLParser;
import org.sqlite.sql.SQLStatement;
import org.sqlite.util.StringUtils;

/**REVOKE {ALL [PRIVILEGES] [, SELECT | INSERT | UPDATE | DELETE
 * | CREATE | ALTER | DROP | PRAGMA | VACUUM | ATTACH}
 * ON [DATABASE | SCHEMA] dbname [, ...]
 * FROM 'user'@'host' [, ...]
 * 
 * @author little-pan
 * @since 2019-09-18
 *
 */
public class RevokeStatement extends MetaStatement {
    
    protected final Set<String> privileges = new HashSet<>();
    protected final Set<String> revokeds = new HashSet<>();
    protected final Set<Grantee> grantees = new HashSet<>();
    
    protected boolean revokeAll;
    
    public RevokeStatement(String sql) {
        super(sql, "REVOKE");
    }

    @Override
    public String getMetaSQL(String metaSchema) throws SQLParseException {
        // check
        int n = this.privileges.size();
        if (n == 0) {
            throw new SQLParseException("No privilege specified");
        }
        n = this.revokeds.size();
        if (n == 0) {
            throw new SQLParseException("No dbname specified");
        }
        n = this.grantees.size();
        if (n == 0) {
            throw new SQLParseException("No 'user'@'host' specified");
        }
        
        // Generate meta SQL
        StringBuilder sb = new StringBuilder();
        if (this.revokeAll) {
            String f = "delete from '%s'.db";
            sb.append(String.format(f, metaSchema));
        } else {
            String f = "update '%s'.db set ";
            sb.append(String.format(f, metaSchema));
            int i = 0;
            for (String p: this.privileges) {
                for (String a: getPrivileges()) {
                    if (p.equals(a)) {
                        String pfx = StringUtils.toLowerEnglish(p);
                        sb.append(i++ == 0? "": ", ").append(pfx+"_priv = 0");
                        break;
                    }
                }
            }
        }
        int i = 0;
        sb.append(" where ");
        for (Grantee g : this.grantees) {
            for (String d : this.revokeds) {
                sb.append(i++ == 0? "": " or ").append('(')
                .append("host = '").append(g.host).append("' and ")
                .append("user = '").append(g.user).append("' and ")
                .append("db = '").append(d).append("'")
                .append(')');
            }
        }
        String sql = sb.toString();
        
        // check
        try (SQLParser parser = new SQLParser(sql)) {
            SQLStatement stmt = parser.next();
            String command = stmt.getCommand();
            if (this.revokeAll) {
                if ("DELETE".equals(command) && !parser.hasNext()) {
                    return stmt.getSQL();
                }
            } else {
                if ("UPDATE".equals(command) && !parser.hasNext()) {
                    return stmt.getSQL();
                }
            }
        } catch (SQLParseException e) {}
        
        throw new SQLParseException(getSQL());
    }
    
    public boolean addPrivilege(String privi) {
        privi = StringUtils.toUpperEnglish(privi);
        if ("ALL".equals(privi)) {
            this.revokeAll = true;
        }
        return this.privileges.add(privi);
    }
    
    public boolean addRevoked(String revoked) {
        return this.revokeds.add(revoked);
    }
    
    public boolean addGrantee(String host, String user) {
        Grantee grantee = new Grantee(host, user);
        return this.grantees.add(grantee);
    }
    
    public boolean hasPrivilege(String priv) {
        priv = StringUtils.toUpperEnglish(priv);
        return this.privileges.contains(priv);
    }

    public boolean exists(String dbname) {
        return this.revokeds.contains(dbname);
    }
    
    public boolean exists(String host, String user) {
        Grantee grantee = new Grantee(host, user);
        return this.grantees.contains(grantee);
    }
    
    public boolean isRevokeAll() {
        return this.revokeAll;
    }
    
    public void setRevokeAll(boolean revokeAll) {
        this.revokeAll = revokeAll;
    }
    
    @Override
    public void postResult() throws SQLException {
        super.postResult();
        SQLiteProcessor proc = getContext();
        proc.getServer().flushPrivileges();
    }
    
    public static String[] getPrivileges() {
        return GrantStatement.getPrivileges();
    }

}
