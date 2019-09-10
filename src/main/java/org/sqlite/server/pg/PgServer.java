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
package org.sqlite.server.pg;

import java.net.Socket;
import java.sql.Types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.server.SQLiteAuthMethod;
import org.sqlite.server.SQLiteProcessor;
import org.sqlite.server.SQLiteServer;

/**
 * This SQLite server implements a subset of the PostgreSQL protocol.
 * 
 * @author little-pan
 * @since 2019-09-01
 *
 */
public class PgServer extends SQLiteServer {
    static final Logger log = LoggerFactory.getLogger(PgServer.class);
    
    public static final String PG_VERSION = "8.2.23";
    
    public static final String AUTH_TRUST = "trust";
    public static final String AUTH_MD5   = "md5";
    public static final String AUTH_PASSWORD  = "password";
    public static final String AUTH_DEFAULT   = AUTH_MD5;
    
    /**
     * The VARCHAR type.
     */
    public static final int PG_TYPE_VARCHAR = 1043;

    public static final int PG_TYPE_BOOL = 16;
    public static final int PG_TYPE_BYTEA = 17;
    public static final int PG_TYPE_BPCHAR = 1042;
    public static final int PG_TYPE_INT8 = 20;
    public static final int PG_TYPE_INT2 = 21;
    public static final int PG_TYPE_INT4 = 23;
    public static final int PG_TYPE_TEXT = 25;
    public static final int PG_TYPE_OID = 26;
    public static final int PG_TYPE_FLOAT4 = 700;
    public static final int PG_TYPE_FLOAT8 = 701;
    public static final int PG_TYPE_UNKNOWN = 705;
    public static final int PG_TYPE_TEXTARRAY = 1009;
    public static final int PG_TYPE_DATE = 1082;
    public static final int PG_TYPE_TIME = 1083;
    public static final int PG_TYPE_TIMESTAMP_NO_TMZONE = 1114;
    public static final int PG_TYPE_NUMERIC = 1700;
    
    private String key, keyDatabase;
    
    public static void main(String args[]) {
        main(new PgServer(), args);
    }
    
    public PgServer() {
        super("pg");
        this.authMethod = AUTH_DEFAULT;
    }
    
    @Override
    public void init(String... args) {
        super.init(args);
        
        if (args != null) {
            for (int i = 0, argc = args.length; i < argc; ++i) {
                String a = args[i];
                if ("--key".equals(a) || "-K".equals(a)) {
                    this.key = args[++i];
                    this.keyDatabase = args[++i];
                }
            }
        }
        
        // check authentication method
        if (CMD_INITDB.equals(this.command)) {
            switch (this.authMethod) {
            case AUTH_MD5:
            case AUTH_PASSWORD:
                if (getPassword() == null) {
                    throw new IllegalArgumentException("No password was provided");
                }
                break;
            case AUTH_TRUST:
                break;
            default:
                throw new IllegalArgumentException("Unknown auth method: " + this.authMethod);
            }
        }
    }
    
    @Override
    public String getBootHelp() {
        return super.getBootHelp() + "\n"
                + "  --key|-K  <key> <keyDatabase> The database specified by the arg key";
    }

    @Override
    public String getName() {
        return "SQLite PG server";
    }
    
    @Override
    public String getVersion() {
        return PG_VERSION;
    }
    
    @Override
    protected SQLiteProcessor newProcessor(Socket s, int processId) {
        return new PgProcessor(s, processId, this);
    }
    
    @Override
    public String getAuthMethods() {
        return AUTH_MD5 + "," + AUTH_PASSWORD + "," + AUTH_TRUST;
    }
    
    @Override
    public String getAuthDefault() {
        return AUTH_DEFAULT;
    }
    
    /**
     * If no key is set, return the original database name. If a key is set,
     * check if the key matches. If yes, return the correct database name. If
     * not, throw an exception.
     *
     * @param db the key to test (or database name if no key is used)
     * @return the database name
     * @throws IllegalArgumentException if a key is set but doesn't match
     */
    public String checkKeyAndGetDatabaseName(String db) {
        if (this.key == null) {
            return db;
        }
        if (this.key.equals(db)) {
            return this.keyDatabase;
        }
        
        throw new IllegalArgumentException("key '" + db + "'");
    }
    
    public static int convertType(final int type) {
        switch (type) {
        case Types.BOOLEAN:
            return PG_TYPE_BOOL;
        case Types.VARCHAR:
            return PG_TYPE_VARCHAR;
        case Types.CLOB:
            return PG_TYPE_TEXT;
        case Types.CHAR:
            return PG_TYPE_BPCHAR;
        case Types.SMALLINT:
            return PG_TYPE_INT2;
        case Types.INTEGER:
            return PG_TYPE_INT4;
        case Types.BIGINT:
            return PG_TYPE_INT8;
        case Types.DECIMAL:
            return PG_TYPE_NUMERIC;
        case Types.REAL:
            return PG_TYPE_FLOAT4;
        case Types.DOUBLE:
            return PG_TYPE_FLOAT8;
        case Types.TIME:
            return PG_TYPE_TIME;
        case Types.DATE:
            return PG_TYPE_DATE;
        case Types.TIMESTAMP:
            return PG_TYPE_TIMESTAMP_NO_TMZONE;
        case Types.VARBINARY:
            return PG_TYPE_BYTEA;
        case Types.BLOB:
            return PG_TYPE_OID;
        case Types.ARRAY:
            return PG_TYPE_TEXTARRAY;
        default:
            return PG_TYPE_UNKNOWN;
        }
    }

    @Override
    public SQLiteAuthMethod newAuthMethod(String authMethod) {
        String procotol = getProtocol();
        switch (authMethod) {
        case AUTH_MD5:
            return new MD5Password(procotol);
        case AUTH_PASSWORD:
            return new CleartextPassword(procotol);
        case AUTH_TRUST:
            return new TrustAuthMethod(procotol);
        default:
            throw new IllegalArgumentException("authMethod " + authMethod);
        }
    }
    
}
