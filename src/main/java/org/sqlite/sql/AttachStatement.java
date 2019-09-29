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

import java.io.File;
import java.sql.SQLException;

import org.sqlite.util.StringUtils;

/**The ATTACh statement: 
 * ATTACH [DATABASE] expr AS schema-name
 * 
 * @author little-pan
 * @since 2019-09-05
 *
 */
public class AttachStatement extends SQLStatement {
    
    protected String dbName;
    protected String schemaName;
    
    public AttachStatement(String sql) {
        super(sql, "ATTACH", true/* Not write into the master DB */);
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
    
    public boolean isMemory() {
        String db = getDbName();
        return (":memory".equals(db));
    }
    
    public boolean isTemporary() {
        String db = getDbName();
        return ("".equals(db));
    }
    
    @Override
    protected void checkPermission() throws SQLException {
        if (isMemory() || isTemporary()) {
            return;
        }
        
        final String metaDb = this.context.getMetaDbName();
        final String fileName = getFileName();
        if (metaDb.equals(fileName)) {
            String message = "Can't attach a database file named by meta db's name";
            throw new SQLException(message, "42000", 1);
        }
        
        final String dirPath = getDirPath();
        final File dbFile;
        if (dirPath == null) {
            dbFile = new File(this.context.getDataDir(), fileName);
        } else {
            dbFile = new File(dirPath, fileName);
        }
        setDbName(dbFile.getAbsolutePath());
        
        this.context.checkPermission(this);
    }
    
    @Override
    public String getExecutableSQL() throws SQLException {
        String f = "attach database '%s' as '%s'";
        String sql = String.format(f, getDbName(), getSchemaName());
        
        // check
        try (SQLParser parser = new SQLParser(sql)) {
            SQLStatement stmt = parser.next();
            if ("ATTACH".equals(stmt.getCommand()) && !parser.hasNext()) {
                return stmt.getSQL();
            }
        }
        
        throw new SQLParseException(getSQL());
    }
    
    public String getFileName() {
        if (isMemory() || isTemporary()) {
            return getDbName();
        }
        File dbFile = new File(getDbName());
        return (StringUtils.toLowerEnglish(dbFile.getName()));
    }
    
    public String getDirPath() {
        if (isMemory() || isTemporary()) {
            return null;
        }
        
        String dbName = getDbName();
        File dbFile = new File(dbName);
        if (dbFile.getName().equalsIgnoreCase(dbName)) {
            return null;
        }
        
        return (dbFile.getParentFile().getAbsolutePath());
    }
    
}

