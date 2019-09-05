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
package org.sqlite.server.sql;

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
        super(sql, "ATTACH");
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
    
}

