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

/**PRAGMA statement:
 * PRAGMA [schema-name.]pragma-name [=pragma-value | (pragma-value)]
 * 
 * @author little-pan
 * @since 2019-09-05
 *
 */
public class PragmaStatement extends SQLStatement {
    
    protected String schemaName;
    protected String name;
    protected String value;
    
    public PragmaStatement(String sql, String command) {
        this(sql, command, false);
    }

    public PragmaStatement(String sql, String command, boolean query) {
        super(sql, command, query);
    }
    
    public String getSchemaName() {
        return schemaName;
    }
    
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
    
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
