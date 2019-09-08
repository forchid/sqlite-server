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

/**SQL statement.
 * 
 * @author little-pan
 * @since 2019-09-04
 *
 */
public class SQLStatement {
    
    protected final String command;
    protected final String sql;
    
    protected boolean query;
    protected boolean comment;
    protected boolean empty;
    
    public SQLStatement(String sql) {
        this(sql, "");
        this.empty = true;
    }
    
    public SQLStatement(String sql, String command){
        this.sql = sql;
        this.command = command;
    }
    
    public SQLStatement(String sql, String command, boolean query){
        this.sql = sql;
        this.command = command;
        this.query = query;
    }
    
    public String getSQL() {
        return this.sql;
    }
    
    public String getCommand() {
        return this.command;
    }
    
    public boolean isQuery() {
        return this.query;
    }
    
    public void setQuery(boolean query) {
        this.query = query;
    }
    
    public boolean isTransaction() {
        return (this instanceof TransactionStatement);
    }
    
    public boolean isComment() {
        return this.comment;
    }
    
    public void setComment(boolean comment) {
        this.comment = comment;
    }
    
    public boolean isEmpty() {
        return this.empty;
    }
    
    public void setEmpty(boolean empty) {
        this.empty = empty;
    }
    
    @Override
    public String toString() {
        return this.sql;
    }
    
}
