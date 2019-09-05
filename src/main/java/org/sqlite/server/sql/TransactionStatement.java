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

/**Transaction statement:
 * BEGIN, COMMIT/ROLLBACK etc
 * 
 * @author little-pan
 * @since 2019-09-05
 *
 */
public class TransactionStatement extends SQLStatement {
    
    public TransactionStatement(String sql, String command) {
        super(sql, command);
        this.tx = true;
    }
    
    public boolean isBegin() {
        return ("BEGIN".equals(getCommand()));
    }
    
    public boolean isCommit() {
        String cmd = getCommand();
        return ("COMMIT".equals(cmd) || "END".equals(cmd));
    }
    
    public boolean isRollback() {
        return ("ROLLBACK".equals(getCommand()));
    }
}

