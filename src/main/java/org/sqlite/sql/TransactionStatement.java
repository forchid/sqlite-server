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

import java.sql.SQLException;

/**Transaction statement:
 * BEGIN, COMMIT/ROLLBACK etc
 * 
 * @author little-pan
 * @since 2019-09-05
 *
 */
public class TransactionStatement extends SQLStatement {
    
    protected String savepointName;
    protected boolean txPrepared;
    
    public TransactionStatement(String sql, String command) {
        super(sql, command);
    }
    
    public TransactionStatement(String sql, String command, String savepointName) {
        this(sql, command);
        this.savepointName = savepointName;
    }
    
    @Override
    public boolean isTransaction() {
        return true;
    }
    
    @Override
    protected void checkPermission() throws SQLException {
        // All transaction statements PASS
    }
    
    @Override
    protected void preExecute(int maxRows) throws SQLException, IllegalStateException {
        super.preExecute(maxRows);
        
        if (this.context.isAutoCommit() && (isBegin() || isSavepoint())) {
            this.context.prepareTransaction(this);
            this.txPrepared = true;
        }
    }
    
    @Override
    protected boolean doExecute(int maxRows) throws SQLException {
        boolean prepared = this.txPrepared;
        boolean failed = true;
        try {
            boolean resultSet = super.doExecute(maxRows);
            
            if (!prepared && isSavepoint()) {
                this.context.pushSavepoint(this);
            }
            failed = false;
            return resultSet;
        } finally {
            if (failed && prepared) {
                this.context.resetAutoCommit();
            }
        }
    }
    
    @Override
    public void postResult() throws SQLException {
        String command = this.command;
        switch (command) {
        case "COMMIT":
        case "END":
        case "ROLLBACK":
            if (hasSavepoint()) {
                break;
            }
            this.context.releaseTransaction(this, true);
            break;
        case "RELEASE":
            this.context.releaseTransaction(this, false);
            break;
        }
        
        super.postResult();
    }
    
    public String getSavepointName() {
        return this.savepointName;
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
    
    public boolean isSavepoint() {
        return ("SAVEPOINT".equals(getCommand()));
    }
    
    public boolean isRelease() {
        return ("RELEASE".equals(getCommand()));
    }
    
    public boolean hasSavepoint() {
        return (getSavepointName() != null);
    }
    
}

