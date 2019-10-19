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
package org.sqlite.server.sql.local;

import java.sql.SQLException;

import org.sqlite.SQLiteErrorCode;
import org.sqlite.server.SQLiteProcessor;
import org.sqlite.sql.SQLStatement;
import org.sqlite.sql.Transaction;
import org.sqlite.sql.TransactionMode;
import org.sqlite.util.ConvertUtils;

/** SET {TRANSACTION | SESSION CHARACTERISTICS AS TRANSACTION} transaction_mode [, ...].
 * 
 * @author little-pan
 * @since 2019-10-10
 *
 */
public class SetTransactionStatement extends LocalStatement {
    
    protected boolean sessionScope;
    protected TransactionMode transactionMode;
    
    public SetTransactionStatement(String sql) {
        super(sql, "SET TRANSACTION");
    }

    public boolean isSessionScope() {
        return sessionScope;
    }

    public void setSessionScope(boolean sessionScope) {
        this.sessionScope = sessionScope;
    }

    public TransactionMode getTransactionMode() {
        return transactionMode;
    }

    public void setTransactionMode(TransactionMode transactionMode) {
        this.transactionMode = transactionMode;
    }
    
    @Override
    protected void checkReadOnly() throws SQLException {
        if (isSessionScope()) {
            return;
        }
        super.checkReadOnly();
    }
    
    @Override
    protected String getSQL(String localSchema) throws SQLException {
        return getUpdatableSQL(localSchema, true);
    }
    
    @Override
    protected boolean doExecute(int maxRows) throws SQLException {
        TransactionMode mode = getTransactionMode();
        Boolean readOnly = mode.isReadOnly();
        
        if (readOnly != null) {
            SQLiteProcessor context = getContext();
            if (isSessionScope()) {
                context.setReadOnly(readOnly);
            } else {
                Transaction tx = context.getTransaction();
                if (tx != null) {
                    SQLStatement firstStmt = tx.getFirstStatement();
                    if (firstStmt != null) {
                        SQLiteErrorCode sqlError = SQLiteErrorCode.SQLITE_ERROR;
                        throw ConvertUtils.convertError(sqlError, sqlError.message, "25001");
                    }
                    tx.setReadOnly(readOnly);
                }
            }
        }
        
        return super.doExecute(maxRows);
    }
    
}
