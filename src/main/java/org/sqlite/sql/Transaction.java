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

/** An instance that manages a transaction state.
 * 
 * @author little-pan
 * @since 2019-10-12
 *
 */
public class Transaction {
    
    protected final SQLContext context;
    protected final TransactionMode mode;
    protected final boolean readOnly;
    protected final boolean implicit;
    
    protected SQLStatement firstStatement;
    
    public Transaction(SQLContext context) {
        this(context, new TransactionMode(), false);
    }
    
    public Transaction(SQLContext context, TransactionMode mode) {
        this(context, mode, false);
    }
    
    public Transaction(SQLContext context, boolean implicit) {
        this(context, new TransactionMode(), implicit);
    }
    
    public Transaction(SQLContext context, TransactionMode mode, boolean implicit) {
        this.context = context;
        this.mode = mode;
        this.implicit = implicit;
        
        final Boolean readOnly = mode.isReadOnly();
        if (readOnly == null) {
            this.readOnly = context.isReadOnly();
        } else {
            this.readOnly = readOnly;
        }
    }
    
    public SQLContext getContext() {
        return this.context;
    }
    
    public SQLStatement getFirstStatement() {
        return this.firstStatement;
    }

    public void setFirstStatement(SQLStatement firstStatement) {
        this.firstStatement = firstStatement;
    }

    public TransactionMode getMode() {
        return this.mode;
    }
    
    public boolean isReadOnly() {
        return (this.readOnly);
    }
    
    public boolean isImplicit() {
        return implicit;
    }
    
}
