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

import static java.lang.String.*;

/**Transaction mode: isolation level, read only or read write.
 * 
 * @author little-pan
 * @since 2019-10-10
 *
 */
public class TransactionMode {
    
    public static final int READ_UNCOMMITTED = 1;
    public static final int READ_COMMITTED   = 2;
    public static final int REPEATABLE_READ  = 3;
    public static final int SERIALIZABLE     = 4;
    
    protected int isolationLevel;
    private Boolean readOnly; // null if not set
    
    public TransactionMode() {
        this.isolationLevel = SERIALIZABLE;
    }
    
    public int getIsolationLevel() {
        return this.isolationLevel;
    }

    public void setIsolationLevel(int isolationLevel) throws IllegalArgumentException {
        switch (isolationLevel) {
        case READ_UNCOMMITTED:
        case READ_COMMITTED:
        case REPEATABLE_READ:
        case SERIALIZABLE:
            break;
        default:
            throw new IllegalArgumentException("isolationLevel " + isolationLevel);
        }
        this.isolationLevel = isolationLevel;
    }

    public Boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(Boolean readOnly) {
        this.readOnly = readOnly;
    }
    
    @Override
    public String toString() {
        String isolation;
        switch (this.isolationLevel) {
        case READ_UNCOMMITTED:
            isolation = "READ UNCOMMITTED";
            break;
        case READ_COMMITTED:
            isolation = "READ COMMITTED";
            break;
        case REPEATABLE_READ:
            isolation = "REPEATABLE READ";
            break;
        case SERIALIZABLE:
            isolation = "SERIALIZABLE";
            break;
        default:
            throw new IllegalArgumentException("isolation " + isolationLevel);
        }
        
        return (format("TransactionMode[isolation %s, readOnly %s]", isolation, this.readOnly));
    }
    
}
