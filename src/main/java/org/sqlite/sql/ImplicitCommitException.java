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

/** An implicit transaction(not user transaction) commit runtime exception in SQLite server.
 * In SQLite DELETE journal mode, COMMIT maybe busy when a read transaction is active.
 * 
 * @author little-pan
 * @since 2019-09-30
 *
 */
public class ImplicitCommitException extends RuntimeException {
    
    private static final long serialVersionUID = -9053851805745416445L;
    
    public ImplicitCommitException(SQLException cause) {
        this("Failure of commit an implicit transaction", cause);
    }

    public ImplicitCommitException(String message, SQLException cause) {
        super(message, cause);
    }
    
    @Override
    public SQLException getCause() {
        return (SQLException)super.getCause();
    }
    
}
