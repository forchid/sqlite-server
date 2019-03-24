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
package org.sqlite.jdbc.v3;

import java.sql.SQLException;
import java.sql.Savepoint;

/**
 * @author little-pan
 * @since 2019-03-24
 *
 */
public class JDBC3Savepoint implements Savepoint {

    final int id;

    final String name;

    public JDBC3Savepoint(int id) {
        this.id = id;
        this.name = null;
    }

    public JDBC3Savepoint(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getSavepointId() throws SQLException {
        return id;
    }

    public String getSavepointName() throws SQLException {
        return name == null ? String.format("SQLITE_SAVEPOINT_%s", id) : name;
    }

}
