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
package org.sqlite.server.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import org.sqlite.server.TestBase;

/**
 * @author little-pan
 * @since 2019-08-31
 *
 */
public class TestDriver extends TestBase {
    
    public static void main(String[] args) throws SQLException {
        new TestDriver().test();
    }
    
    public void test() throws SQLException {
        try (Connection conn = getConnection()) {
            assertTrue(conn != null);
        }
    }

}
