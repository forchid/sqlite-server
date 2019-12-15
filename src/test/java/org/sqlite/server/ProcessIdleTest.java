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
package org.sqlite.server;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.sqlite.TestDbBase;

/** SQLite server process idle test.
 * 
 * @author little-pan
 * @since 2019-12-15
 *
 */
public class ProcessIdleTest extends TestDbBase {
    
    public static void main(String[] args) throws SQLException {
        new ProcessIdleTest().test();
    }

    @Override
    protected void doTest() throws SQLException {
        idleInTxTimeoutTest("select 1");
    }
    
    protected void idleInTxTimeoutTest(String sql) throws SQLException {
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            Statement s = conn.createStatement();
            s.execute(sql);
            
            info("ProcessIdleTest: idle...");
            sleep(this.currentEnv.getSleepInTxTimeout() + 250L);
            info("ProcessIdleTest: wakeup");
            
            try {
                conn.rollback();
                fail("Not closed after idle timeout");
            } catch (SQLException e) {
                if (!e.getSQLState().startsWith("08")) {
                    throw e;
                }
            }
            
            s.close();
        }
    }

}
