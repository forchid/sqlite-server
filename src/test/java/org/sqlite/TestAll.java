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
package org.sqlite;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.sqlite.server.SQLiteServerTest;
import org.sqlite.server.jdbc.ConnectionTest;
import org.sqlite.server.jdbc.PreparedStatementTest;
import org.sqlite.server.jdbc.StatementTest;
import org.sqlite.server.jdbc.TransactionTest;
import org.sqlite.sql.SQLParserTest;
import org.sqlite.sql.SQLReaderTest;
import org.sqlite.util.DateTimeUtilsTest;
import org.sqlite.util.locks.SpinLockTest;

/**
 * @author little-pan
 * @since 2019-08-31
 *
 */
public class TestAll extends TestBase {
    
    protected final List<TestBase> tests = new ArrayList<>();
    
    public static void main(String[] args) throws SQLException {
        new TestAll().test();
    }

    @Override
    protected void doTest() throws SQLException {
        addAll().doTestAll();
    }
    
    @Override
    protected void cleanup() {
        this.tests.clear();
        super.cleanup();
    }
    
    protected TestAll doTestAll() throws SQLException {
        for(TestBase test: tests) {
            String className = test.getClass().getName();
            long start = System.currentTimeMillis();
            println("%s start", className);
            test.test();
            long end = System.currentTimeMillis();
            println("%s ok(%dms)", className, end - start);
        }
        return this;
    }

    protected TestAll add(TestBase test) {
        tests.add(test);
        return this;
    }
    
    protected TestAll addAll() {
        add(new ConnectionTest()).
        add(new DateTimeUtilsTest()).
        add(new PreparedStatementTest()).
        add(new SpinLockTest()).
        add(new StatementTest()).
        add(new SQLReaderTest()).
        add(new SQLParserTest()).
        add(new SQLiteServerTest()).
        add(new TransactionTest());
        
        return this;
    }
    
}
