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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.sqlite.server.jdbc.TestDriver;
import org.sqlite.server.jdbc.TestStatement;
import org.sqlite.sql.TestSQLParser;
import org.sqlite.sql.TestSQLReader;

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
    public void test() throws SQLException {
        addAll();
        doTest();
    }
    
    protected void doTest() throws SQLException {
        for(TestBase test: tests) {
            String className = test.getClass().getName();
            long start = System.currentTimeMillis();
            println("%s start", className);
            test.test();
            long end = System.currentTimeMillis();
            println("%s ok(%dms)", className, end - start);
        }
        tests.clear();
    }

    protected TestAll add(TestBase test) {
        tests.add(test);
        return this;
    }
    
    protected void addAll() {
        add(new TestDriver()).
        add(new TestStatement()).
        add(new TestSQLReader()).
        add(new TestSQLParser());
    }
    
}
