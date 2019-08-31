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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.sqlite.jdbc.Driver;

import junit.framework.TestCase;

/**
 * @author little-pan
 * @since 2019-08-31
 *
 */
public abstract class TestBase extends TestCase {
    protected static String url = "jdbc:sqlite-server://localhost/test.db";
    protected static String user = "root";
    protected static String password = "123456";
    
    public abstract void test() throws SQLException;
    
    protected Connection getConnection() throws SQLException {
        Driver.load();
        
        return (DriverManager.getConnection(url, user, password));
    }

}
