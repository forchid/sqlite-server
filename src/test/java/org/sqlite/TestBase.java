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

import org.sqlite.server.SQLiteServer;
import org.sqlite.server.Server;

import junit.framework.TestCase;

/**
 * @author little-pan
 * @since 2019-08-31
 *
 */
public abstract class TestBase extends TestCase {
    protected static String url = "jdbc:postgresql://localhost:"+Server.PORT_DEFAULT+"/test.db";
    protected static String user = "root";
    protected static String password = "123456";
    
    protected static final SQLiteServer server;
    static {
        server = new SQLiteServer();
        server.init("--trace", "-p", password);
        server.start();
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                server.listen();
            }
        });
        thread.setDaemon(true);
        thread.start();
    }
    
    public abstract void test() throws SQLException;
    
    protected Connection getConnection() throws SQLException {
        return (DriverManager.getConnection(url, user, password));
    }

}