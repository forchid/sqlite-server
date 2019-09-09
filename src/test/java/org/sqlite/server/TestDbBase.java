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

import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.sqlite.server.util.IoUtils;

/**
 * @author little-pan
 * @since 2019-09-05
 *
 */
public abstract class TestDbBase extends TestBase {
    protected static String url = "jdbc:postgresql://localhost:"+SQLiteServer.PORT_DEFAULT+"/test.db";
    protected static String user = "root";
    protected static String password = "123456";
    
    protected static SQLiteServer server;
    static {
        String dataDir= "data"+File.separator+"sqlite3Test";
        
        deleteDataDir(new File(dataDir));
        String[] initArgs = {"initdb", "-D", dataDir, "-p", password};
        server = SQLiteServer.create(initArgs);
        server.initdb(initArgs);
        IoUtils.close(server);
        
        String[] bootArgs = {"boot", "-D", dataDir, "-T"};
        server = SQLiteServer.create(bootArgs);
        server.bootAsync(bootArgs);
    }
    
    protected Connection getConnection() throws SQLException {
        return (DriverManager.getConnection(url, user, password));
    }
    
    protected static void deleteDataDir(File dataDir) {
        if (dataDir.isDirectory()) {
            dataDir.list(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    File f = new File(dir, name);
                    if (f.isFile()) {
                        return f.delete();
                    }
                    
                    return false;
                }
            });
            
            dataDir.delete();
        }
    }

}
