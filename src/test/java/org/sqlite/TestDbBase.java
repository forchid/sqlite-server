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

import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.sqlite.server.SQLiteServer;
import org.sqlite.util.IoUtils;

/**
 * @author little-pan
 * @since 2019-09-05
 *
 */
public abstract class TestDbBase extends TestBase {
    
    protected static final String dataDir = getDataDir();
    
    protected static String user = "root";
    protected static String password = "123456";
    
    protected static final String [] environments = {
        "SQLite WAL pg extended query environment", "SQLite DELETE pg extended query environment",
        "SQLite WAL pg simple query environment", "SQLite DELETE pg simple query environment",
    };
    
    protected static final String [] urls = {
        "jdbc:postgresql://localhost:"+getPortDefault()+"/"+getDbDefault()+
            "?preferQueryMode=extended&socketFactory=org.sqlite.server.jdbc.pg.PgSocketFactory"
            ,//"&loggerLevel=TRACE&loggerFile=./logs/pgjdbc.log",
        "jdbc:postgresql://localhost:"+getPortDefault()+"/"+getDbDefault()+
            "?preferQueryMode=extended&socketFactory=org.sqlite.server.jdbc.pg.PgSocketFactory"
            ,//"&loggerLevel=TRACE&loggerFile=./logs/pgjdbc.log",
        "jdbc:postgresql://localhost:"+getPortDefault()+"/"+getDbDefault()+
            "?preferQueryMode=simple&socketFactory=org.sqlite.server.jdbc.pg.PgSocketFactory"
            ,//"&loggerLevel=TRACE&loggerFile=./logs/pgjdbc.log",
        "jdbc:postgresql://localhost:"+getPortDefault()+"/"+getDbDefault()+
            "?preferQueryMode=simple&socketFactory=org.sqlite.server.jdbc.pg.PgSocketFactory"
            ,//"&loggerLevel=TRACE&loggerFile=./logs/pgjdbc.log",
    };
    
    protected static final String [][] initArgsList = new String[][] {
        {"-D", dataDir, "-p", password, "--journal-mode", "wal"},
        {"-D", dataDir, "-p", password, "--journal-mode", "delete", 
            "-S", "off"
        },
        {"-D", dataDir, "-p", password, "--journal-mode", "wal"},
        {"-D", dataDir, "-p", password, "--journal-mode", "delete", 
            "-S", "off"
        },
    };
    
    protected static final String [][] bootArgsList = new String[][] {
        {"-D", dataDir, //"--trace-error", "-T",
            "--worker-count", "4", "--max-conns", "50",
            "--journal-mode", "wal"
        },
        {"-D", dataDir, //"--trace-error", //"-T", 
            "--worker-count", "4", "--max-conns", "50",
            "--journal-mode", "delete", "-S", "off"
        },
        {"-D", dataDir, //"--trace-error", "-T",
            "--worker-count", "4", "--max-conns", "50",
            "--journal-mode", "wal"
        },
        {"-D", dataDir, //"--trace-error", //"-T", 
            "--worker-count", "4", "--max-conns", "50",
            "--journal-mode", "delete", "-S", "off"
        },
    };
    
    protected DbTestEnv currentEnv;
    protected int envIndex, envMax;
    
    protected void init() {
        this.envIndex = 0;
        this.envMax   = initArgsList.length;
    }
    
    public Iterator<TestEnv> iterator() {
        this.init();
        return new DbTestEnvIterator(this);
    }
    
    @Override
    protected void cleanup() {
        IoUtils.close(this.currentEnv);
        super.cleanup();
    }
    
    protected static String getUrl(int port, String path) {
        return ("jdbc:postgresql://localhost:"+port+"/"+path);
    }
    
    protected Connection getConnection(String url) throws SQLException {
        if (!url.startsWith("jdbc:")) {
            url = getUrl(getPortDefault(), url);
        }
        return (DriverManager.getConnection(url, user, password));
    }
    
    protected Connection getConnection(String url, String user, String password) 
            throws SQLException {
        if (!url.startsWith("jdbc:")) {
            url = getUrl(getPortDefault(), url);
        }
        return (DriverManager.getConnection(url, user, password));
    }
    
    protected Connection getConnection(String user, String password) throws SQLException {
        String url = getUrl(getPortDefault(), getDbDefault());
        return (DriverManager.getConnection(url, user, password));
    }
    
    protected Connection getConnection(int port, String path, String user, String password) 
            throws SQLException {
        String url = getUrl(port, path);
        return (DriverManager.getConnection(url, user, password));
    }
    
    protected Connection getConnection() throws SQLException {
        String url = getUrl();
        return (getConnection(url, user, password));
    }
    
    protected Connection getConnection(boolean fromPool) throws SQLException {
        if (fromPool) {
            return currentEnv.dataSource.getConnection();
        }
        
        return (getConnection());
    }
    
    protected String getUrl() {
        return (urls[this.currentEnv.envIndex]);
    }
    
    protected static String getUserDefault() {
        return user;
    }
    
    protected static String getDbDefault() {
        return getUserDefault();
    }
    
    protected static int getPortDefault() {
        return SQLiteServer.PORT_DEFAULT;
    }
    
    protected int getWorkCount() {
        return this.currentEnv.getWorkerCount();
    }
    
    protected int getMaxConns() {
        return this.currentEnv.getMaxConns();
    }
    
    protected static String getDataDir() {
        return getDataDir(null);
    }
    
    protected static String getDataDir(String dataDir) {
        if (dataDir == null || dataDir.length() == 0) {
            dataDir = "sqlite3Test";
        }
        return ("data"+File.separator+dataDir);
    }
    
    protected static String getExtraDir() {
        return getDataDir("extraTest");
    }
    
    protected void initTableAccounts() throws SQLException {
        try (Connection conn = getConnection()) {
            initTableAccounts(conn);
        }
    }
    
    protected void initTableAccounts(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("drop table if exists accounts");
        String sql = "create table accounts("
                //+ "id serial primary key, "
                + "id integer primary key, "
                + "name varchar(50) not null, "
                + "balance decimal(12, 2))";
        stmt.executeUpdate(sql);
    }
    
    protected static void deleteDataDir(String dataDir) {
        File target = new File(dataDir);
        deleteDataDir(target);
    }
    
    protected static void deleteDataDir(File dataDir) {
        assertTrue(!dataDir.exists() || dataDir.isDirectory());
        
        dataDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                File f = new File(dir, name);
                if (f.isFile()) {
                    boolean ok = f.delete();
                    // wait for sqlite3 closing DB
                    for (int i = 0;!ok && i < 10; ++i) {
                        try {
                            Thread.sleep(100L);
                            ok = f.delete();
                        } catch (InterruptedException e) {}
                    }
                    assertTrue(ok);
                    return ok;
                }
                
                return false;
            }
        });
        
        assertTrue(!dataDir.exists() || dataDir.delete());
    }

    protected void connectionTest(Connection conn, String sql, String result)
            throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            if (stmt.execute(sql)) {
                try (ResultSet rs = stmt.getResultSet()) {
                    if (result == null) {
                        assertTrue(!rs.next());
                    } else {
                        assertTrue(rs.next());
                        assertTrue(result.equals(rs.getString(1)));
                    }
                }
            } else {
                assertTrue(result.equals(stmt.getUpdateCount()+""));
            }
        }
    }
    
    protected static class DbTestEnv extends TestEnv {
        protected SQLiteServer server;
        protected DataSource dataSource;
        
        protected final boolean simpleQuery;
        protected final int envIndex;
        
        protected DbTestEnv(int envIndex) {
            int i = envIndex;
            this.envIndex = envIndex;
            this.name = environments[envIndex];
            
            deleteDataDir(new File(dataDir));
            
            String[] initArgs = initArgsList[i];
            SQLiteServer svr = SQLiteServer.create(initArgs);
            svr.initdb(initArgs);
            IoUtils.close(svr);
            
            String[] bootArgs = bootArgsList[i];
            this.server = SQLiteServer.create(bootArgs);
            this.server.bootAsync(bootArgs);
            
            String url = urls[i];
            this.dataSource = new DataSource();
            int maxActive = this.server.getMaxConns() * getWorkerCount();
            this.dataSource.setMaxActive(maxActive);
            this.dataSource.setMaxIdle(10);
            this.dataSource.setMinIdle(0);
            this.dataSource.setDriverClassName("org.postgresql.Driver");
            this.dataSource.setUrl(url);
            this.dataSource.setUsername(getUserDefault());
            this.dataSource.setPassword(password);
            
            this.simpleQuery = url.contains("preferQueryMode=simple");
        }
        
        public int getWorkerCount() {
            return this.server.getWorkerCount();
        }
        
        public int getMaxConns() {
            return this.server.getMaxConns();
        }
        
        public boolean isSimpleQuery() {
            return this.simpleQuery;
        }
        
        @Override
        public void close() {
            if (this.dataSource != null) {
                this.dataSource.close();
                sleep(100L);
            }
            IoUtils.close(this.server);
            deleteDataDir(new File(dataDir));
            
            super.close();
        }
    }
    
    protected static class DbTestEnvIterator implements Iterator<TestEnv> {
        protected final TestDbBase base;
        protected boolean hasNextCalled;
        
        protected DbTestEnvIterator(TestDbBase base) {
            this.base = base;
        }
        
        @Override
        public boolean hasNext() {
            this.hasNextCalled = true;
            return (base.envIndex < base.envMax);
        }

        @Override
        public TestEnv next() {
            if (this.hasNextCalled) {
                if (hasNext()) {
                    DbTestEnv env = new DbTestEnv(base.envIndex++);
                    this.hasNextCalled = false;
                    return (this.base.currentEnv = env);
                } else {
                    throw new NoSuchElementException();
                }
            } else {
                throw new IllegalStateException("hasNext() not called since the last call");
            }
        }

        @Override
        public void remove() {
            
        }
    }
    
}
