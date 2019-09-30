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

import java.io.PrintStream;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import junit.framework.TestCase;

/**
 * @author little-pan
 * @since 2019-08-31
 *
 */
public abstract class TestBase extends TestCase implements Iterable<TestEnv> {
    
    static final String LINESEP = System.getProperty("line.separator");
    protected static boolean disableINFO = false, disableERROR = true;
    
    protected int iteration = 0, iterations = 2;
    
    public final void test() throws SQLException {
        long since = System.currentTimeMillis();
        boolean failed = true;
        try {
            for (int i = 0; i < this.iterations; ++i) {
                this.iteration = i;
                for (TestEnv env : this) {
                    println("Iteration %d: test in %s", this.iteration, env);
                    doTest();
                    env.close();
                }
                cleanup();
            }
            println("Total time: %dms", (System.currentTimeMillis() - since));
            failed = false;
        } finally {
            cleanup();
            if (failed) {
                // wait for logger
                sleep(1000L);
            }
        }
    }
    
    protected abstract void doTest() throws SQLException;
    
    public Iterator<TestEnv> iterator() {
        return new OnceTestEnvIterator();
    }
    
    protected void cleanup() {
        // NOOP
    }
    
    public static void println(String format, Object ...args) {
        String f = format + LINESEP;
        printf(System.out, "CONS", f, args);
    }
    
    public static void info(String format, Object ...args) {
        if (disableINFO || disableERROR) {
            return;
        }
        
        String f = format + LINESEP;
        printf(System.out, "INFO", f, args);
    }
    
    public static void error(String format, Object ...args) {
        if (disableERROR) {
            return;
        }
        
        String f = format + LINESEP;
        printf(System.err, "ERROR", f, args);
    }
    
    public static void printf(PrintStream out, String tag, String format, Object ...args) {
        DateFormat df = new SimpleDateFormat("MM/dd HH:mm:ss.SSS");
        String prefix = String.format("%s [%-5s][%s] ", df.format(new Date()), tag, 
                Thread.currentThread().getName());
        out.printf(prefix +format, args);
    }
    
    public static void sleep(long millis) {
        try { Thread.sleep(millis); } 
        catch (InterruptedException e) {}
    }
    
    protected static class OnceTestEnvIterator implements Iterator<TestEnv> {
        
        protected boolean hasNext = true;

        @Override
        public boolean hasNext() {
            return this.hasNext;
        }

        @Override
        public TestEnv next() {
            this.hasNext = false;
            return new TestEnv();
        }

        @Override
        public void remove() {
            
        }
    }
    
}
