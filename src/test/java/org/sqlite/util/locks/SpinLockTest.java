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
package org.sqlite.util.locks;

import java.sql.SQLException;

import org.sqlite.TestBase;
import org.sqlite.server.util.locks.SpinLock;

/**
 * @author little-pan
 * @since 2019-10-19
 *
 */
public class SpinLockTest extends TestBase {
    
    protected final SpinLock lock = new SpinLock();

    public static void main(String[] args) throws SQLException {
        new SpinLockTest().test();
    }

    @Override
    protected void doTest() throws SQLException {
        simpleLockTest(false);
        simpleLockTest(true);
        
        reentrantTest("rt1", 2, 1);
        reentrantTest("rt2", 2, 2);
        reentrantTest("rt3", 2, 10);
        reentrantTest("rt4", 100, 10);
        reentrantTest("rt5", 1000, 10);
        reentrantTest("rt6", 10000, 10);
        reentrantTest("rt7", 100000, 1);
        reentrantTest("rt8", -1, 1);
        reentrantTest("rt9", -1, 5);
        reentrantTest("rt10", 2, 1);
        reentrantTest("rt11", 2, 2);
        reentrantTest("rt12", 2, 10);
        
        simpleLockTest(true);
        simpleLockTest(false);
    }
    
    protected void simpleLockTest(boolean thrown) {
        info("simpleLockTest(): thrown? %s", thrown);
        this.lock.lock();
        try {
            if (thrown) {
                throw new Exception();
            }
        } catch (Exception e) {
            // ignore
        } finally {
            this.lock.unlock();
        }
    }
    
    protected void reentrantTest(String prefix, final int levels, int concs) {
        info("reentrantTest(): %s levels %s, concs %s", prefix, levels, concs);
        
        Thread [] workers = new Thread[concs];
        for (int i = 0; i < concs; ++i) {
            Thread worker = new Thread(new Runnable() {
                @Override
                public void run() {
                    reentry(levels);
                }
                
            }, prefix+"-worker-"+ i);
            worker.setDaemon(true);
            worker.start();
            workers[i] = worker;
        }
        
        for (int i = 0; i < concs; ++i) {
            try {
                workers[i].join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    protected void reentry(int levels) {
        this.lock.lock();
        try {
            if (levels < 0) {
                info("infinite... "+
                "But oraclejdk8 and openjdk7/8 no StackOverflowError "+
                "on ubuntu 14 Travis-CI test environment so skip this test");
                //reentry(levels);
                //reentry(--levels);
            } else {
                if(--levels > 0) {
                    reentry(levels);
                }
            }
        } catch (IllegalStateException | StackOverflowError e) {
            info("Expected exception: %s", e);
        } finally {
            this.lock.unlock();
        }
    }

}
