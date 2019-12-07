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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.sqlite.TestBase;
import org.sqlite.server.util.locks.SpinLock;

/**
 * @author little-pan
 * @since 2019-10-19
 *
 */
public class SpinLockTest extends TestBase {
    
    protected final SpinLock lock = new SpinLock();
    final List<byte[]> buffers = Collections.synchronizedList(new ArrayList<byte[]>());

    public static void main(String[] args) throws SQLException {
        new SpinLockTest().test();
    }

    @Override
    protected void doTest() throws SQLException {
        simpleLockTest(false);
        simpleLockTest(true);
        
        reentrantTest("rt0", 0, 1);
        reentrantTest("rt1", 1, 1);
        reentrantTest("rt2", 2, 1);
        reentrantTest("rt3", 2, 2);
        reentrantTest("rt4", 2, 10);
        reentrantTest("rt5", 100, 10);
        reentrantTest("rt6", 1000, 10);
        reentrantTest("rt7", 10000, 10);
        reentrantTest("rt8", 100000, 1);
        reentrantTest("rt9", -1, 1);
        reentrantTest("rt10", -1, 5);
        reentrantTest("rt11", 2, 1);
        reentrantTest("rt12", 2, 2);
        reentrantTest("rt13", 2, 10);
        
        reentrantTest("rt14", 2, 10, true);
        reentrantTest("rt15", 200, 10, true);
        reentrantTest("rt16", 500, 1, true);
        reentrantTest("rt17", 1000, 2, true);
        reentrantTest("rt18", 10000, 10, true);
        reentrantTest("rt19", -1, 1, true);
        reentrantTest("rt20", -1, 10, true);
        
        simpleLockTest(true);
        simpleLockTest(false);
    }
    
    @Override
    protected void cleanup() {
        this.buffers.clear();
        super.cleanup();
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
    
    protected void reentrantTest(String prefix, final int deep, int concs) {
        reentrantTest(prefix, deep, concs, false);
    }
    
    protected void reentrantTest(String prefix, final int deep, int concs, final boolean oomTest) {
        info("reentrantTest(): %s deep %s, concs %s, oomTest %s", prefix, deep, concs, oomTest);
        this.buffers.clear();
        
        final AtomicReference<Throwable> cause = new AtomicReference<>();
        Thread [] workers = new Thread[concs];
        for (int i = 0; i < concs; ++i) {
            Thread worker = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        reentry(deep, oomTest);
                    } catch (Throwable e) {
                        cause.set(e);
                    }
                }
                
            }, prefix+"-worker-"+ i);
            worker.setDaemon(true);
            worker.start();
            workers[i] = worker;
        }
        
        // Check
        for (int i = 0; i < concs; ++i) {
            try {
                workers[i].join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        final Throwable e = cause.get();
        if (e != null) {
            throw new RuntimeException(e);
        }
    }
    
    protected void reentry(int deep, boolean oomTest) {
        if (deep == 0) {
            return;
        }
        
        this.lock.lock();
        try {
            if (oomTest) {
                final byte[] buffer = new byte[1 << 20];
                for (int i = 0; i < buffer.length; ++i) {
                    buffer[i] = (byte)i;
                }
                this.buffers.add(buffer);
            }
            
            if (deep < 0) {
                reentry(deep, oomTest);
            } else {
                if(--deep > 0) {
                    reentry(deep, oomTest);
                }
            }
        } catch (IllegalStateException e) {
            info("Expected exception: %s", e);
        } catch (OutOfMemoryError e) {
            this.buffers.clear(); // release
            info("Expected exception: %s", e);
        } finally {
            this.lock.unlock();
        }
    }

}
