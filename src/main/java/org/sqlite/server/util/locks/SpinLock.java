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
package org.sqlite.server.util.locks;

import java.util.concurrent.atomic.AtomicReference;
import static java.lang.Integer.*;

/** <p>A simple reentrant spin lock.</p>
 * 
 * <p> Add the system property {@code org.sqlite.server.spinLock.maxDeep} to limit reentrant max deep
 * for avoiding this reentrant spin lock violated by {@link StackOverflowError} since version 0.3.29.
 * </p>
 * 
 * @author little-pan
 * @since 2019-10-19
 *
 */
public class SpinLock {
    
    private static final int MAX_DEEP = getInteger("org.sqlite.server.spinLock.maxDeep", 1000);
    
    private final AtomicReference<Thread> locked;
    private int deep;
    
    public SpinLock() {
        this.locked = new AtomicReference<>();
    }
    
    /** Acquires the lock.
     * 
     * @throws IllegalStateException if reentrant deep exceeds the system property value of 
     * {@code org.sqlite.server.spinLock.maxDeep}
     */
    public void lock() throws IllegalStateException {
        final Thread current = Thread.currentThread();
        // support reentrant lock
        for (; !this.locked.compareAndSet(null, current) && 
                this.locked.get() != current;) {
            // spin
        }
        if (++this.deep > MAX_DEEP) {
            unlock();
            throw new IllegalStateException("Reentrant lock too deep(max " + MAX_DEEP + ")");
        }
    }
    
    /**
     * Releases the lock.
     * @throws IllegalMonitorStateException if the current thread doesn't own this lock since 0.3.29
     */
    public void unlock() throws IllegalMonitorStateException {
        if (Thread.currentThread() == this.locked.get()) {
            if (--this.deep == 0) {
                this.locked.set(null);
            }
        } else {
            throw new IllegalMonitorStateException();
        }
    }
    
}
