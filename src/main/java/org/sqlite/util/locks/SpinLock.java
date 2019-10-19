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

import java.util.concurrent.atomic.AtomicReference;

/** A simple reentrant spin lock.
 * 
 * @author little-pan
 * @since 2019-10-19
 *
 */
public class SpinLock {
    
    private final AtomicReference<Thread> locked;
    private int levels;
    
    public SpinLock() {
        this.locked = new AtomicReference<>();
    }
    
    /** Acquires the lock.
     * 
     * @throws IllegalStateException if reentrant levels reaches {@code Integer.MAX_VALUE}
     */
    public void lock() throws IllegalStateException {
        final Thread current = Thread.currentThread();
        // support reentrant lock
        for (; !this.locked.compareAndSet(null, current) && 
                this.locked.get() != current;) {
            // spin
        }
        if (++this.levels == Integer.MAX_VALUE) {
            unlock();
            throw new IllegalStateException("Reentrant levels too deep: " + Integer.MAX_VALUE);
        }
    }
    
    /**
     * Releases the lock.
     */
    public void unlock() {
        final Thread current = Thread.currentThread();
        if (this.locked.get() == current && --this.levels == 0) {
            this.locked.set(null);
        }
    }
    
}
