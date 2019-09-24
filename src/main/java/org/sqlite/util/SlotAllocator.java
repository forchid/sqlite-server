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
package org.sqlite.util;

/** Slot allocator based on array
 * 
 * @author little-pan
 * @since 2019-09-24
 *
 */
public class SlotAllocator<T> {
    
    protected final Object[] slots;
    protected int maxSlot; // the slot maybe allocated quickly
    private int size;      // allocated slot number
    
    public SlotAllocator() {
        this(16);
    }
    
    public SlotAllocator(int capacity) {
        this.slots = new Object[capacity];
    }
    
    public int allocate(T t) throws NullPointerException {
        if (t == null) {
            throw new NullPointerException("Argument t");
        }
        
        int maxSlot = this.maxSlot;
        if (maxSlot < this.slots.length) {
            int i = this.maxSlot++;
            this.slots[i] = t;
            ++this.size;
            return i;
        }
        
        for (int i = 0; i < maxSlot; ++i) {
            Object o = this.slots[i];
            if (o == null) {
                this.slots[i] = t;
                ++this.size;
                return i;
            }
        }
        
        return -1;
    }
    
    public boolean deallocate(int i, T t) throws NullPointerException {
        if (t == null) {
            throw new NullPointerException("Argument t");
        }
        
        final Object o = this.slots[i];
        if (o != t) {
            return false;
        }
        
        this.slots[i] = null;
        --this.size;
        if (this.maxSlot == i + 1) {
            // recycle maxSlot
            for (; i >= 0 && this.slots[i] == null; --i) {
                --this.maxSlot;
            }
        }
        return true;
    }
    
    @SuppressWarnings("unchecked")
    public T deallocate(int i) {
        if (i >= this.maxSlot) {
            return null;
        }
        
        final Object o = this.slots[i];
        this.slots[i] = null;
        if (o != null) {
            --this.size;
        }
        if (this.maxSlot == i + 1) {
            // recycle maxSlot
            for (; i >= 0 && this.slots[i] == null; --i) {
                --this.maxSlot;
            }
        }
        return (T)o;
    }
    
    public void clear() {
        for (int i = 0; i < this.maxSlot; ++i) {
            this.slots[i] = null;
        }
        this.maxSlot = this.size = 0;
    }
    
    @SuppressWarnings("unchecked")
    public T get(int i) {
        return (T)this.slots[i];
    }
    
    public int size() {
        return this.size;
    }
    
    public int maxSlot() {
        return this.maxSlot;
    }
    
}
