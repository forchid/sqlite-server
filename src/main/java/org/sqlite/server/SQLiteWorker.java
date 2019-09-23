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

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.util.IoUtils;

/**SQLite server worker thread.
 * 
 * @author little-pan
 * @since 2019-09-14
 *
 */
public class SQLiteWorker implements Runnable {
    static final Logger log = LoggerFactory.getLogger(SQLiteWorker.class);
    
    protected static final int ioRatio;
    static {
        String prop = "org.sqlite.server.worker.ioRatio";
        int i = Integer.getInteger(prop, 50);
        if (i <= 0 || i > 100) {
            String message = prop + " " + i + ", expect (0, 100]";
            throw new ExceptionInInitializerError(message);
        }
        ioRatio = i;
    }
    
    protected final SQLiteServer server;
    
    protected final AtomicBoolean open = new AtomicBoolean();
    protected final AtomicBoolean wakeup = new AtomicBoolean();
    protected Selector selector;
    protected Thread runner;
    private volatile boolean stopped;
    
    protected final int id;
    protected final String name;
    
    protected final BlockingQueue<SQLiteProcessor> procQueue;
    protected final int maxConns;
    private int conns;
    protected final AtomicBoolean procsLock = new AtomicBoolean();
    private final Map<Integer, SQLiteProcessor> processors;
    private final PriorityQueue<SQLiteProcessor> busyQueue;
    
    public SQLiteWorker(SQLiteServer server, int id) {
        this.server = server;
        this.id = id;
        this.name = server.getName() + " worker-"+this.id;
        this.maxConns = server.getMaxConns();
        this.procQueue = new ArrayBlockingQueue<>(maxConns);
        this.processors= new HashMap<>();
        this.busyQueue = new PriorityQueue<>(this.maxConns, busyQueueCmp);
    }
    
    public int getId() {
        return this.id;
    }
    
    public String getName() {
        return this.name;
    }
    
    public boolean isOpen() {
        return this.open.get();
    }
    
    public void start() throws IOException {
        if (!this.open.compareAndSet(false, true)) {
            throw new IllegalStateException(this.name + " has been started");
        }
        if (isStopped()) {
            throw new IllegalStateException(this.name + " has been stopped"); 
        }
        
        boolean failed = true;
        try {
            this.selector = Selector.open();
            
            Thread runner = new Thread(this, this.name);
            runner.setDaemon(true);
            runner.start();
            this.runner = runner;
            failed = false;
        } finally {
            if (failed) {
                IoUtils.close(this.selector);
            }
        }
    }
    
    public boolean isStopped() {
        return this.stopped;
    }
    
    public void stop() {
        this.stopped = true;
        this.selector.wakeup();
    }
    
    protected void close() {
        if (this.runner != Thread.currentThread()) {
            throw new IllegalStateException("Not in " + this.name);
        }
        
        this.open.set(false);
        IoUtils.close(this.selector);
        
        for (;;) {
            SQLiteProcessor p = this.procQueue.poll();
            if (p == null) {
                break;
            }
            IoUtils.close(p);
        }
        
        Iterator<Map.Entry<Integer, SQLiteProcessor>> procs;
        procsLock();
        try {
            procs = this.processors.entrySet().iterator();
            for (; procs.hasNext();) {
                SQLiteProcessor p = procs.next().getValue();
                IoUtils.close(p);
                procs.remove();
            }
        } finally {
            procsUnlock();
        }
    }
    
    public void close(SQLiteProcessor processor) {
        if (processor == null) {
            return;
        }
        processor.stop();
        
        int id = processor.getId();
        procsLock();
        try {
            SQLiteProcessor p = this.processors.get(id);
            if (p == processor) {
                this.processors.remove(id);
                --this.conns;
            }
        } finally {
            procsUnlock();
        }
        
        processor.close();
    }

    @Override
    public void run() {
        try {
            for (; !isStopped() || this.processors.size() > 0;) {
                long timeout = minSelectTimeout();
                int n;
                if (timeout < 0L) {
                    n = this.selector.select();
                } else if (timeout == 0L) {
                    n = this.selector.selectNow();
                } else {
                    n = this.selector.select(timeout);
                }
                this.wakeup.set(false);
                if (0 == n) {
                    processQueues(0L);
                    continue;
                }
                
                if (100 == ioRatio) {
                    processIO();
                    processQueues(0L);
                    continue;
                }
                
                long ioStart = System.nanoTime();
                processIO();
                long ioTime = System.nanoTime() - ioStart;
                processQueues(ioTime * (100 - ioRatio) / ioRatio);
            }
        } catch (IOException e) {
            log.error("Fatal event", e);
        } finally {
            close();
        }
    }
    
    protected void processQueues(long runNanos) {
        BlockingQueue<SQLiteProcessor> queue = this.procQueue;
        Selector selector = this.selector;
        long deadNano = System.nanoTime() + runNanos;
        // Q1: procQueue
        for (;;) {
            if (runNanos > 0L && System.nanoTime() >= deadNano) {
                return;
            }
            
            SQLiteProcessor p = queue.poll();
            if (p == null) {
                break;
            }
            
            try {
                p.setSelector(selector);
                p.setWorker(this);
                if (this.conns >= this.maxConns) {
                    p.tooManyConns();
                    p.stop();
                    p.enableWrite();
                } else {
                    int id = p.getId();
                    try {
                        p.start();
                    } catch (IllegalStateException e) {
                        log.warn("Can't start " + p.getName(), e);
                        IoUtils.close(p);
                        continue;
                    }
                    procsLock();
                    try {
                        this.processors.put(id, p);
                    } finally {
                        procsUnlock();
                    }
                    ++this.conns;
                }
            } catch (IOException e) {
                log.debug("Handle processor error", e);
                IoUtils.close(p);
            }
        }
        
        // Q2: busyQueue
        PriorityQueue<SQLiteProcessor> busyQueue = this.busyQueue;
        for (;;) {
            if (runNanos > 0L && System.nanoTime() >= deadNano) {
                return;
            }
            SQLiteProcessor proc = busyQueue.peek();
            if (proc == null || !proc.getBusyContext().isReady()) {
                break;
            }
            this.server.trace(log, "Run busy processor: {}", proc);
            busyQueue.poll();
            proc.getBusyContext().run();
        }
    }
    
    protected void processIO() {
        Iterator<SelectionKey> keys = this.selector.selectedKeys().iterator();
        for (; keys.hasNext(); keys.remove()) {
            SelectionKey key = keys.next();
            SQLiteProcessor p;
            
            if (!key.isValid()) {
                continue;
            }
            
            if (key.isWritable()) {
                try {
                    p = (SQLiteProcessor)key.attachment();
                    Thread.currentThread().setName(this.name + "-" + p.getName());
                    p.write();
                } finally {
                    Thread.currentThread().setName(this.name);
                }
            } else if (key.isReadable()) {
                try {
                    p = (SQLiteProcessor)key.attachment();
                    Thread.currentThread().setName(this.name + "-" + p.getName());
                    p.read();
                } finally {
                    Thread.currentThread().setName(this.name);
                }
            } else {
                key.cancel();
            }
        }
    }

    public boolean offer(SQLiteProcessor process) {
        if (isStopped()) {
            return false;
        }
        
        boolean ok = this.procQueue.offer(process);
        if (ok && this.wakeup.compareAndSet(false, true)) {
            this.selector.wakeup();
        }
        return ok;
    }
    
    public boolean offerBusy(SQLiteProcessor process) {
        if (process.isStopped() || !process.isOpen()) {
            return false;
        }
        
        this.server.trace(log, "Offer busy processor: {}", process);
        return this.busyQueue.offer(process);
    }
    
    protected long minSelectTimeout() {
        SQLiteProcessor proc = this.busyQueue.peek();
        if (proc == null) {
            return -1L;
        }
        SQLiteBusyContext context = proc.getBusyContext();
        if (context.isReady()) {
            return 0L;
        }
        long timeout = context.getExecuteTime() - System.currentTimeMillis();
        return (timeout <= 0L? 0L: timeout);
    }

    SQLiteProcessor getProcessor(int pid) {
        procsLock();
        try {
            return this.processors.get(pid);
        } finally {
            procsUnlock();
        }
    }
    
    protected void procsLock() {
        for (; !this.procsLock.compareAndSet(false, true);) {
            // LOOP
        }
    }
    
    protected void procsUnlock() {
        this.procsLock.set(false);
    }
    
    static final Comparator<SQLiteProcessor> busyQueueCmp = new Comparator<SQLiteProcessor>() {
        
        @Override
        public int compare(SQLiteProcessor a, SQLiteProcessor b) {
            if (!a.isOpen()) {
                return -1;
            }
            if (!b.isOpen()) {
                return -1;
            }
            
            long ta = a.getBusyContext().getExecuteTime();
            long tb = b.getBusyContext().getExecuteTime();
            if (ta == tb) {
                return 0;
            }
            
            return (ta < tb ? -1: 1);
        }
        
    };
    
}
