/**
 * A SQLite server based on the C/S architecture, little-pan (c) 2019
 */
package org.sqlite.server;

import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.exception.NetworkException;
import org.sqlite.util.IoUtils;

/**<p>
 * The SQLite server connection session.
 * </p>
 * 
 * @author little-pan
 * @since 2019-03-20
 *
 */
public class SQLiteSession implements Runnable {
    
    static final Logger log = LoggerFactory.getLogger(SQLiteSession.class);
    
    protected final SQLiteServer server;
    protected final Socket socket;
    protected final int id;
    protected final String name;
    
    private volatile Thread thread;
    
    public SQLiteSession(SQLiteServer server, Socket socket, int id){
        this.server = server;
        this.socket = socket;
        this.id = id;
        this.name = String.format("session-%s", this.id);
    }


    @Override
    public void run() {
        final Socket socket = this.socket;
        try{
            log.debug("init");
            // 0. init session
            final SQLiteTransfer transfer = new SQLiteTransfer(socket);
            
            // 1. connection phase
            if(handleConnection(transfer)){
             // 2. command phase
                handleCommand(transfer);
            }
            log.debug("exit");
        }catch(final NetworkException e){
            log.debug("Network error", e);
        }finally{
            IoUtils.close(socket);
            this.server.decrSessionCount();
            this.thread = null;
        }
    }
    
    /**
     * @param transfer
     */
    protected void handleCommand(SQLiteTransfer transfer) {
        
    }


    /**
     * @param transfer
     */
    protected boolean handleConnection(SQLiteTransfer transfer) {
        // Handshake request packet format:
        // header 4 bytes(packet length 3 bytes + seq 1 byte)
        // protocol version 1 byte
        // database name utf-8 string
        
        return false;
    }

    public void start(){
        final Thread t = new Thread(this, name());
        t.setPriority(Thread.NORM_PRIORITY);
        t.start();
        this.thread = t;
    }
    
    public int id(){
        return this.id;
    }
    
    public String name(){
        return this.name;
    }
    
    public SQLiteServer server(){
        return this.server;
    }
    
    public void interrupt(){
        final Thread t = this.thread;
        if(t != null){
            t.interrupt();
        }
    }

}
