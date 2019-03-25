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

import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.exception.NetworkException;
import org.sqlite.protocol.HandshakeInit;
import org.sqlite.protocol.HandshakeResponse;
import org.sqlite.protocol.ResultPacket;
import org.sqlite.protocol.Transfer;
import org.sqlite.util.IoUtils;
import org.sqlite.util.SecurityUtils;

import static org.sqlite.util.SecurityUtils.*;

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
            final Map<String, Object> props = new HashMap<>();
            props.put(Transfer.PROP_SOCKET, socket);
            props.put(Transfer.PROP_INIT_PACKET, server.getInitPacket());
            props.put(Transfer.PROP_MAX_PACKET, server.getMaxPacket());
            final Transfer transfer = new Transfer(props);
            
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
     * @param t
     */
    protected void handleCommand(Transfer t) {
        log.debug("Handle command");
        
    }


    /**
     * @param t
     */
    protected boolean handleConnection(Transfer t) {
        log.debug("Handle connection");
        
        final HandshakeInit hsInit = new HandshakeInit();
        hsInit.setSeq(0);
        hsInit.setProtocolVersion(Transfer.PROTOCOL_VERSION);
        hsInit.setServerVersion(SQLiteServer.VERSION);
        hsInit.setSessionId(this.id);
        hsInit.setSeed(SecurityUtils.genSeed());
        hsInit.write(t);
        
        final HandshakeResponse response = new HandshakeResponse();
        response.read(t);
        response.checkSeq(hsInit.getSeq() + 1);
        
        // - Test
        final String user = "root";
        final String encPassword = "6bb4837eb74329105ee4568dda7dc67ed2ca2ad9";
        // 
        
        final ResultPacket result;
        if(user.equals(response.getUser()) && 
                signEquals(response.getSign(), hsInit.getSeed(), encPassword)){
            result = new ResultPacket(SQLiteErrorCode.SQLITE_OK);
            result.setSeq(response.getSeq() + 1);
            result.write(t);
            return true;
        }
        
        result = new ResultPacket(SQLiteErrorCode.SQLITE_PERM);
        result.setSeq(response.getSeq() + 1);
        result.write(t);
        return false;
    }

    public void start(){
        final Thread t = new Thread(this, getName());
        t.setPriority(Thread.NORM_PRIORITY);
        t.start();
        this.thread = t;
    }
    
    public int getId(){
        return this.id;
    }
    
    public String getName(){
        return this.name;
    }
    
    public SQLiteServer getServer(){
        return this.server;
    }
    
    public Socket getSocket(){
        return this.socket;
    }
    
    public void interrupt(){
        final Thread t = this.thread;
        if(t != null){
            t.interrupt();
        }
    }

}
