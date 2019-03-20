/**
 * A SQLite server based on the C/S architecture, little-pan (c) 2019
 */
package org.sqlite.server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import org.sqlite.exception.NetworkException;

/**<p>
 * The SQLite server value codec.
 * </p>
 * 
 * @author little-pan
 * @since 2019-3-20
 *
 */
public class SQLiteTransfer {
    
    public static final int PROTOCOL_VERSION = 1;
    public static final String ENCODING = "UTF-8";
    
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    
    private int protocolVersion = PROTOCOL_VERSION;
    private String encoding = ENCODING;

    public SQLiteTransfer(Socket socket, int bufferSize) throws NetworkException {
        this.socket = socket;
        try {
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), bufferSize));
            this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), bufferSize));
        }catch(IOException e){
            throw new NetworkException("Access socket stream error", e);
        }
    }
    
    public SQLiteTransfer(Socket socket)throws NetworkException {
        this(socket, 1<<16);
    }
    
    public int getProtocolVersion() {
        return this.protocolVersion;
    }

    public void setProtocolVersion(int protocolVersion) {
        this.protocolVersion = protocolVersion;
    }
    
    public String getEncoding() {
        return this.encoding;
    }

}
