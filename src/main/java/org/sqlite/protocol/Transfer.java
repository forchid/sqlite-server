/**
 * A SQLite server based on the C/S architecture, little-pan (c) 2019
 */
package org.sqlite.protocol;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.util.Map;

import org.sqlite.exception.NetworkException;
import org.sqlite.util.ConvertUtils;

/**<p>
 * The SQLite client/server protocol codec.
 * </p>
 * 
 * @author little-pan
 * @since 2019-3-20
 *
 */
public class Transfer {
    
    public static final String PROP_SOCKET = "socket";
    public static final String PROP_INIT_PACKET = "initPacket";
    public static final String PROP_MAX_PACKET  = "maxPacket";
    
    public static final int PROTOCOL_VERSION = 1;
    public static final String ENCODING = "UTF-8";
    
    public static final int INIT_PACKET_LEN = 1 << 12; // default 4 KB
    public static final int MAX_PACKET_LEN  = 1 << 20; // default 1 MB

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    
    private final int initPacket;
    private final int maxPacket;
    private byte buffer[];
    
    private int protocolVersion = PROTOCOL_VERSION;
    private String encoding = ENCODING;

    public Transfer(final Map<String, Object> props) throws NetworkException {
        this.socket     = (Socket)props.get(PROP_SOCKET);
        this.initPacket = ConvertUtils.parseInt(PROP_INIT_PACKET, INIT_PACKET_LEN);
        this.maxPacket  = ConvertUtils.parseInt(PROP_MAX_PACKET, MAX_PACKET_LEN);
        this.buffer     = new byte[this.initPacket];
        try {
            this.in  = this.socket.getInputStream();
            this.out = this.socket.getOutputStream();
        }catch(IOException e){
            throw new NetworkException("Access socket stream error", e);
        }
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

    /**
     * @param index
     * @param seq
     */
    public int writeByte(int index, int b) {
        ensureCapacity(index, 1);
        this.buffer[index] = (byte)b;
        return 1;
    }

    /**
     * @param i
     * @param len
     */
    private void ensureCapacity(int i, int len) {
        final int size = i + len;
        if(size > this.buffer.length) {
            if(size > this.maxPacket) {
                final String s = String.format("Exceed max packet limit %s, expect %s", 
                        this.maxPacket, size);
                throw new NetworkException(s);
            }
            final int cap = Math.max(this.buffer.length << 1, size);
            final byte buf[] = new byte[Math.min(cap, this.maxPacket)];
            System.arraycopy(this.buffer, 0, buf, 0, this.buffer.length);
            this.buffer = buf;
        }
    }

    /**
     * @param index
     * @param serverVersion
     */
    public int writeString(int index, String s) {
        try {
            int offset = index;
            final byte a[] = s.getBytes(ENCODING);
            offset += writeVarint(offset, a.length);
            offset += writeBytes(offset, a);
            return offset - index;
        } catch(UnsupportedEncodingException e) {
            throw new NetworkException("Decode string error", e);
        }
    }
    
    /**
     * @param index
     * @param a
     * @return writen
     */
    public int writeBytes(int index, byte[] a) {
        return writeBytes(index, a, 0, a.length);
    }
    
    /**
     * @param index
     * @param a
     * @return writen
     */
    public int writeBytes(int index, byte[] a, int offset, int len) {
        ensureCapacity(index, len);
        System.arraycopy(a, offset, this.buffer, index, len);
        return len;
    }

    public final int writeVarint(int index, int i) {
        int offset = index;
        for(; (i & ~0x7F) != 0; ) {
            writeByte(offset++, (i & 0x7f) | 0x80);
            i >>>= 7;
        }
        writeByte(offset++, i);
        return offset - index;
    }

    /**
     * @param index
     * @param sessionId
     * @return writen
     */
    public int writeInt(int index, int i) {
        final int len = 4;
        ensureCapacity(index, len);
        this.buffer[index++] = (byte)(i >> 24);
        this.buffer[index++] = (byte)(i >> 16);
        this.buffer[index++] = (byte)(i >> 8);
        this.buffer[index++] = (byte)(i);
        return len;
    }

    /**
     * @param p
     */
    public Transfer writePacketLen(int len) {
        this.buffer[0] = (byte)(len >> 16);
        this.buffer[1] = (byte)(len >> 8);
        this.buffer[2] = (byte)(len);
        return this;
    }
    
    public void flush(int len) {
        try {
            this.out.write(this.buffer, 0, len);
            this.out.flush();
        } catch(IOException e) {
            throw new NetworkException("Flush buffer error", e);
        }
    }
    
    public int readPacketLen() {
        readFully(this.buffer, 0, 3);
        int i = 0, index = 0;
        i |= (0xFF & this.buffer[index++]) << 16;
        i |= (0xFF & this.buffer[index++]) << 8;
        i |= (0xFF & this.buffer[index++]);
        return i;
    }
    
    public void readFully(byte a[], int offset, int len) {
        try {
            int i = 0;
            for(; i < len; ) {
                final int n = this.in.read(this.buffer, i, len-i);
                if(n == -1) {
                    throw new EOFException();
                }
                i += n;
            }
        } catch(IOException e) {
            throw new NetworkException("Socket read error", e);
        }
    }

}
