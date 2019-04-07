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
package org.sqlite.protocol;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.exception.NetworkException;
import org.sqlite.util.ConvertUtils;
import org.sqlite.util.IoUtils;

/**<p>
 * The SQLite client/server protocol codec.
 * </p>
 * 
 * @author little-pan
 * @since 2019-3-20
 *
 */
public class Transfer implements AutoCloseable {
    static final Logger log = LoggerFactory.getLogger(Transfer.class);
    
    public static final boolean TRACE = Boolean.getBoolean("sqlite.server.protocol.trace");
    
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
    private ByteBuffer buffer;
    
    private int protocolVersion = PROTOCOL_VERSION;
    private String encoding = ENCODING;

    public Transfer(final Map<String, Object> props) throws NetworkException {
        this.socket     = (Socket)props.get(PROP_SOCKET);
        this.initPacket = ConvertUtils.parseInt(PROP_INIT_PACKET, INIT_PACKET_LEN);
        this.maxPacket  = ConvertUtils.parseInt(PROP_MAX_PACKET, MAX_PACKET_LEN);
        this.buffer     = newBuffer(this.initPacket);
        try {
            this.in  = this.socket.getInputStream();
            this.out = this.socket.getOutputStream();
        }catch(IOException e){
            throw new NetworkException("Access socket stream error", e);
        }
    }
    
    /**
     * @param size
     * @return
     */
    protected static ByteBuffer newBuffer(int size) {
        final ByteBuffer b = ByteBuffer.allocate(size);
        b.order(ByteOrder.BIG_ENDIAN);
        return b;
    }
    
    public int position() {
        return this.buffer.position();
    }
    
    public Transfer position(int pos) {
        this.buffer.position(pos);
        return this;
    }
    
    public int limit(){
        return this.buffer.limit();
    }
    
    public Transfer limit(int lim){
        this.buffer.limit(lim);
        return this;
    }
    
    public int capacity(){
        return this.buffer.capacity();
    }
    
    /**<p>
     * Clear buffer for read or write.
     * </p>
     */
    public Transfer clear(){
        this.buffer.clear();
        return this;
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
    
    public void readyWrite(){
        if(position() > 0){
            return;
        }
        this.buffer.position(3);
    }

    /**
     * @param seq
     */
    public Transfer writeByte(int b) {
        readyWrite();
        
        ensureCapacity(position(), 1);
        this.buffer.put((byte)b);
        return this;
    }

    /**
     * @param serverVersion
     */
    public Transfer writeString(String s) {
        readyWrite();
        
        try {
            final byte a[] = s.getBytes(ENCODING);
            writeVarint(a.length);
            writeBytes(a);
            return this;
        } catch(UnsupportedEncodingException e) {
            throw new NetworkException("Decode string error", e);
        }
    }
    
    /**
     * @param a
     * @return this transfer
     */
    public Transfer writeBytes(byte[] a) {
        readyWrite();
        
        return writeBytes(a, 0, a.length);
    }
    
    /**
     * @param a
     * @return this transfer
     */
    public Transfer writeBytes(byte[] a, int offset, int len) {
        readyWrite();
        
        ensureCapacity(position(), len);
        this.buffer.put(a, offset, len);
        return this;
    }

    public final Transfer writeVarint(int i) {
        readyWrite();
        
        for(; (i & ~0x7F) != 0; ) {
            writeByte((i & 0x7f) | 0x80);
            i >>>= 7;
        }
        writeByte(i);
        return this;
    }

    /**
     * @param sessionId
     * @return this transfer
     */
    public Transfer writeInt(int i) {
        readyWrite();
        
        ensureCapacity(this.position(), 4);
        this.buffer.putInt(i);
        return this;
    }
    
    protected int writePacketLen(){
        final int len = position() - 4;
        if(len < 0){
            throw new NetworkException("Packet header not complete");
        }
        
        final ByteBuffer buf = this.buffer;
        buf.put(0, (byte)(len >> 16));
        buf.put(1, (byte)(len >> 8));
        buf.put(2, (byte)(len));
        return len;
    }
    
    /**<p>
     * Flush a packet into socket.
     * </p>
     */
    public void flush() {
        final int size = writePacketLen() + 4;
        try {
            final byte a[] = this.buffer.array();
            this.out.write(a, 0, size);
            this.out.flush();
            this.clear();
            trace("Flush buffer", a, 0, size);
        } catch(IOException e) {
            throw new NetworkException("Flush buffer error", e);
        }
    }
    
    /**
     * @param i
     * @param len
     */
    protected void ensureCapacity(int index, int len) {
        final int size = index + len;
        if(size > this.capacity()) {
            final int plen = size - 4;
            if(plen > this.maxPacket) {
                final String s = String.format("Exceed max packet limit %s, expect %s", 
                        this.maxPacket, plen);
                throw new NetworkException(s);
            }
            
            final ByteBuffer sli = this.buffer.slice();
            sli.flip();
            
            final int cap = Math.max(this.capacity() << 1, size);
            final ByteBuffer buf = newBuffer(Math.min(cap, this.maxPacket));
            this.buffer = buf.put(sli);
        }
    }
    
    /**
     * @param tag
     * @param a
     * @param offset
     * @param len
     */
    private static void trace(String tag, byte[] a, int offset, int len) {
        if(TRACE) {
            final StringBuilder sb = new StringBuilder(len << 1);
            IoUtils.dump(sb, a, offset, len);
            log.info("{}\n{}", tag, sb);
        }
    }
    
    public void readyRead(){
        if(position() > 0){
            return;
        }
        fill();
    }
    
    /**<p>
     * Read a packet into buffer.
     * </p>
     * @return this transfer
     */
    protected Transfer fill(){
        final int len = readPacketLen();
        readFully(1/*seq*/ + len);
        return this;
    }

    protected int readPacketLen() {
        readFully(3);
        final ByteBuffer buf = this.buffer;
        int 
        i  = (0xFF & buf.get()) << 16;
        i |= (0xFF & buf.get()) << 8;
        i |= (0xFF & buf.get());
        if(i > this.maxPacket) {
            throw new NetworkException(i+" exceeds max packet limit " + this.maxPacket);
        }
        return i;
    }
    
    protected void readFully(final int len) {
        try {
            final int offset = this.position();
            ensureCapacity(offset, len);
            final byte a[] = this.buffer.array();
            final InputStream in = this.in;
            
            final int size = offset + len;
            int i = offset;
            for(; i < size; ) {
                final int n = in.read(a, i, size-i);
                if(n == -1) {
                    throw new EOFException();
                }
                i += n;
            }
            this.limit(size);
            
            trace("Fill buffer", a, offset, len);
        } catch(IOException e) {
            throw new NetworkException("Socket read error", e);
        }
    }

    @Override
    public void close() {
        IoUtils.close(this.socket);
    }

    /**
     * @return the byte
     */
    public int readByte() {
        readyRead();
        
        return this.buffer.get() & 0xFF;
    }
    
    public int readVarint() {
        readyRead();
        
        int b = readByte();
        int i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            b = readByte();
            i |= (b & 0x7F) << shift;
        }
        return i;
    }

    /**
     * @return
     */
    public String readString() {
        readyRead();
        
        final int len = readVarint();
        try {
            final byte a[] = readBytes(len);
            return new String(a, 0, len, this.encoding);
        } catch (UnsupportedEncodingException e) {
            throw new NetworkException("Encode string error", e);
        }
    }

    /**
     * @return
     */
    public int readInt() {
        readyRead();
        
        return this.buffer.getInt();
    }

    /**
     * @param len
     * @return
     */
    public byte[] readBytes(int len) {
        readyRead();
        
        final byte a[] = new byte[len];
        this.buffer.get(a, 0, len);
        return a;
    }
    
}
