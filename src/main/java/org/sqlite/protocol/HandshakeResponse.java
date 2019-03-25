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

import org.sqlite.util.SecurityUtils;

/**<p>
 * The client handshake response to server: <br/><br/>
 * header - 4 bytes(packet length 3 bytes + seq 1 byte) <br/>
 * protocol version - 1 byte <br/>
 * file name - utf-8s <br/>
 * open flags - int 4 bytes <br/>
 * user - utf-8s <br/>
 * login sign - 20 bytes <br/>
 * </p>
 * 
 * @author little-pan
 * @since 2019-03-25
 *
 */
public class HandshakeResponse extends Packet {
    
    private int protocolVersion;
    private String fileName;
    private int openFlags;
    private String user;
    private byte sign[];
    
    public HandshakeResponse(){
        
    }
    
    public int getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(int protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getOpenFlags() {
        return openFlags;
    }

    public void setOpenFlags(int openFlags) {
        this.openFlags = openFlags;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public byte[] getSign() {
        return sign;
    }

    public void setSign(byte[] sign) {
        this.sign = sign;
    }

    /**
     * @param t
     */
    @Override
    public void writeBody(Transfer t) {
        t.writeByte(this.protocolVersion)
        .writeString(this.fileName)
        .writeInt(this.openFlags)
        .writeString(this.user)
        .writeBytes(this.sign);
    }
    
    /**
     * @param t
     */
    @Override
    public void readBody(Transfer t){
        this.protocolVersion = t.readByte();
        this.fileName = t.readString();
        this.openFlags= t.readInt();
        this.user = t.readString();
        this.sign = t.readBytes(SecurityUtils.SEED_LEN);
    }

}
