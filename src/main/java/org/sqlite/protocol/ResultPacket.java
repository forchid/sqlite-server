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

import org.sqlite.SQLiteErrorCode;

/**<p>
 * The server result packet: <br/><br/>
 * header <br/>
 * status - int 4 bytes <br/>
 * message- utf-8s <br/>
 * </p>
 * 
 * @author little-pan
 * @since 2019-03-25
 *
 */
public class ResultPacket extends Packet {
    
    private int status;
    private String message;
    
    public ResultPacket(){
        
    }
    
    public ResultPacket(SQLiteErrorCode code){
        this.status = code.code;
        this.message= code.toString();
    }
    
    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * @param t
     */
    @Override
    public void writeBody(Transfer t) {
        t.writeInt(this.status)
        .writeString(this.message);
    }
    
    @Override
    public void readBody(Transfer t){
        this.status = t.readInt();
        this.message= t.readString();
    }
    
    @Override
    public String toString(){
        return this.message;
    }
    
}
