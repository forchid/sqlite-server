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

import org.sqlite.exception.NetworkException;

/**<p>
 * The protocol packet.
 * </p>
 * 
 * @author little-pan
 * @since 2019-03-25
 *
 */
public abstract class Packet {
    
    protected int seq;
    
    protected Packet(){
        
    }
    
    public int getSeq() {
        return seq;
    }

    public void setSeq(int seq) {
        this.seq = seq;
    }
    
    /**
     * @param expected
     */
    public void checkSeq(int expected) {
        if(this.seq != expected){
            final String s = 
                    String.format("Packet seq %s error, expect %s", 
                            this.seq, expected);
            throw new NetworkException(s);
        }
    }
    
    public void write(Transfer t){
        t.writeByte(this.seq);
        writeBody(t);
        t.flush();
    }
    
    public Packet read(Transfer t){
        this.seq = t.readByte();
        readBody(t);
        t.clear();
        return this;
    }

    /**
     * @param t
     */
    protected abstract void readBody(Transfer t);

    /**
     * @param t
     */
    protected abstract void writeBody(Transfer t);

}
