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

/**<p>
 * The SQLite client command packet.
 * </p>
 * 
 * @author little-pan
 * @since 2019-03-26
 *
 */
public abstract class Command extends Packet {
    
    public static final int COM_QUIT       = 0x01;
    
    public static final int COM_PREPARE    = 0x02;
    public static final int COM_BIND_COUNT = 0x03;
    public static final int COM_BIND_VALUE = 0x04;
    public static final int COM_STEP       = 0x05;
    public static final int COM_RESET      = 0x06;
    public static final int COM_FINALIZE   = 0x07;
    
    protected int opcode;
    
    protected Command(final int opcode){
        this.opcode = opcode;
    }

    @Override
    protected void readBody(Transfer t) {
        this.opcode = t.readByte();
    }

    @Override
    protected void writeBody(Transfer t) {
        t.writeByte(this.opcode);
    }

}
