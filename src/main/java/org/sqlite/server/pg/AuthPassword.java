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
package org.sqlite.server.pg;

import java.io.IOException;
import java.util.Arrays;

/**Simple authentication method based on password.
 * 
 * @author little-pan
 * @since 2019-09-03
 *
 */
public class AuthPassword implements AuthMethod {
    
    static final byte[] EMPTY_BYTEA = new byte[0];
    static final String ENCODING = "UTF-8";
    
    private final String password;
    
    public AuthPassword(String password) {
        this.password = password;
    }
    
    @Override
    public byte[] encode() {
        try {
            if (this.password == null) {
                return EMPTY_BYTEA;
            }
            
            return this.password.getBytes(ENCODING);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to encode password with "+ENCODING, e);
        }
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o || this.password == null) {
            return true;
        }
        
        if (o instanceof AuthPassword) {
            AuthPassword pa = (AuthPassword)o;
            return (this.password.equals(pa.password));
        }
        
        if (o instanceof byte[]) {
            return (Arrays.equals(encode(), (byte[])o));
        }
        
        return (this.password.equals(o));
    }
    
    @Override
    public int hashCode() {
        if (this.password == null) {
            return EMPTY_BYTEA.hashCode();
        }
        
        return (this.password.hashCode());
    }

    @Override
    public String getName() {
        return "password";
    }
    
    @Override
    public String toString() {
        return getName();
    }
    
}

