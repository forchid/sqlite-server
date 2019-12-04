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

import java.util.Arrays;

import org.sqlite.server.SQLiteAuthMethod;
import org.sqlite.server.util.ConvertUtils;

/**Clear-text password authentication method.
 * 
 * @author little-pan
 * @since 2019-09-03
 *
 */
public class CleartextPassword extends SQLiteAuthMethod {
    
    public CleartextPassword(String protocol) {
        super(protocol);
    }
    
    public byte[] encode() {
        return ConvertUtils.hexBytes(this.storePassword);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        
        if (o instanceof String) {
            String password = genStorePassword(this.user, (String)o);
            return (this.storePassword.equals(password));
        }
        
        if (o instanceof CleartextPassword) {
            CleartextPassword pa = (CleartextPassword)o;
            return (this.storePassword.equals(pa.storePassword));
        }
        
        if (o instanceof byte[]) {
            return (Arrays.equals(encode(), (byte[])o));
        }
        
        return false;
    }
    
    @Override
    public int hashCode() {
        return (this.storePassword.hashCode());
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

