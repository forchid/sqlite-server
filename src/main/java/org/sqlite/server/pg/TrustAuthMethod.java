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

import org.sqlite.server.SQLiteAuthMethod;

/**Trust authentication method.
 * 
 * @author little-pan
 * @since 2019-09-09
 *
 */
public class TrustAuthMethod extends SQLiteAuthMethod {
    
    public TrustAuthMethod(String protocol) {
        super(protocol);
    }
    
    @Override
    public void init(String user, String storePassword) {
        // NOOP
    }
    
    @Override
    public String genStorePassword(String user, String password) {
        return null;
    }
    
    public boolean authenticate(Object o) {
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public String getName() {
        return "trust";
    }
    
    @Override
    public String toString() {
        return getName();
    }
    
}
