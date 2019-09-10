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
package org.sqlite.server;

import org.sqlite.util.ConvertUtils;
import org.sqlite.util.MD5Utils;

/**SQLite server authentication method.
 * 
 * @author little-pan
 * @since 2019-09-03
 *
 */
public abstract class SQLiteAuthMethod {
    
    protected static final String ENCODING = "UTF-8";
    
    protected final String protocol;
    protected String user;
    protected String storePassword;
    
    public SQLiteAuthMethod(String protocol) {
        this.protocol = protocol;
    }
    
    public String getProtocol() {
        return this.protocol;
    }
    
    public String genStorePassword(String user, String password) {
        return ConvertUtils.bytesToHexString(MD5Utils.encode(password + user));
    }
    
    public void init(String user, String storePassword) {
        if (user == null) {
            throw new IllegalArgumentException("No user was provided");
        }
        
        if (storePassword == null) {
            throw new IllegalArgumentException("No storePassword was provided");
        }
        this.user = user;
        this.storePassword = storePassword;
    }
    
    public abstract String getName();
    
    @Override
    public abstract boolean equals(Object o);
    
}
