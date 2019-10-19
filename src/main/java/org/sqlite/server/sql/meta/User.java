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
package org.sqlite.server.sql.meta;


/**
 * @author little-pan
 * @since 2019-09-08
 *
 */
public class User {
    
    private String user;
    private String password;
    private String host;
    private String db;
    private String protocol;
    private String authMethod;
    private int sa;
    
    public User() {
        
    }
    
    public User(String user, String password) {
        this(user, password, 0);
    }
    
    public User(String user, String password, int sa) {
        this.user = user;
        this.password = password;
        this.sa = sa;
    }
    
    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getAuthMethod() {
        return authMethod;
    }

    public void setAuthMethod(String authMethod) {
        this.authMethod = authMethod;
    }

    public int getSa() {
        return sa;
    }

    public void setSa(int sa) {
        this.sa = sa;
    }
    
    public boolean isSa() {
        return (this.sa == 1);
    }
    
    @Override
    public int hashCode() {
        return (this.protocol.hashCode() ^ this.user.hashCode() ^ this.host.hashCode());
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof User) {
            final User u = (User)o;
            return (u.protocol.equals(this.protocol)
                    && u.user.equals(this.user) && u.host.equals(this.host));
        }
        
        return false;
    }

    public static int convertSa(boolean sa) {
        return (sa? 1: 0);
    }
    
    public static User createSuperuser(String user, String password) {
        return new User(user, password, 1);
    }
    
}
