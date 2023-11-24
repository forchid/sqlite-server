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

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.sqlite.server.SQLiteAuthMethod;
import org.sqlite.server.util.ConvertUtils;
import org.sqlite.server.util.SecurityUtils;

/**Encodes user/password/salt information in the following way: 
 * MD5(MD5(password + user) + salt).
 * 
 * @author little-pan
 * @since 2019-09-03
 *
 */
public class MD5Password extends SQLiteAuthMethod {
    
    private final byte[] salt;
    
    public MD5Password(String protocol) {
        this(protocol, SecurityUtils.nextBytes(4));
    }
    
    public MD5Password(String protocol, byte[] salt) {
        super(protocol);
        if (salt == null) {
            throw new IllegalArgumentException("No salt was provided");
        }
        this.salt = salt;
    }
    
    public byte[] getSalt() {
        return this.salt;
    }
    
    /**Encodes user/password/salt information in the following way: 
     * MD5(MD5(password + user) + salt).
     * 
     * @param user The connecting user
     * @param password The connecting user's password
     * @return A 35-byte array, comprising the string "md5" and an MD5 digest
     */
    public byte[] encode() {
        MessageDigest md;
        byte[] tempDigest;
        byte[] passDigest;
        byte[] hexDigest = new byte[35];

        try {
            tempDigest = this.storePassword.getBytes(ENCODING);
            System.arraycopy(tempDigest, 0, hexDigest, 0, tempDigest.length);
            
            md = MessageDigest.getInstance("MD5");
            md.update(hexDigest, 0, 32);
            md.update(this.salt);
            passDigest = md.digest();

            ConvertUtils.bytesToHex(passDigest, hexDigest, 3);
            hexDigest[0] = (byte) 'm';
            hexDigest[1] = (byte) 'd';
            hexDigest[2] = (byte) '5';
            
            return hexDigest;
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            throw new IllegalStateException("Unable to encode password with MD5", e);
        }
    }
    
    public boolean authenticate(Object o) {
        if (this == o) {
            return true;
        }
        
        if (o instanceof byte[]) {
            return (Arrays.equals(encode(), (byte[])o));
        }
        
        if (o instanceof MD5Password) {
            MD5Password md5 = (MD5Password)o;
            return (this.storePassword.equals(md5.storePassword)
                    && Arrays.equals(this.salt, md5.salt));
        }
        
        if (o instanceof String) {
            String md5s = (String)o;
            try {
                byte[] md5 = md5s.getBytes(ENCODING);
                return (Arrays.equals(encode(), md5));
            } catch (UnsupportedEncodingException e) {
                return false;
            }
        }
        
        return false;
    }
    
    @Override
    public int hashCode() {
        return (this.storePassword.hashCode() ^ this.salt.hashCode());
    }

    @Override
    public String getName() {
        return "md5";
    }
    
    @Override
    public String toString() {
        return getName();
    }
    
}

