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
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.sqlite.server.util.ConvertUtils;
import org.sqlite.server.util.SecurityUtils;

/**Encodes user/password/salt information in the following way: 
 * MD5(MD5(password + user) + salt).
 * 
 * @author little-pan
 * @since 2019-09-03
 *
 */
public class MD5Password implements AuthMethod {
    
    static final String ENCODING = "UTF-8";
    
    private final byte[] salt;
    private final String user;
    private final String password;
    
    public MD5Password(String user, String password) {
        this(user, password, new byte[4]);
        initSalt();
    }
    
    public MD5Password(String user, String password, byte[] salt) {
        this.salt = salt;
        this.user = user;
        this.password = password;
        if (password == null) {
            throw new IllegalArgumentException("No password was provided");
        }
    }
    
    private void initSalt() {
        SecurityUtils.getSecureRandom().nextBytes(this.salt);
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
    @Override
    public byte[] encode() {
        MessageDigest md;
        byte[] tempDigest;
        byte[] passDigest;
        byte[] hexDigest = new byte[35];

        try {
            md = MessageDigest.getInstance("MD5");

            md.update(this.password.getBytes(ENCODING));
            md.update(this.user.getBytes(ENCODING));
            tempDigest = md.digest();

            ConvertUtils.bytesToHex(tempDigest, hexDigest, 0);
            md.update(hexDigest, 0, 32);
            md.update(this.salt);
            passDigest = md.digest();

            ConvertUtils.bytesToHex(passDigest, hexDigest, 3);
            hexDigest[0] = (byte) 'm';
            hexDigest[1] = (byte) 'd';
            hexDigest[2] = (byte) '5';
            
            return hexDigest;
        } catch (NoSuchAlgorithmException | IOException e) {
            throw new IllegalStateException("Unable to encode password with MD5", e);
        }
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        
        if (o instanceof byte[]) {
            return (Arrays.equals(encode(), (byte[])o));
        }
        
        if (o instanceof MD5Password) {
            MD5Password md5 = (MD5Password)o;
            return (this.password.equals(md5.password) 
                    && this.user.equals(md5.user) 
                    && Arrays.equals(this.salt, md5.salt));
        }
        
        if (o instanceof String) {
            String md5s = (String)o;
            try {
                byte[] md5 = md5s.getBytes(ENCODING);
                return (equals(md5));
            } catch (UnsupportedEncodingException e) {
                return false;
            }
        }
        
        return false;
    }
    
    @Override
    public int hashCode() {
        return (this.user.hashCode() ^ this.password.hashCode() ^ this.salt.hashCode());
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

