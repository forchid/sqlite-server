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
package org.sqlite.util;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;

/**
 * @author little-pan
 * @since 2019-03-23
 *
 */
public final class SecurityUtils {
    
    public static final int SEED_LEN = 20;
    
    private static SecureRandom secureRandom;
    
    private SecurityUtils() {}
    
    public static SecureRandom newSecureRandom() {
        final String os = System.getProperty("os.name").toLowerCase();
        if(os.indexOf("linux") >= 0) {
            try {
                return SecureRandom.getInstance("NativePRNG");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Get SecureRandom instance error", e);
            }
        }
        return new SecureRandom();
    }
    
    public static synchronized SecureRandom getSecureRandom() {
        if (secureRandom == null) {
            secureRandom = newSecureRandom();
        }
        
        return secureRandom;
    }
    
    public static byte[] genSeed(){
        final byte seed[] = new byte[SEED_LEN];
        getSecureRandom().nextBytes(seed);
        return seed;
    }

    /**
     * @param password
     * @param seed
     * @return the sign
     */
    public static byte[] sign(String password, byte[] seed) {
        final MessageDigest md = sha1();
        
        final byte p[] = toBytes(password);
        final byte hash1[] = md.digest(p);
        final byte hash2[] = md.digest(hash1);
        
        md.update(seed);
        md.update(hash2);
        final byte hash3[] = md.digest();
        
        for(int i = 0, size = hash1.length; i < size; ++i){
            hash1[i] ^= hash3[i];
        }
        
        return hash1;
    }
    
    public static MessageDigest sha1(){
        final MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-1");
            return md;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static byte[] toBytes(String s){
        try {
            return s.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static byte[] sha1(byte a[]){
        return sha1().digest(a);
    }
    
    public static byte[] sha1(String s){
        return sha1(toBytes(s));
    }
    
    public static String sha1hex(byte a[]){
        final byte b[] = sha1(a);
        return ConvertUtils.hexString(b);
    }
    
    public static String sha1hex(String s){
        final byte b[] = sha1(s);
        return ConvertUtils.hexString(b);
    }
    
    public static byte[] sha1dup(byte a[]){
        final MessageDigest md = sha1();
        a = md.digest(a);
        a = md.digest(a);
        return a;
    }
    
    public static byte[] sha1dup(String s){
        return sha1dup(toBytes(s));
    }
    
    public static String sha1duphex(byte a[]){
        final byte b[] = sha1dup(a);
        return ConvertUtils.hexString(b);
    }
    
    public static String sha1duphex(String s){
        final byte b[] = sha1dup(s);
        return ConvertUtils.hexString(b);
    }
    
    /**
     * @param sign
     * @param seed
     * @param encPassword
     */
    public static boolean signEquals(byte[] sign, byte[] seed, String encPassword) {
        final byte hash2[] = ConvertUtils.hexBytes(encPassword);
        
        final MessageDigest md = sha1();
        md.update(seed);
        md.update(hash2);
        final byte hash3[] = md.digest();
        
        final byte hash1[] = new byte[SEED_LEN];
        for(int i = 0, size = hash1.length; i < size; ++i){
            hash1[i] = (byte)(sign[i] ^ hash3[i]);
        }
        
        return (Arrays.equals(md.digest(hash1), hash2));
    }

    /**
     * Get a cryptographically secure pseudo random long value.
     *
     * @return the random long value
     */
    public static long secureRandomLong() {
        return getSecureRandom().nextLong();
    }
    
    public static String nextHexs(int n) {
        byte[] a = nextBytes(n);
        return (ConvertUtils.bytesToHexString(a));
    }
    
    public static byte[] nextBytes(int n) {
        byte[] a = new byte[n];
        getSecureRandom().nextBytes(a);
        return a;
    }
    
}
