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
package org.sqlite.server.util;

import java.sql.SQLException;

import org.sqlite.SQLiteErrorCode;

/**<p>
 * The common object or value convert utils.
 * </p>
 * 
 * @author little-pan
 * @since 2019-03-23
 *
 */
public final class ConvertUtils {
    
    static final char[] HEXTBL =
        {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    
    private ConvertUtils() {}
    
    public static SQLException convertError(SQLiteErrorCode error) {
        return convertError(error, null, null);
    }
    
    public static  SQLException convertError(SQLiteErrorCode error, String message) {
        return convertError(error, message, null);
    }
    
    public static SQLException convertError(SQLiteErrorCode error, String message, String sqlState) {
        if (sqlState == null) {
            switch (error.code) {
            case 1:  // SQLITE_ERROR
                sqlState = "42000";
                break;
            case 2:  // SQLITE_INTERNAL
                sqlState = "58005";
                break;
            case 3:  // SQLITE_PERM
                sqlState = "42501";
                break;
            case 8:  // SQLITE_READONLY
                sqlState = "25000";
                break;
            case 10: // SQLITE_IOERR
                sqlState = "58030";
                break;
            case 19:  // SQLITE_CONSTRAINT
                sqlState = "23514";
                break;
            case 23: // SQLITE_AUTH
                sqlState = "28000";
                break;
            case 2067: // SQLITE_CONSTRAINT_UNIQUE
                sqlState = "23505";
                break;
            default:
                sqlState = "HY000";
                break;
            }
        } // if
        
        if (message == null) {
            message = error.message;
        }
        return new SQLException(message, sqlState, error.code);
    }
    
    public final static int parseInt(Object o, final int defaultValue) {
        try {
            if(o == null) {
                return defaultValue;
            }
            return parseInt(o);
        } catch(final Exception e) {
            return defaultValue;
        }
    }
    
    public final static int parseInt(Object o) {
        return Integer.parseInt(o + "");
    }
    
    public final static long parseLong(Object o, final long defaultValue) {
        try {
            if(o == null) {
                return defaultValue;
            }
            return parseLong(o);
        } catch(final Exception e) {
            return defaultValue;
        }
    }
    
    public final static long parseLong(Object o) {
        return Long.parseLong(o + "");
    }
    
    public final static boolean parseBoolean(Object o, boolean defaultValue) {
        if(o == null) {
            return defaultValue;
        }
        return parseBoolean(o);
    }
    
    public final static boolean parseBoolean(Object o) {
        return Boolean.getBoolean(o + "");
    }
    
    public static final String hexString(byte a[]){
        return bytesToHexString(a);
    }
    
    public static final byte[] hexBytes(String hex){
        final byte a[] = new byte[hex.length()>>1];
        for(int i = 0, size = a.length; i < size; ++i){
            final int j = i << 1;
            a[i] = (byte)Integer.parseInt(hex.substring(j, j + 2), 16);
        }
        return a;
    }
    
    public static void bytesToHex(byte[] bytes, byte[] hex, int offset) {
        int i, j, c, pos = offset;
        
        for (i = 0; i < 16; i++) {
            c = bytes[i] & 0xFF;
            j = c >> 4;
            hex[pos++] = (byte) HEXTBL[j];
            j = (c & 0xF);
            hex[pos++] = (byte) HEXTBL[j];
        }
    }
    
    public static String bytesToHexString(byte[] bytes) {
        return bytesToHex(bytes, 0, bytes.length);
    }
    
    public static String bytesToHex(byte[] bytes, int offset, int len) {
        if (bytes == null) {
            return null;
        }
        
        int i, j, c;
        
        StringBuilder sb = new StringBuilder(len << 1);
        for (i = offset; i < len; i++) {
            c = bytes[i] & 0xFF;
            j = c >> 4;
            sb.append(HEXTBL[j]);
            j = (c & 0xF);
            sb.append(HEXTBL[j]);
        }
        
        return sb.toString();
   }
    
}
