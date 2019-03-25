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

import java.io.IOException;

/**
 * @author little-pan
 * @since 2019-3-19
 *
 */
public final class IoUtils {
    
    static final String PROP_DUMP = "sqlite.server.io.dump";
    
    public static final boolean DUMP = Boolean.getBoolean(PROP_DUMP);
    /** line separator */
    public static final String LNSEP = System.getProperty("line.separator");
    
    private IoUtils() {}
    
    public static final void close(AutoCloseable closeable) {
        if(closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }
    
    public static final void dump(byte a[]) {
        dump(System.out, a);
    }
    
    public static final void dump(Appendable out, byte a[]) {
        dump(out, a, 0, a.length);
    }
    
    public static final void dump(byte a[], int offset, int len) {
        dump(System.out, a, offset, len);
    }
    
    public static final void dump(Appendable out, byte a[], int offset, int len) {
        if(DUMP) {
            final int lineLen = 16, size = offset + len;
            int i = offset, j = 0;
            try {
                for( ; i < size; ) {
                    out.append(String.format("%02X", a[i++]));
                    if((++j%lineLen) == 0 || i == size) {
                        if(i == size && j < lineLen){
                            // padding
                            for(int k = j; k < lineLen; ++k){
                                out.append("   ");
                            }
                        }
                        dumpAscii(out, a, i - j, j, "    ");
                        out.append(LNSEP);
                        if(i < size) {
                            j = 0;
                        }
                    }else {
                        out.append(' ');
                    }
                }
            } catch(IOException e) {
                throw new RuntimeException("Dump error", e);
            }
        }
    }
    
    public static final void dumpAscii(Appendable out, byte[] a) {
        dumpAscii(out, a, 0, a.length, null);
    }
    
    public static final void dumpAscii(Appendable out, byte[] a, int offset, int len) {
        dumpAscii(out, a, offset, len, null);
    }
    
    /**
     * @param out
     * @param a
     * @param offset
     * @param len
     * @param prefix
     */
    public static final void dumpAscii(Appendable out, byte[] a, int offset, int len, String prefix) {
        if(DUMP) {
            try {
                if(prefix != null) {
                    out.append(prefix);
                }
                for(int i = offset, size = offset + len; i < size; ++i) {
                    final byte b = a[i];
                    if(b > 31 && i < 127) {
                        out.append((char)b);
                    }else {
                        out.append('.');
                    }
                }
            } catch(IOException e) {
                throw new RuntimeException("Dump error", e);
            }
        }
    }

    public static void main(String args[]) {
        dump(System.out, new byte[] {1, 2, 3, 4, 5, 6, 7});
        dump(System.out, new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
        dump(System.out, new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17});
        dump(new byte[] {31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47});
    }

}
