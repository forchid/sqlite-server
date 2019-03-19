/**
 * A SQLite server based on the C/S architecture, little-pan (c) 2019
 */
package org.sqlite.util;

/**
 * @author little-pan
 * @since 2019-3-19
 *
 */
public final class IoUtils {
    
    private IoUtils() {}
    
    public final static void close(AutoCloseable closeable) {
        if(closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

}
