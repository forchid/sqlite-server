/**
 * A SQLite server based on the C/S architecture, little-pan (c) 2019
 */
package org.sqlite.util;

/**<p>
 * The common object or value convert utils.
 * </p>
 * 
 * @author little-pan
 * @since 2019-03-23
 *
 */
public final class ConvertUtils {
    
    private ConvertUtils() {}
    
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
    
    
}
