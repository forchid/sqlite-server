/**
 * A SQLite server based on the C/S architecture, little-pan (c) 2019
 */
package org.sqlite.util;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

/**
 * @author little-pan
 * @since 2019-03-23
 *
 */
public final class SecurityUtils {
    
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

}
