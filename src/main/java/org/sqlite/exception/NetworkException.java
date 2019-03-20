/**
 * A SQLite server based on the C/S architecture, little-pan (c) 2019
 */
package org.sqlite.exception;

import java.io.IOException;

/**<p>
 * A runtime exception that represents network error.
 * </p>
 * 
 * @author little-pan
 * @since 2019-03-20
 *
 */
public class NetworkException extends RuntimeException {
    
    private static final long serialVersionUID = -2350753450442042479L;
    
    private IOException ioException;
    
    public NetworkException(String message, IOException cause){
        super(message, cause);
        this.ioException = cause;
    }
    
    public NetworkException(String message){
        super(message);
    }
    
    public NetworkException(IOException cause){
        super(cause);
        this.ioException = cause;
    }
    
    public IOException ioException(){
        return this.ioException;
    }

}
