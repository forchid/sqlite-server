/**
 * A SQLite server based on the C/S architecture, little-pan (c) 2019
 */
package org.sqlite.server;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.util.IoUtils;

/**<p>
 * The SQLite server based on the C/S architecture.
 * </p>
 * 
 * @author little-pan
 * @since 2019-03-19
 *
 */
public class SQLiteServer implements Runnable {
    final static Logger log = LoggerFactory.getLogger(SQLiteServer.class);
    
    private String datadir = "./data";
    
    private String host = "localhost";
    private int port = 3272;
    private int backlog = 150;
    
    public static void main(String args[]) {
        final SQLiteServer server = new SQLiteServer();
        server.parse(args);
        server.run();
    }
    
    /**
     * @param args
     */
    protected void parse(String[] args) {
        for(int i = 0, size = args.length; i < size; ++i) {
            final String arg = args[i];
            if("-datadir".equals(arg)) {
                this.datadir = args[++i];
            }else if("-host".equals(arg)) {
                this.host = args[++i];
            }else if("-port".equals(arg)) {
                this.port = Integer.parseInt(args[++i]);
            }else if("-help".equals(arg) || "-?".equals(arg)) {
                help(0);
            }else {
                help(1);
            }
        }
    }

    @Override
    public void run() {
        // bootstrap
        final File dir = new File(this.datadir);
        if(!dir.exists() || !dir.isDirectory()) {
            log.error("The datadir not exists: {}", dir);
            return;
        }
        
        final SocketAddress address = new InetSocketAddress(this.host, this.port);
        ServerSocket serverSocket = null;
        boolean failed = true;
        try {
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(address, this.backlog);
            failed = false;
        } catch(final IOException e) {
            log.error("Bind error", e);
        } finally {
            if(failed) {
                IoUtils.close(serverSocket);
                serverSocket = null;
            }
        }
        
        // server loop
        if(serverSocket != null) {
            try {
                log.info("Ready for connection on {}", address);
                for(;;) {
                    try {
                        final Socket socket = serverSocket.accept();
                        failed = true;
                        try {
                            handleSocket(socket);
                            failed = false;
                        }finally {
                            if(failed) {
                                IoUtils.close(socket);
                            }
                        }
                    } catch (final IOException e) {
                        log.error("Accept connection error", e);
                        break;
                    } catch (final Throwable e) {
                        log.warn("Server loop error", e);
                    }
                }
            } finally {
                IoUtils.close(serverSocket);
            }
        }
    }
    
    /**
     * Handle the client connection.
     * 
     * @param socket
     */
    protected void handleSocket(final Socket socket) {
        log.debug("A new connection {}", socket);
        
        boolean failed = true;
        try {
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            
            //failed = false;
        } catch (SocketException e) {
            log.warn("Init connection error", e);
        } finally {
            if(failed) {
                IoUtils.close(socket);
            }
        }
    }

    static void help(int status) {
        final PrintStream out = System.out;
        out.println("Usage: java org.sqlite.server.SQLiteServer [OPTIONS]\n"+
                "OPTIONS: \n"+
                "  -datadir <DATADIR> The server data directory, default the user home\n"+
                "  -host    <HOST>    The server listen host or IP, default localhost\n"+
                "  -port    <PORT>    The server listen port, default 3272\n"+
                "  -help|-? Show this message");
        System.exit(status);
    }

}
