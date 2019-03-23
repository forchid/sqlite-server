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
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.protocol.Transfer;
import org.sqlite.util.ConvertUtils;
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
    static final Logger log = LoggerFactory.getLogger(SQLiteServer.class);
    
    public static final String VERSION = "SQLite Server-0.27.2";
    static final int PORT_DEFAULT      = 3272;
    static final int MAX_CONNS_DEFAULT = 151;
    
    private String dataDir = "./data";
    
    private String host    = "localhost";
    private int port       = PORT_DEFAULT;
    private int backlog    = 150;
    private int maxConns   = MAX_CONNS_DEFAULT;
    private int maxPacket  = Transfer.MAX_PACKET_LEN;
    private int initPacket = Transfer.INIT_PACKET_LEN;
    
    private final AtomicInteger sessionCount = new AtomicInteger(0);
    private int maxSessionId;
    
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
            if("--data-dir".equals(arg)   || "-D".equals(arg)) {
                this.dataDir = args[++i];
            }else if("--host".equals(arg) || "-H".equals(arg)) {
                this.host = args[++i];
            }else if("--port".equals(arg) || "-P".equals(arg)) {
                this.port = ConvertUtils.parseInt(args[++i]);
            }else if("--max-conns".equals(arg)) {
                this.maxConns = ConvertUtils.parseInt(args[++i]);
            }else if("--max-packet".equals(arg)) {
                this.maxPacket = ConvertUtils.parseInt(args[++i]);
            }else if("--help".equals(arg) || "-?".equals(arg)) {
                help(0);
            }else {
                help(1);
            }
        }
    }

    @Override
    public void run() {
        // bootstrap
        final File dir = new File(this.dataDir);
        if(!dir.exists() || !dir.isDirectory()) {
            log.error("The data dir not exists: {}", dir);
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
        if(serverSocket == null) {
            return;
        }
        
        // server loop
        try {
            log.info("Ready for connection on {}", address);
            for(;;) {
                try {
                    final Socket socket = serverSocket.accept();
                    handleSocket(socket);
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
    
    /**
     * Handle the client connection.
     * 
     * @param socket
     */
    protected void handleSocket(final Socket socket) {
        boolean failed = true;
        try {
            final int count = incrSessionCount();
            log.debug("A new connection {}", socket);
            if(count > this.maxConns){
                log.warn("Exceed max connections {}: close", this.maxConns);
                return;
            }
            
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            final int id = nextSessionId();
            final SQLiteSession session = new SQLiteSession(this, socket, id);
            session.start();
            failed = false;
        } catch (final Throwable e) {
            log.warn("Init connection error", e);
        } finally {
            if(failed) {
                decrSessionCount();
                IoUtils.close(socket);
            }
        }
    }
    
    protected int incrSessionCount(){
        return this.sessionCount.incrementAndGet();
    }
    
    protected int decrSessionCount(){
        return this.sessionCount.decrementAndGet();
    }
    
    private int nextSessionId(){
        int id = ++this.maxSessionId;
        if(id < 1){
            id = this.maxSessionId = 1;
        }
        return id;
    }
    
    public String getDataDir() {
        return this.dataDir;
    }

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public int getMaxConns() {
        return this.maxConns;
    }

    public int getMaxPacket() {
        return this.maxPacket;
    }
    
    public int getInitPacket() {
        return initPacket;
    }

    static void help(int status) {
        final PrintStream out = System.out;
        out.println("Usage: java org.sqlite.server.SQLiteServer [OPTIONS]\n"+
                "OPTIONS: \n"+
                "  --data-dir|-D  <path>       Server data directory, default data in work dir\n"+
                "  --host|-H      <host>       Server listen host or IP, default localhost\n"+
                "  --port|-P      <number>     Server listen port, default "+PORT_DEFAULT+"\n"+
                "  --max-conns    <number>     Max connections limit, default "+MAX_CONNS_DEFAULT+"\n"+
                "  --max-packet   <number>     Max packet length limit, default "+Transfer.MAX_PACKET_LEN+"\n" +
                "  --help|-?                   Show this message");
        System.exit(status);
    }

}
