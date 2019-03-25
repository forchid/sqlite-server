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
package org.sqlite.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.BusyHandler;
import org.sqlite.Function;
import org.sqlite.ProgressHandler;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.core.DB;
import org.sqlite.protocol.HandshakeInit;
import org.sqlite.protocol.HandshakeResponse;
import org.sqlite.protocol.ResultPacket;
import org.sqlite.protocol.Transfer;
import org.sqlite.util.ConvertUtils;
import org.sqlite.util.IoUtils;
import org.sqlite.util.SecurityUtils;

/**<p>
 * The remote DB implementation.
 * </p>
 * 
 * @author little-pan
 * @since 2019-03-24
 *
 */
public class RemoteDB extends DB {
    static final Logger log = LoggerFactory.getLogger(RemoteDB.class);
    
    protected String host = "localhost";
    protected int port = 3272;
    protected String fileName;
    
    private String user = "";
    private String password = "";
    
    private int openFlags;
    
    protected Transfer transfer;

    /**
     * @param url
     * @param fileName
     * @param config
     * @throws SQLException
     */
    public RemoteDB(String url, String fileName, SQLiteConfig config) 
            throws SQLException {
        super(url, fileName, config);
    }

    @Override
    protected void _close() throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public int _exec(String sql) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    protected void _open(String fileName, int openFlags) throws SQLException {
        // parse:  host, port, dbName
        int i = fileName.indexOf('/');
        if(i == -1) {
            throw new SQLException("DB address malformed: " + fileName);
        }
        if(i == 0) {
            this.fileName = fileName.substring(i+1);
        }else {
            this.fileName = fileName.substring(i + 1);
            final String addr = fileName.substring(0, i);
            final String sa[] = addr.split(":");
            this.host = sa[0];
            if(sa.length > 1) {
                this.port = ConvertUtils.parseInt(sa[1]);
            }
        }
        
        // connect
        this.transfer = connect();
        
    }

    /**
     * @return
     */
    protected Transfer connect() throws SQLException {
        final Transfer t = doConnect();
        
        boolean failed = true;
        try {
            doHandshake(t);
            failed = false;
            return t;
        } finally {
            if(failed) {
                IoUtils.close(t);
            }
        }
    }
    
    /**
     * @param t
     */
    protected Transfer doHandshake(Transfer t) throws SQLException {
        final HandshakeInit init = new HandshakeInit();
        init.read(t);
        log.debug("Server: {}", init.getServerVersion());
        
        final byte sign[] = SecurityUtils.sign(this.password, init.getSeed());
        final HandshakeResponse response = new HandshakeResponse();
        response.setSeq(init.getSeq() + 1);
        response.setProtocolVersion(Transfer.PROTOCOL_VERSION);
        response.setFileName(this.fileName);
        response.setOpenFlags(this.openFlags);
        response.setUser(this.user);
        response.setSign(sign);
        response.write(t);
        
        // check result
        final ResultPacket result = new ResultPacket();
        result.read(t);
        result.checkSeq(response.getSeq() + 1);
        if(result.getStatus() != SQLiteErrorCode.SQLITE_OK.code){
            throw new SQLException(result.getMessage(), null, result.getStatus());
        }
        
        log.debug("Result: {}", result);
        return t;
    }

    protected Transfer doConnect() throws SQLException {
        final Socket socket = new Socket();
        
        boolean failed = true;
        try {
            socket.connect(new InetSocketAddress(this.host, this.port));
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            
            final Map<String, Object> props = new HashMap<>();
            props.put(Transfer.PROP_SOCKET, socket);
            final Transfer t = new Transfer(props);
            failed = false;
            return t;
        } catch(IOException e) {
            throw new SQLException("Connection error", "08001", e);
        } finally {
            if(failed) {
                IoUtils.close(socket);
            }
        }
    }

    @Override
    public int backup(String dbName, String destFileName, ProgressObserver observer) 
            throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    int bind_blob(long stmt, int pos, byte[] v) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    int bind_double(long stmt, int pos, double v) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    int bind_int(long stmt, int pos, int v) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    int bind_long(long stmt, int pos, long v) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    int bind_null(long stmt, int pos) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    int bind_parameter_count(long stmt) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    int bind_text(long stmt, int pos, String v) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void busy_handler(BusyHandler busyHandler) throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void busy_timeout(int ms) throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public int changes() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int clear_bindings(long stmt) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void clear_progress_handler() throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public byte[] column_blob(long stmt, int col) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int column_count(long stmt) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String column_decltype(long stmt, int col) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public double column_double(long stmt, int col) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int column_int(long stmt, int col) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long column_long(long stmt, int col) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    boolean[][] column_metadata(long stmt) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String column_name(long stmt, int col) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String column_table_name(long stmt, int col) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String column_text(long stmt, int col) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int column_type(long stmt, int col) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int create_function(String name, Function f, int flags) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int destroy_function(String name) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int enable_load_extension(boolean enable) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    String errmsg() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected int finalize(long stmt) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    void free_functions() throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void interrupt() throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public String libversion() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected long prepare(String sql) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void register_progress_handler(int vmCalls, ProgressHandler progressHandler)
            throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public int reset(long stmt) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int restore(String dbName, String sourceFileName, ProgressObserver observer) 
            throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void result_blob(long context, byte[] val) throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void result_double(long context, double val) throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void result_error(long context, String val) throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void result_int(long context, int val) throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void result_long(long context, long val) throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void result_null(long context) throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void result_text(long context, String val) throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public int shared_cache(boolean enable) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int step(long stmt) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int total_changes() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public byte[] value_blob(Function f, int arg) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public double value_double(Function f, int arg) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int value_int(Function f, int arg) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long value_long(Function f, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String value_text(Function f, int arg) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int value_type(Function f, int arg) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * @param user
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * @param password
     */
    public void setPassword(String password) {
        this.password = password;
    }

}
