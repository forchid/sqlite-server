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
package org.sqlite.server.pg;

import static java.lang.String.format;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConnection;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.core.CoreResultSet;
import org.sqlite.server.MetaStatement;
import org.sqlite.server.NetworkException;
import org.sqlite.server.SQLiteProcessorTask;
import org.sqlite.server.SQLiteProcessor;
import org.sqlite.server.SQLiteQueryTask;
import org.sqlite.server.SQLiteWorker;
import org.sqlite.server.sql.meta.User;
import org.sqlite.sql.ImplicitCommitException;
import org.sqlite.sql.SQLParseException;
import org.sqlite.sql.SQLParser;
import org.sqlite.sql.SQLStatement;

import static org.sqlite.util.ConvertUtils.*;

import org.sqlite.util.DateTimeUtils;
import org.sqlite.util.IoUtils;
import org.sqlite.util.SecurityUtils;
import org.sqlite.util.StringUtils;

/**The PG protocol handler.
 * 
 * @author little-pan
 * @since 2019-09-01
 *
 */
public class PgProcessor extends SQLiteProcessor {
    
    static final Logger log = LoggerFactory.getLogger(PgProcessor.class);
    private static final boolean INTEGER_DATE_TYPES = false;
    
    // auth method
    private static final int AUTH_REQ_OK = 0;
    private static final int AUTH_REQ_PASSWORD = 3;
    private static final int AUTH_REQ_MD5 = 5;
    
    protected static final String UNNAMED  = "";
    
    private final int secret;
    private DataInputStream dataBuf;
    private int x, inSize = -1;
    
    private int messageType;
    private ByteArrayOutputStream outBuf;
    private DataOutputStream dataOut;
    private boolean needFlush;
    
    private String userName;
    private String clientEncoding = "UTF-8";
    private String dateStyle = "ISO, MDY";
    
    private boolean initDone, xQueryFailed;
    private final HashMap<String, Prepared> prepared = new HashMap<>();
    private final HashMap<String, Portal> portals = new HashMap<>();
    private Portal portal;

    protected PgProcessor(PgServer server, SocketChannel channel, int id) throws NetworkException {
        super(server, channel, id);
        this.secret = (int)SecurityUtils.secureRandomLong();
    }
    
    public PgServer getServer() {
        return (PgServer)this.server;
    }
    
    @Override
    protected void deny(InetSocketAddress remote) throws IOException {
        String message = format("Host '%s' not allowed", remote.getHostName());
        SQLException error = convertError(SQLiteErrorCode.SQLITE_PERM, message);
        sendErrorResponse(error);
    }
    
    @Override
    protected void interalError() throws IOException {
        String message = "Internal error in SQLite server";
        SQLException error = convertError(SQLiteErrorCode.SQLITE_INTERNAL, message);
        sendErrorResponse(error);
    }
    
    @Override
    protected void tooManyConns() throws IOException {
        String message = "Too many connections";
        SQLException error = convertError(SQLiteErrorCode.SQLITE_PERM, message, "08004");
        sendErrorResponse(error);
    }
    
    private void setParameter(PreparedStatement prep,
            int pgType, int i, int[] formatCodes) throws SQLException, IOException {
        boolean text = (i >= formatCodes.length) || (formatCodes[i] == 0);
        int col = i + 1;
        int paramLen = readInt();
        if (paramLen == -1) {
            prep.setNull(col, Types.NULL);
        } else if (text) {
            // plain text
            byte[] data = new byte[paramLen];
            readFully(data);
            String str = new String(data, getEncoding());
            switch (pgType) {
            case PgServer.PG_TYPE_DATE: {
                // Strip timezone offset
                int idx = str.indexOf(' ');
                if (idx > 0) {
                    str = str.substring(0, idx);
                }
                break;
            }
            case PgServer.PG_TYPE_TIME: {
                // Strip timezone offset
                int idx = str.indexOf('+');
                if (idx <= 0) {
                    idx = str.indexOf('-');
                }
                if (idx > 0) {
                    str = str.substring(0, idx);
                }
                break;
            }
            }
            prep.setString(col, str);
        } else {
            // binary
            switch (pgType) {
            case PgServer.PG_TYPE_INT2:
                checkParamLength(2, paramLen);
                prep.setShort(col, readShort());
                break;
            case PgServer.PG_TYPE_INT4:
                checkParamLength(4, paramLen);
                prep.setInt(col, readInt());
                break;
            case PgServer.PG_TYPE_INT8:
                checkParamLength(8, paramLen);
                prep.setLong(col, dataBuf.readLong());
                break;
            case PgServer.PG_TYPE_FLOAT4:
                checkParamLength(4, paramLen);
                prep.setFloat(col, dataBuf.readFloat());
                break;
            case PgServer.PG_TYPE_FLOAT8:
                checkParamLength(8, paramLen);
                prep.setDouble(col, dataBuf.readDouble());
                break;
            case PgServer.PG_TYPE_BYTEA:
                byte[] d1 = new byte[paramLen];
                readFully(d1);
                prep.setBytes(col, d1);
                break;
            default:
                server.trace(log, "Binary format for type: {} is unsupported", pgType);
                byte[] d2 = new byte[paramLen];
                readFully(d2);
                prep.setString(col, new String(d2, getEncoding()));
            }
        }
    }
    
    private static void checkParamLength(int expected, int got) throws IOException {
        if (expected != got) {
            throw new IOException(format("paramLen %d(expect %d)", got, expected));
        }
    }
    
    @Override
    protected void process() throws IOException {
        PgServer server = getServer();
        SQLiteWorker worker = this.worker;
        SocketChannel ch = getChannel();
        
        int rem = 0;
        do {
            ByteBuffer inBuf = getReadBuffer(5);
            int n = 0;
            
            if (this.inSize == -1) {
                // 1. read header
                if (!this.initDone && inBuf.position()==0) {
                    inBuf.put((byte)0);
                }
                n = ch.read(inBuf);
                if (n < 0) {
                    stop();
                    enableWrite();
                    return;
                }
                if (inBuf.position() < 5) {
                    return;
                }
                x = inBuf.get(0) & 0xFF;
                
                this.inSize = (inBuf.get(1) & 0xFF) << 24
                        | (inBuf.get(2) & 0xFF) << 16
                        | (inBuf.get(3) & 0xFF) << 8
                        | (inBuf.get(4) & 0xFF) << 0;
                this.inSize -= 4;
                server.trace(log, ">> message: type '{}'(c) {}, len {}", (char)x, x, this.inSize);
            }
            
            // 2. read body
            int buffered = inBuf.position() - 5;
            if (buffered < inSize) {
                inBuf = getReadBuffer(inSize - buffered);
                n = ch.read(inBuf);
                if (n < 0) {
                    stop();
                    enableWrite();
                    return;
                }
                buffered = inBuf.position() - 5;
                if (buffered < inSize) {
                    return;
                }
            }
            
            // process: read OK
            byte[] data = inBuf.array();
            // mark read state
            inBuf.flip();
            inBuf.position(5 + inSize);
            if (this.xQueryFailed && 'S' != x) {
                server.trace(log, "Discard any message for xQuery error detected until Sync");
                this.dataBuf = null;
                this.inSize = -1;
                rem = resetReadBuffer();
                continue;
            }
            this.dataBuf = new DataInputStream(new ByteArrayInputStream(data, 5, inSize));
            
            this.needFlush = false;
            switch (x) {
            case 0:
                server.trace(log, "Init");
                int version = readInt();
                if (version == 80877102) {
                    server.trace(log, "CancelRequest");
                    int pid = readInt();
                    int key = readInt();
                    PgProcessor processor = (PgProcessor)server.getProcessor(pid);
                    if (processor != null && processor.secret == key) {
                        try {
                            processor.cancelRequest();
                        } catch (SQLException e) {
                            server.traceError(log, "can't cancel request", e);
                        }
                    } else {
                        // According to the PostgreSQL documentation, when canceling
                        // a request, if an invalid secret is provided then no
                        // exception should be sent back to the client.
                        server.trace(log, "Invalid CancelRequest: pid={}, key={}, proc={}", pid, key, processor);
                    }
                    worker.close(this);
                } else if (version == 80877103) {
                    server.trace(log, "SSLRequest");
                    this.needFlush = true;
                    ByteBuffer buf = ByteBuffer.allocate(1)
                    .put((byte)'N');
                    buf.flip();
                    offerWriteBuffer(buf);
                    enableWrite();
                } else {
                    server.trace(log, "StartupMessage");
                    server.trace(log, "version {} ({}.{})", version, (version >> 16), (version & 0xff));
                    this.needFlush = true;
                    for (;;) {
                        String param = readString();
                        if (param.isEmpty()) {
                            break;
                        }
                        String value = readString();
                        if ("user".equals(param)) {
                            this.userName = value;
                        } else if ("database".equals(param)) {
                            this.databaseName = server.checkKeyAndGetDatabaseName(value);
                        } else if ("client_encoding".equals(param)) {
                            // UTF8
                            this.clientEncoding = value;
                        } else if ("DateStyle".equals(param)) {
                            if (value.indexOf(',') < 0) {
                                value += ", MDY";
                            }
                            this.dateStyle = value;
                        }
                        // extra_float_digits 2
                        // geqo on (Genetic Query Optimization)
                        server.trace(log, "param {} = {}", param, value);
                    }
                    
                    // Check user and database
                    if (this.userName == null) {
                        // user required
                        sendErrorAuth();
                        break;
                    }
                    if (this.databaseName == null) {
                        // database optional, and default as user
                        this.databaseName = this.userName;
                    }
                    this.databaseName = StringUtils.toLowerEnglish(this.databaseName);
                    User user = null;
                    try {
                        user = server.selectUser(getRemoteAddress(), this.userName, this.databaseName);
                    } catch (SQLException e) {
                        log.error("Can't query user information", e);
                        user = null;
                    }
                    if (user == null) {
                        sendErrorAuth();
                    } else {
                        this.user = user;
                        // Request authentication
                        if ("trust".equals(user.getAuthMethod())) {
                            authOk();
                        } else {
                            sendAuthenticationMessage();
                            this.initDone = true;
                        }
                    }
                }
                break;
            case 'p': {
                server.trace(log, "PasswordMessage");
                this.needFlush = true;
                
                String password = readString();
                if (!this.authMethod.equals(password)) {
                    sendErrorAuth();
                    break;
                }
                authOk();
                break;
            }
            case 'P': {
                server.trace(log, "Parse");
                this.xQueryFailed = true;
                Prepared p = new Prepared();
                p.name = readString();
                try (SQLParser parser = newSQLParser(readString())) {
                    SQLStatement sqlStmt = parser.next();
                    // check for single SQL prepared statement
                    for (; parser.hasNext(); ) {
                        SQLStatement next = parser.next();
                        if (sqlStmt.isEmpty()) {
                            IoUtils.close(sqlStmt);
                            sqlStmt = next;
                            continue;
                        }
                        
                        if (!next.isEmpty()) {
                            throw new SQLParseException(sqlStmt.getSQL()+"^");
                        }
                    }
                    sqlStmt.setContext(this);
                    p.sql = sqlStmt;
                    server.trace(log, "named '{}' sql \"{}\"", p.name, p.sql);
                    if (UNNAMED.equals(p.name)) {
                        destroyPrepared(UNNAMED);
                    }
                    int paramTypesCount = readShort();
                    int[] paramTypes = null;
                    if (paramTypesCount > 0) {
                        if (sqlStmt instanceof MetaStatement) {
                            String message = "Meta statement can't be parameterized";
                            throw convertError(SQLiteErrorCode.SQLITE_PERM, message);
                        }
                        paramTypes = new int[paramTypesCount];
                        for (int i = 0; i < paramTypesCount; i++) {
                            paramTypes[i] = readInt();
                        }
                    }
                    
                    // Prepare SQL
                    if (!sqlStmt.isEmpty()) {
                        PreparedStatement prep = sqlStmt.prepare();
                        if (!(sqlStmt instanceof MetaStatement)) {
                            ParameterMetaData meta = prep.getParameterMetaData();
                            p.paramType = new int[meta.getParameterCount()];
                            for (int i = 0; i < p.paramType.length; i++) {
                                int type;
                                if (i < paramTypesCount && paramTypes[i] != 0) {
                                    type = paramTypes[i];
                                } else {
                                    type = PgServer.convertType(meta.getParameterType(i + 1));
                                }
                                p.paramType[i] = type;
                            }
                        }
                    }
                    prepared.put(p.name, p);
                    
                    sendParseComplete();
                    this.xQueryFailed = false;
                } catch (SQLParseException e) {
                    sendErrorResponse(e);
                } catch (SQLException e) {
                    sendErrorResponse(e);
                }
                break;
            }
            case 'B': {
                server.trace(log, "Bind");
                this.xQueryFailed = true;
                Portal portal = new Portal();
                portal.name = readString();
                String prepName = readString();
                Prepared prep = prepared.get(prepName);
                if (prep == null) {
                    sendErrorResponse("Prepared not found");
                    break;
                }
                portal.prep = prep;
                portals.put(portal.name, portal);
                int formatCodeCount = readShort();
                int[] formatCodes = new int[formatCodeCount];
                for (int i = 0; i < formatCodeCount; i++) {
                    formatCodes[i] = readShort();
                }
                int paramCount = readShort();
                try {
                    PreparedStatement ps = prep.sql.getPreparedStatement();
                    for (int i = 0; i < paramCount; i++) {
                        setParameter(ps, prep.paramType[i], i, formatCodes);
                    }
                } catch (SQLException e) {
                    sendErrorResponse(e);
                    break;
                }
                int resultCodeCount = readShort();
                portal.resultColumnFormat = new int[resultCodeCount];
                for (int i = 0; i < resultCodeCount; i++) {
                    portal.resultColumnFormat[i] = readShort();
                }
                sendBindComplete();
                this.portal = portal;
                this.xQueryFailed = false;
                break;
            }
            case 'C': {
                server.trace(log, "Close");
                this.needFlush = true;
                char type = (char) readByte();
                String name = readString();
                if (type == 'S') {
                    destroyPrepared(name);
                } else if (type == 'P') {
                    portals.remove(name);
                } else {
                    server.trace(log, "expected S or P, got {}", type);
                    sendErrorResponse("expected S or P");
                    break;
                }
                sendCloseComplete();
                break;
            }
            case 'D': {
                server.trace(log, "Describe");
                this.xQueryFailed = true;
                char type = (char) readByte();
                String name = readString();
                if (type == 'S') {
                    Prepared p = prepared.get(name);
                    if (p == null) {
                        sendErrorResponse("Prepared not found: " + name);
                    } else {
                        try {
                            ParameterMetaData paramMeta = null;
                            ResultSetMetaData rsMeta = null;
                            SQLStatement sql = p.sql;
                            if (!sql.isEmpty()) {
                                if (!(sql instanceof MetaStatement)) {
                                    paramMeta = sql.getParameterMetaData();
                                }
                                rsMeta = sql.getPreparedMetaData();
                            }
                            sendParameterDescription(paramMeta, p.paramType);
                            sendRowDescription(rsMeta);
                            this.xQueryFailed = false;
                        } catch (SQLException e) {
                            sendErrorResponse(e);
                        }
                    }
                } else if (type == 'P') {
                    Portal p = portals.get(name);
                    if (p == null) {
                        sendErrorResponse("Portal not found: " + name);
                    } else {
                        SQLStatement sqlStmt = p.prep.sql;
                        try {
                            ResultSetMetaData meta = null;
                            if (!sqlStmt.isEmpty()) {
                                meta = sqlStmt.getPreparedMetaData();
                            }
                            sendRowDescription(meta);
                            this.xQueryFailed = false;
                        } catch (SQLException e) {
                            sendErrorResponse(e);
                        }
                    }
                } else {
                    server.trace(log, "expected S or P, got {}", type);
                    sendErrorResponse("expected S or P");
                }
                break;
            }
            case 'E': {
                server.trace(log, "Execute");
                this.xQueryFailed = true;
                String name = readString();
                final Portal p = portals.get(name);
                if (p == null) {
                    sendErrorResponse("Portal not found: " + name);
                    break;
                }
                final int maxRows = readShort();
                Prepared prepared = p.prep;
                final SQLStatement sqlStmt = prepared.sql;
                // check empty statement
                if (sqlStmt.isEmpty()) {
                    server.trace(log, "query string empty: {}", sqlStmt);
                    sendEmptyQueryResponse();
                    this.xQueryFailed = false;
                    break;
                }
                processXQuery(p, maxRows);
                break;
            }
            case 'S': {
                server.trace(log, "Sync");
                SyncProcessTask syncTask = new SyncProcessTask(this);
                startQueryTask(syncTask);
                break;
            }
            case 'Q': {
                server.trace(log, "Query");
                destroyPrepared(UNNAMED);
                String query = readString();
                processQuery(query);
                break;
            }
            case 'X': {
                server.trace(log, "Terminate");
                // Here we must release DB resources!
                IoUtils.close(getConnection());
                worker.close(this);
                break;
            }
            default:
                server.trace(log, "Unsupported message: type {}(c) {}", (char) x, x);
                break;
            }
            
            // reset and cleanup
            this.dataBuf = null;
            this.inSize = -1;
            rem = resetReadBuffer();
        } while(!this.needFlush && rem >= 5 && !hasAsyncTask());
        
        // Must disable read if a statement running
        if (hasAsyncTask()) {
            disableRead();
        }
    }
    
    protected boolean hasAsyncTask() {
        return (this.queryTask != null || this.writeTask != null);
    }
    
    protected void processXQuery(Portal p, int maxRows) throws IllegalStateException {
        SQLiteQueryTask queryTask = new XQueryTask(this, p, maxRows);
        startQueryTask(queryTask);
    }
    
    protected void processQuery(final String query) throws IllegalStateException {
        SQLiteQueryTask queryTask = new QueryTask(this, query);
        startQueryTask(queryTask);
    }
    
    protected void startQueryTask (SQLiteQueryTask queryTask) throws IllegalStateException {
        if (this.queryTask != null) {
            throw new IllegalStateException("A query task already exists");
        }
        
        this.queryTask = queryTask;
        this.queryTask.run(); 
    }
    
    protected void startWriteTask (SQLiteProcessorTask writeTask) throws IllegalStateException {
        if (this.writeTask != null) {
            throw new IllegalStateException("A write task already exists");
        }
        this.writeTask = writeTask;
        this.writeTask.run();
    }
    
    protected void authOk() throws IOException {
        SQLiteConnection conn = null;
        boolean failed = true;
        try {
            this.authMethod = null;
            
            conn = server.newSQLiteConnection(this.databaseName);
            if (isTrace()) {
                trace(log, "SQLite init: autoCommit {}", conn.getAutoCommit());
            }
            InitQueryTask initTask = new InitQueryTask(this, conn);
            startQueryTask(initTask);
            failed = false;
        } catch (SQLException cause) {
            sendErrorResponse(cause);
            stop();
            traceError(log, "Can't init database", cause);
        } finally {
            if(failed) {
                IoUtils.close(conn);
            }
        }
    }
    
    protected SQLParser newSQLParser(String sqls) {
        return new SQLParser(sqls, true);
    }
    
    protected void destroyPrepared(String name) {
        Prepared p = prepared.remove(name);
        if (p != null) {
            server.trace(log, "Destroy");
            server.trace(log, "the named '{}' prepared and it's all portal", name);
            IoUtils.close(p.sql);
            // Need to close all generated portals by this prepared
            Iterator<Entry<String, Portal>> i;
            for (i = portals.entrySet().iterator(); i.hasNext(); ) {
                Portal po = i.next().getValue();
                if (po.prep == p) {
                    i.remove();
                }
            }
        }
    }
    
    private static boolean formatAsText(int pgType) {
        switch (pgType) {
        // TODO: add more types to send as binary once compatibility is
        // confirmed
        case PgServer.PG_TYPE_BYTEA:
            return false;
        }
        return true;
    }
    
    private static int getTypeSize(int pgType, int precision) {
        switch (pgType) {
        case PgServer.PG_TYPE_BOOL:
            return 1;
        case PgServer.PG_TYPE_VARCHAR:
            return Math.max(255, precision + 10);
        default:
            return precision + 4;
        }
    }
    
    private void sendAuthenticationMessage() throws IOException {
        switch (this.user.getAuthMethod()) {
        case PgServer.AUTH_PASSWORD:
            sendAuthenticationCleartextPassword();
            break;
        default: // md5
            sendAuthenticationMD5Password();
            break;
        }
        this.authMethod.init(this.user.getUser(), this.user.getPassword());
        
        PgServer server = getServer();
        server.trace(log, "authMethod {}", this.authMethod);
    }
    
    private void sendAuthenticationCleartextPassword() throws IOException {
        PgServer server = getServer();
        String proto = server.getProtocol();
        this.authMethod = server.newAuthMethod(proto, this.user.getAuthMethod());
        
        startMessage('R');
        writeInt(AUTH_REQ_PASSWORD);
        sendMessage();
    }
    
    private void sendAuthenticationMD5Password() throws IOException {
        PgServer server = getServer();
        String proto = server.getProtocol();
        MD5Password md5 = (MD5Password)server.newAuthMethod(proto, this.user.getAuthMethod());
        this.authMethod = md5;
        
        startMessage('R');
        writeInt(AUTH_REQ_MD5);
        write(md5.getSalt());
        sendMessage();
    }
    
    private void sendAuthenticationOk() throws IOException {
        startMessage('R');
        writeInt(AUTH_REQ_OK);
        sendMessage();
        sendParameterStatus("client_encoding", clientEncoding);
        sendParameterStatus("DateStyle", dateStyle);
        sendParameterStatus("integer_datetimes", "off");
        sendParameterStatus("is_superuser", "off");
        sendParameterStatus("server_encoding", "SQL_ASCII");
        sendParameterStatus("server_version", PgServer.PG_VERSION);
        sendParameterStatus("session_authorization", userName);
        sendParameterStatus("standard_conforming_strings", "off");
        // TODO PostgreSQL TimeZone
        sendParameterStatus("TimeZone", "CET");
        sendParameterStatus("integer_datetimes", INTEGER_DATE_TYPES ? "on" : "off");
        sendBackendKeyData();
        sendReadyForQuery();
    }
    
    private void sendBindComplete() throws IOException {
        startMessage('2');
        sendMessage();
    }
    
    private void sendCancelQueryResponse() throws IOException {
        server.trace(log, "CancelSuccessResponse");
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        writeString("57014");
        write('M');
        writeString("canceling statement due to user request");
        write(0);
        sendMessage();
    }
    
    private void sendCloseComplete() throws IOException {
        startMessage('3');
        sendMessage();
    }
    
    private void sendCommandComplete(SQLStatement sql, int updateCount, boolean resultSet) 
        throws IOException {
        String command = sql.getCommand();
        
        startMessage('C');
        switch (command) {
        case "INSERT":
            writeStringPart("INSERT 0 ");
            writeString(updateCount + "");
            break;
        case "UPDATE":
            writeStringPart("UPDATE ");
            writeString(updateCount + "");
            break;
        case "DELETE":
            writeStringPart("DELETE ");
            writeString(updateCount + "");
            break;
        case "CALL":
        case "PRAGMA":
        case "SELECT":
            writeString("SELECT");
            break;
        case "BEGIN":
            writeString("BEGIN");
            break;
        default:
            server.trace(log, "check CommandComplete for command {}", command);
            writeStringPart("UPDATE ");
            writeString(updateCount + "");
            break;
        }
        sendMessage();
    }
    
    private void sendEmptyQueryResponse() throws IOException {
        startMessage('I');
        sendMessage();
    }
    
    private void sendErrorAuth() throws IOException {
        PgServer server = getServer();
        String protocol = server.getProtocol();
        
        SQLiteErrorCode error = SQLiteErrorCode.SQLITE_AUTH;
        String message = error.message;
        if (this.user == null) {
            InetSocketAddress remoteAddr = getRemoteAddress();
            String host = remoteAddr.getAddress().getHostAddress();
            message = format("%s for '%s'@'%s' (using protocol: %s)", 
                    message, this.userName, host, protocol);
        } else {
            String authMethod = this.user.getAuthMethod();
            message = format("%s for '%s'@'%s' (using protocol: %s, auth method: %s)", 
                    message, this.userName, this.user.getHost(), protocol, authMethod);
        }
        
        sendErrorResponse(convertError(error, message));
        stop();
    }
    
    private void sendErrorResponse(SQLParseException e) throws IOException {
        SQLiteErrorCode code = SQLiteErrorCode.SQLITE_ERROR;
        String message = format("SQL error, %s", e.getMessage());
        SQLException sqlError = new SQLException(message, "42000", code.code);
        sendErrorResponse(sqlError);
    }
    
    private void sendErrorResponse(SQLException e) throws IOException {
        this.server.traceError(log, "send an error message", e);
        String sqlState = e.getSQLState();
        if (sqlState == null) {
            sqlState = "HY000";
        }
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        writeString(sqlState);
        write('M');
        writeString(e.getMessage());
        write('D');
        writeString(e.toString());
        write(0);
        sendMessage();
    }
    
    private void sendErrorResponse(String message) throws IOException {
        server.trace(log, "Exception: {}", message);
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        // PROTOCOL VIOLATION
        writeString("08P01");
        write('M');
        writeString(message);
        sendMessage();
    }
    
    private void sendMessage() throws IOException {
        this.dataOut.flush();
        byte[] buff = this.outBuf.toByteArray();
        int len = buff.length - 1;
        
        ByteBuffer buffer = ByteBuffer.wrap(buff)
        .put(0, (byte)this.messageType)
        .put(1, (byte)(len >>> 24))
        .put(2, (byte)(len >>> 16))
        .put(3, (byte)(len >>> 8))
        .put(4, (byte)(len >>> 0));
        
        offerWriteBuffer(buffer);
        if (this.needFlush) {
            enableWrite();
        }
        
        // cleanup
        this.dataOut = null;
        this.outBuf  = null;
    }
    
    private void sendNoData() throws IOException {
        startMessage('n');
        sendMessage();
    }
    
    private void sendParameterDescription(ParameterMetaData meta, int[] paramTypes) 
            throws IOException, SQLException {
        int count = 0;
        if (meta != null) {
            count = meta.getParameterCount();
        }
        
        startMessage('t');
        writeShort(count);
        for (int i = 0; i < count; i++) {
            int type;
            if (paramTypes != null && paramTypes[i] != 0) {
                type = paramTypes[i];
            } else {
                type = PgServer.PG_TYPE_VARCHAR;
            }
            writeInt(type);
        }
        sendMessage();
    }
    
    private void sendParameterStatus(String param, String value) throws IOException {
        startMessage('S');
        writeString(param);
        writeString(value);
        sendMessage();
    }
    
    private void sendParseComplete() throws IOException {
        startMessage('1');
        sendMessage();
    }
    
    private void sendBackendKeyData() throws IOException {
        startMessage('K');
        writeInt(this.id);
        writeInt(this.secret);
        sendMessage();
    }
    
    private void sendReadyForQuery() throws IOException {
        startMessage('Z');
        char c;
        try {
            SQLiteConnection conn = getConnection();
            boolean autocommit = conn.getAutoCommit();
            if (autocommit) {
                // idle
                c = 'I';
            } else {
                // in a transaction block
                c = 'T';
            }
            trace(log, "SQLite ready: autoCommit {}", autocommit);
        } catch (SQLException e) {
            // failed transaction block
            c = 'E';
        }
        write((byte) c);
        sendMessage();
    }
    
    private void sendDataRow(ResultSet rs, int[] formatCodes) throws IOException, SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columns = metaData.getColumnCount();
        startMessage('D');
        writeShort(columns);
        for (int i = 1; i <= columns; i++) {
            int pgType = PgServer.convertType(metaData.getColumnType(i));
            boolean text = formatAsText(pgType);
            if (formatCodes != null) {
                if (formatCodes.length == 0) {
                    text = true;
                } else if (formatCodes.length == 1) {
                    text = formatCodes[0] == 0;
                } else if (i - 1 < formatCodes.length) {
                    text = formatCodes[i - 1] == 0;
                }
            }
            writeDataColumn(rs, i, pgType, text);
        }
        sendMessage();
    }
    
    private void sendRowDescription(ResultSetMetaData meta) throws IOException, SQLException {
        CoreResultSet rs = null;
        if (meta instanceof CoreResultSet) {
            rs = (CoreResultSet)meta;
        }
        if (meta == null || (rs != null && (rs.colsMeta==null || rs.colsMeta.length==0))) {
            sendNoData();
        } else {
            int columns = meta.getColumnCount();
            int[] types = new int[columns];
            int[] precision = new int[columns];
            String[] names = new String[columns];
            for (int i = 0; i < columns; i++) {
                String name = meta.getColumnName(i + 1);
                names[i] = name;
                int type = meta.getColumnType(i + 1);
                int pgType = PgServer.convertType(type);
                // the ODBC client needs the column pg_catalog.pg_index
                // to be of type 'int2vector'
                // if (name.equalsIgnoreCase("indkey") &&
                //         "pg_index".equalsIgnoreCase(
                //         meta.getTableName(i + 1))) {
                //     type = PgServer.PG_TYPE_INT2VECTOR;
                // }
                precision[i] = meta.getColumnDisplaySize(i + 1);
                types[i] = pgType;
            }
            startMessage('T');
            writeShort(columns);
            for (int i = 0; i < columns; i++) {
                writeString(StringUtils.toLowerEnglish(names[i]));
                // object ID
                writeInt(0);
                // attribute number of the column
                writeShort(0);
                // data type
                writeInt(types[i]);
                // pg_type.typlen
                writeShort(getTypeSize(types[i], precision[i]));
                // pg_attribute.atttypmod
                writeInt(-1);
                // the format type: text = 0, binary = 1
                writeShort(formatAsText(types[i]) ? 0 : 1);
            }
            sendMessage();
        }
    }
    
    private void startMessage(int newMessageType) throws IOException {
        this.messageType = newMessageType;
        this.outBuf = new ByteArrayOutputStream();
        this.dataOut = new DataOutputStream(this.outBuf);
        // Preserve the message header space
        this.dataOut.write(0);
        this.dataOut.writeInt(0);
    }
    
    private void writeDataColumn(ResultSet rs, int column, int pgType, boolean text)
            throws IOException, SQLException {
        rs.getObject(column);
        if (rs.wasNull()) {
            writeInt(-1);
            return;
        }
        if (text) {
            // plain text
            switch (pgType) {
            case PgServer.PG_TYPE_BOOL:
                writeInt(1);
                dataOut.writeByte(rs.getInt(column) == 1 ? 't' : 'f');
                break;
            default:
                String value = rs.getString(column);
                byte[] data = value.getBytes(getEncoding());
                writeInt(data.length);
                write(data);
            }
        } else {
            // binary
            switch (pgType) {
            case PgServer.PG_TYPE_INT2:
                writeInt(2);
                writeShort(rs.getShort(column));
                break;
            case PgServer.PG_TYPE_INT4:
                writeInt(4);
                writeInt(rs.getInt(column));
                break;
            case PgServer.PG_TYPE_INT8:
                writeInt(8);
                dataOut.writeLong(rs.getLong(column));
                break;
            case PgServer.PG_TYPE_FLOAT4:
                writeInt(4);
                dataOut.writeFloat(rs.getFloat(column));
                break;
            case PgServer.PG_TYPE_FLOAT8:
                writeInt(8);
                dataOut.writeDouble(rs.getDouble(column));
                break;
            case PgServer.PG_TYPE_BYTEA: {
                byte[] data = rs.getBytes(column);
                writeInt(data.length);
                write(data);
                break;
            }
            case PgServer.PG_TYPE_DATE: {
                Date d = rs.getDate(column);
                writeInt(4);
                writeInt((int) (toPostgreDays(d.getTime())));
                break;
            }
            case PgServer.PG_TYPE_TIME: {
                Time t = rs.getTime(column);
                writeInt(8);
                long m = t.getTime() * 1000000L;
                if (INTEGER_DATE_TYPES) {
                    // long format
                    m /= 1_000;
                } else {
                    // double format
                    m = Double.doubleToLongBits(m * 0.000_000_001);
                }
                dataOut.writeLong(m);
                break;
            }
            case PgServer.PG_TYPE_TIMESTAMP_NO_TMZONE: {
                Timestamp t = rs.getTimestamp(column);
                writeInt(8);
                long m = toPostgreDays(t.getTime()) * 86_400;
                long nanos = t.getTime() * 1000000L;
                if (INTEGER_DATE_TYPES) {
                    // long format
                    m = m * 1_000_000 + nanos / 1_000;
                } else {
                    // double format
                    m = Double.doubleToLongBits(m + nanos * 0.000_000_001);
                }
                dataOut.writeLong(m);
                break;
            }
            default: throw new IllegalStateException("output binary format is undefined");
            }
        }
    }
    
    private void writeString(String s) throws IOException {
        writeStringPart(s);
        write(0);
    }

    private void writeStringPart(String s) throws IOException {
        write(s.getBytes(getEncoding()));
    }
    
    private void writeInt(int i) throws IOException {
        this.dataOut.writeInt(i);
    }

    private void writeShort(int i) throws IOException {
        this.dataOut.writeShort(i);
    }

    private void write(byte[] data) throws IOException {
        this.dataOut.write(data);
    }

    private void write(int b) throws IOException {
        this.dataOut.write(b);
    }
    
    private String readString() throws IOException {
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        while (true) {
            int x = this.dataBuf.read();
            if (x <= 0) {
                break;
            }
            buff.write(x);
        }
        return new String(buff.toByteArray(), getEncoding());
    }
    
    private int readInt() throws IOException {
        return this.dataBuf.readInt();
    }

    private short readShort() throws IOException {
        return this.dataBuf.readShort();
    }

    private byte readByte() throws IOException {
        return this.dataBuf.readByte();
    }

    private void readFully(byte[] buff) throws IOException {
        this.dataBuf.readFully(buff);
    }
    
    private Charset getEncoding() {
        if ("UNICODE".equals(clientEncoding)) {
            return StandardCharsets.UTF_8;
        }
        return Charset.forName(clientEncoding);
    }
    
    private static long toPostgreDays(long dateValue) {
        return DateTimeUtils.absoluteDayFromDateValue(dateValue) - 10_957;
    }
    
    /**
     * Represents a PostgreSQL Prepared object.
     */
    static class Prepared {

        /**
         * The object name.
         */
        String name;

        /**
         * The SQL statement.
         */
        SQLStatement sql;

        /**
         * The list of parameter types (if set).
         */
        int[] paramType;
    }
    
    /**
     * Represents a PostgreSQL Portal object.
     */
    static class Portal {

        /**
         * The portal name.
         */
        String name;

        /**
         * The format used in the result set columns (if set).
         */
        int[] resultColumnFormat;

        /**
         * The prepared object.
         */
        Prepared prep;
    }
    
    static class InitQueryTask extends SQLiteQueryTask {
        final SQLiteConnection connection;
        
        protected InitQueryTask(PgProcessor proc, SQLiteConnection connection) {
            super(proc);
            this.connection = connection;
        }

        @Override
        protected void execute() throws IOException {
            PgProcessor proc = (PgProcessor)this.proc;
            boolean timeout = true;
            try {
                checkBusyState();
                timeout = false;
                proc.server.initConnection(connection);
                proc.setConnection(this.connection);
                proc.worker.dbIdle();
            } catch (SQLException e) {
                long currentTime = System.currentTimeMillis();
                int passTime = (int)(currentTime - proc.getCreateTime());
                int connectTimeout = proc.server.getConnectTimeout();
                int busyTimeout = connectTimeout - passTime;
                if (busyTimeout < 0 || !handleBlocked(timeout, e, busyTimeout)) {
                    proc.sendErrorResponse(e);
                    proc.stop();
                }
                return;
            }
            
            proc.sendAuthenticationOk();
            proc.initDone = true;
            finish();
        }
        
    }
    
    static class XQueryTask extends SQLiteQueryTask {
        final Portal p;
        final SQLStatement sqlStmt;
        final int maxRows;
        
        XQueryTask(PgProcessor proc, Portal p, int maxRows) {
            super(proc);
            this.p = p;
            this.sqlStmt = p.prep.sql;
            this.maxRows = maxRows;
        }
        
        @Override
        protected void execute() throws IOException {
            PgProcessor proc = (PgProcessor)this.proc;
            boolean timeout = true;
            try {
                checkBusyState();
                timeout = false;
                boolean resultSet = this.sqlStmt.execute(maxRows);
                if (resultSet) {
                    boolean async = this.async;
                    this.async = false;
                    finish();
                    // Hand over task to write task
                    XQueryWriteTask writeTask = new XQueryWriteTask(proc, this.p, async);
                    proc.startWriteTask(writeTask);
                    return;
                }
                
                int count = this.sqlStmt.getUpdateCount();
                proc.sendCommandComplete(this.sqlStmt, count, resultSet);
                proc.xQueryFailed = false;
            } catch (SQLException e) {
                if (handleBlocked(timeout, e)) {
                    return;
                }
                
                if (this.sqlStmt.executionException(e)) {
                    proc.sendCommandComplete(this.sqlStmt, 0, false);
                    proc.xQueryFailed = false;
                } else {
                    if (proc.server.isCanceled(e)) {
                        proc.sendCancelQueryResponse();
                    } else {
                        proc.sendErrorResponse(e);
                    } 
                }
            } // try-catch
            
            finish();
        }
    }
    
    static class XQueryWriteTask extends SQLiteProcessorTask {
        final Portal p;
        ResultSet rs;
        
        XQueryWriteTask(PgProcessor proc, Portal p, boolean async) {
            super(proc);
            this.p = p;
            this.async = async;
        }
        
        XQueryWriteTask(PgProcessor proc, Portal p) {
            this(proc, p, false);
        }
            
        @Override
        protected void execute() throws IOException {
            PgProcessor proc = (PgProcessor)this.proc;
            SQLStatement stmt = p.prep.sql;
            boolean resetTask = true;
            try {
                if (this.rs == null) {
                    this.rs = stmt.getResultSet();
                }
                // the meta-data is sent in the prior 'Describe'
                for (; this.rs.next(); ) {
                    proc.sendDataRow(this.rs, this.p.resultColumnFormat);
                    if (proc.canFlush()) {
                        this.async = true;
                        proc.enableWrite();
                        resetTask = false;
                        return;
                    }
                }
                
                // detach only after resultSet finished
                int n = stmt.getUpdateCount();
                proc.sendCommandComplete(stmt, n, true);
                proc.xQueryFailed = false;
            } catch (SQLException e) {
                if (proc.server.isCanceled(e)) {
                    proc.sendCancelQueryResponse();
                } else {
                    proc.sendErrorResponse(e);
                }
            } finally {
                if (resetTask) {
                    proc.writeTask = null;
                    IoUtils.close(this.rs);
                }
            }
            
            finish();
        }
        
    }
    
    static class QueryTask extends SQLiteQueryTask {
        final SQLParser parser;
        
        // ResultSet remaining state
        SQLStatement curStmt;
        ResultSet rs;
        
        QueryTask(PgProcessor proc, String query) {
            super(proc);
            this.parser = proc.newSQLParser(query);
        }
        
        @Override
        protected void execute() throws IOException {
            PgProcessor proc = (PgProcessor)this.proc;
            SQLStatement sqlStmt = this.curStmt;
            boolean resetTask = true;
            try {
                boolean next = true;
                
                if (this.rs == null) {
                    // check empty query string
                    for (; sqlStmt == null;) {
                        if (this.parser.hasNext()) {
                            SQLStatement s = this.parser.next();
                            if (s.isEmpty()) {
                                continue;
                            }
                            sqlStmt = s;
                        }
                        break;
                    }
                    if (sqlStmt == null) {
                        proc.server.trace(log, "query string empty");
                        proc.needFlush = true;
                        proc.sendEmptyQueryResponse();
                        next = false;
                    }
                } else {
                    // Continue write remaining resultSet
                    for (; this.rs.next(); ) {
                        proc.sendDataRow(this.rs, null);
                        if (proc.canFlush()) {
                            proc.enableWrite();
                            resetTask = false;
                            return;
                        }
                    }
                    int n = this.curStmt.getUpdateCount();
                    this.curStmt.complete(true);
                    IoUtils.close(this.curStmt);
                    this.rs = null;
                    proc.sendCommandComplete(this.curStmt, n, true);
                    
                    // try next
                    if (next=this.parser.hasNext()) {
                        IoUtils.close(sqlStmt);
                        sqlStmt = this.parser.next();
                    }
                }
                
                for (; next; ) {
                    boolean timeout = true;
                    try {
                        sqlStmt.setContext(proc);
                        proc.writeTask = null;
                        checkBusyState();
                        timeout = false;
                        boolean result = sqlStmt.execute(0);
                        if (result) {
                            ResultSet rs = sqlStmt.getResultSet();
                            ResultSetMetaData meta = rs.getMetaData();
                            proc.sendRowDescription(meta);
                            for (; rs.next(); ) {
                                proc.sendDataRow(rs, null);
                                if (proc.canFlush()) {
                                    this.async = true;
                                    this.curStmt = sqlStmt;
                                    this.rs = rs;
                                    proc.enableWrite();
                                    resetTask = false;
                                    proc.startWriteTask(this);
                                    return;
                                }
                            }
                            int n = sqlStmt.getUpdateCount();
                            sqlStmt.complete(true);
                            IoUtils.close(sqlStmt);
                            this.rs = null;
                            this.curStmt = null;
                            proc.sendCommandComplete(sqlStmt, n, result);
                        } else {
                            int count = sqlStmt.getUpdateCount();
                            sqlStmt.complete(true);
                            proc.sendCommandComplete(sqlStmt, count, result);
                        }
                        
                        // try next
                        if (next=this.parser.hasNext()) {
                            IoUtils.close(sqlStmt);
                            sqlStmt = this.parser.next();
                        }
                    } catch (SQLException e) {
                        if (handleBlocked(timeout, e)) {
                            return;
                        }
                        
                        if (sqlStmt.executionException(e)) {
                            sqlStmt.complete(true);
                            proc.sendCommandComplete(sqlStmt, 0, false);
                            // try next
                            if (next=this.parser.hasNext()) {
                                IoUtils.close(sqlStmt);
                                sqlStmt = this.parser.next();
                            }
                        } else {
                            throw e;
                        }
                    }
                } // For statements
            } catch (SQLParseException e) {
                if (sqlStmt != null && sqlStmt.isOpen())  {
                    sqlStmt.complete(false);
                }
                proc.sendErrorResponse(e);
            } catch (SQLException e) {
                if (sqlStmt != null && sqlStmt.isOpen()) {
                    sqlStmt.complete(false);
                }
                if (proc.server.isCanceled(e)) {
                    proc.sendCancelQueryResponse();
                } else {
                    proc.sendErrorResponse(e);
                }
            } finally {
                if (resetTask) {
                    proc.writeTask = null;
                    IoUtils.close(sqlStmt);
                    IoUtils.close(this.curStmt);
                    this.rs = null;
                    IoUtils.close(parser);
                }
            }
            
            this.async = false;
            if (resetTask) {
                proc.needFlush = true;
                proc.sendReadyForQuery();
            }
            finish();
        }
        
    }

    static class SyncProcessTask extends SQLiteQueryTask {
        
        public SyncProcessTask(PgProcessor proc) {
            super(proc);
        }

        @Override
        protected void execute() throws IOException {
            PgProcessor proc = (PgProcessor)this.proc;
            Portal portal = proc.portal;
            
            if (portal != null) { // after bind 
                SQLStatement sqlStmt = portal.prep.sql;
                boolean success = !proc.xQueryFailed;
                boolean timeout = true;
                
                try {
                    checkBusyState();
                    timeout = false;
                    sqlStmt.complete(success);
                } catch (ImplicitCommitException e) {
                    SQLException cause = e.getCause();
                    if (handleBlocked(timeout, cause)) {
                        return;
                    }
                    
                    sqlStmt.complete(false); // rollback
                    proc.sendErrorResponse(cause);
                } catch (SQLException cause) {
                    sqlStmt.complete(false); // rollback
                    proc.sendErrorResponse(cause);
                }
            }
            
            proc.portal = null;
            proc.xQueryFailed = false;
            proc.needFlush = true;
            proc.sendReadyForQuery();
            
            finish();
        }
        
    }
    
}
