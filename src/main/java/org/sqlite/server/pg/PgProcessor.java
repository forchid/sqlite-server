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
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConnection;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.core.CoreResultSet;
import org.sqlite.server.NetworkException;
import org.sqlite.server.SQLiteProcessor;
import org.sqlite.server.SQLiteWorker;
import org.sqlite.server.meta.User;
import org.sqlite.sql.SQLParseException;
import org.sqlite.sql.SQLParser;
import org.sqlite.sql.SQLStatement;
import org.sqlite.sql.TransactionStatement;
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
    private final ByteBuffer inHeader = ByteBuffer.allocate(5);
    private ByteBuffer inBuf = inHeader;
    private DataInputStream dataBuf;
    private int x, inSize;
    
    private int messageType;
    private ByteArrayOutputStream outBuf;
    private DataOutputStream dataOut;
    private boolean needFlush;
    
    private String userName;
    private String databaseName;
    private String clientEncoding = "UTF-8";
    private String dateStyle = "ISO, MDY";
    
    private boolean initDone, xQueryFailed;
    private final HashMap<String, Prepared> prepared = new HashMap<>();
    private final HashMap<String, Portal> portals = new HashMap<>();
    private final Stack<TransactionStatement> savepointStack;

    protected PgProcessor(SocketChannel channel, int processId, PgServer server) 
            throws NetworkException {
        super(channel, processId, server);
        this.secret = (int)SecurityUtils.secureRandomLong();
        this.savepointStack = new Stack<>();
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
        
        int n;
        if (this.inBuf == this.inHeader) {
            if (this.initDone) {
                n = ch.read(this.inBuf);
                if (n < 0) {
                    stop();
                    enableWrite();
                    return;
                }
                if (this.inBuf.position() < 1) {
                    return;
                }
                x = this.inBuf.get(0) & 0xFF;
            } else {
                if (this.inBuf.position() == 0) {
                    this.inBuf.position(1);
                }
                x = 0;
            }
            
            n = ch.read(this.inBuf);
            if (n == -1) {
                stop();
                enableWrite();
                return;
            }
            if (this.inBuf.hasRemaining()) {
                return;
            }
            
            inSize =  (this.inBuf.get(1) & 0xFF) << 24
                    | (this.inBuf.get(2) & 0xFF) << 16
                    | (this.inBuf.get(3) & 0xFF) << 8
                    | (this.inBuf.get(4) & 0xFF) << 0;
            inSize -= 4;
            this.inBuf.clear();
            server.trace(log, ">> message: type '{}'(c) {}, len {}", (char)x, x, inSize);
            
            byte[] data = new byte[inSize];
            this.inBuf = ByteBuffer.wrap(data);
        }
        n = ch.read(this.inBuf);
        if (n < 0) {
            stop();
            enableWrite();
            return;
        }
        if (this.inBuf.hasRemaining()) {
            return;
        }
        // process: read OK
        byte[] data = this.inBuf.array();
        this.inBuf = this.inHeader;
        
        if (this.xQueryFailed && 'S' != x) {
            server.trace(log, "Discard any message for xQuery error detected until Sync");
            return;
        }
        this.dataBuf = new DataInputStream(new ByteArrayInputStream(data, 0, data.length));
        
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
                while (true) {
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
                        clientEncoding = value;
                    } else if ("DateStyle".equals(param)) {
                        if (value.indexOf(',') < 0) {
                            value += ", MDY";
                        }
                        dateStyle = value;
                    }
                    // extra_float_digits 2
                    // geqo on (Genetic Query Optimization)
                    server.trace(log, "param {} = {}", param, value);
                }
                
                // Check user and database
                User user = null;
                try {
                    user = server.selectUser(getRemoteAddress(), this.userName, this.databaseName);
                } catch (SQLException e) {
                    log.error("Can't query user information", e);
                    user = null;
                }
                if (user == null) {
                    this.needFlush = true;
                    sendErrorAuth();
                    break;
                }
                this.user = user;
                this.needFlush = true;
                
                // Request authentication
                if ("trust".equals(user.getAuthMethod())) {
                    authOk();
                } else {
                    sendAuthenticationMessage();
                }
                initDone = true;
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
                        sqlStmt = next;
                        continue;
                    }
                    
                    if (!next.isEmpty()) {
                        throw new SQLParseException(sqlStmt.getSQL()+"^");
                    }
                }
                p.sql = sqlStmt;
                server.trace(log, "named '{}' SQL: {}", p.name, p.sql);
                if (UNNAMED.equals(p.name)) {
                    destroyPrepared(UNNAMED);
                }
                int paramTypesCount = readShort();
                int[] paramTypes = null;
                if (paramTypesCount > 0) {
                    if (sqlStmt.isMetaStatement()) {
                        String message = "Meta statement can't be parameterized";
                        throw convertError(SQLiteErrorCode.SQLITE_PERM, message);
                    }
                    paramTypes = new int[paramTypesCount];
                    for (int i = 0; i < paramTypesCount; i++) {
                        paramTypes[i] = readInt();
                    }
                }
                
                if (!sqlStmt.isEmpty()) {
                    checkPerm(sqlStmt);
                    String sql = getSQL(sqlStmt);
                    p.prep = getConnection().prepareStatement(sql);
                    if (!sqlStmt.isMetaStatement()) {
                        ParameterMetaData meta = p.prep.getParameterMetaData();
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
                for (int i = 0; i < paramCount; i++) {
                    setParameter(prep.prep, prep.paramType[i], i, formatCodes);
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
                        if (!p.sql.isEmpty()) {
                            if (!p.sql.isMetaStatement()) {
                                paramMeta = p.prep.getParameterMetaData();
                            }
                            rsMeta = p.prep.getMetaData();
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
                    PreparedStatement prep = p.prep.prep;
                    try {
                        ResultSetMetaData meta = null;
                        if (!p.prep.sql.isEmpty()) {
                            meta = prep.getMetaData();
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
            Portal p = portals.get(name);
            if (p == null) {
                sendErrorResponse("Portal not found: " + name);
                break;
            }
            int maxRows = readShort();
            Prepared prepared = p.prep;
            SQLStatement sql = prepared.sql;
            server.trace(log, "execute SQL: {}", sql);
            
            // check empty statement
            if (sql.isEmpty()) {
                server.trace(log, "query string empty: {}", sql);
                sendEmptyQueryResponse();
                this.xQueryFailed = false;
                break;
            }
            
            try {
                PreparedStatement prep = prepared.prep;
                prep.setMaxRows(maxRows);
                // Set correct autoCommit for transaction status 
                // in readyForQuery() response
                boolean txBegin = tryBegin(sql);
                boolean exeFail = true, result = false;
                try {
                    result = prep.execute();
                    if (!txBegin && sql.isTransaction()) {
                        TransactionStatement txSql = (TransactionStatement)sql;
                        if (txSql.isSavepoint()) {
                            this.savepointStack.push(txSql);
                        }
                    }
                    exeFail= false;
                } finally {
                    if (exeFail && txBegin) {
                        resetAutoCommit();
                    }
                }
                tryFinish(sql);
                
                if (result) {
                    processResultSet(p);
                } else {
                    n = prep.getUpdateCount();
                    sendCommandComplete(sql, n, result);
                    this.xQueryFailed = false;
                }
            } catch (SQLException e) {
                if (isCanceled(e)) {
                    sendCancelQueryResponse();
                } else {
                    sendErrorResponse(e);
                }
            }
            break;
        }
        case 'S': {
            server.trace(log, "Sync");
            this.xQueryFailed = false;
            this.needFlush = true;
            sendReadyForQuery();
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
        
        // cleanup
        this.dataBuf = null;
    }
    
    protected void processResultSet(final Portal p) {
        if (this.writeTask != null) {
            throw new IllegalStateException("A write task already exists");
        }
        this.writeTask = new WriteTask(this) {
            final PgProcessor self = (PgProcessor)this.proc;
            final PreparedStatement prep = p.prep.prep;
            final SQLStatement sql = p.prep.sql;
            ResultSet rs;
            
            @Override
            protected void write() throws IOException {
                boolean resetTask = true;
                try {
                    if (this.rs == null) {
                        this.rs = prep.getResultSet();
                    }
                    // the meta-data is sent in the prior 'Describe'
                    for (; this.rs.next(); ) {
                        sendDataRow(this.rs, p.resultColumnFormat);
                        if (canFlush()) {
                            enableWrite();
                            resetTask = false;
                            return;
                        }
                    }
                    sendCommandComplete(sql, 0, true);
                    self.xQueryFailed = false;
                } catch (SQLException e) {
                    if (isCanceled(e)) {
                        sendCancelQueryResponse();
                    } else {
                        sendErrorResponse(e);
                    }
                } finally {
                    if (resetTask) {
                        self.writeTask = null;
                        IoUtils.close(this.rs);
                    }
                }
            }
        };
        
        this.writeTask.run();
    }
    
    protected void processQuery(final String query) throws IOException {
        if (this.writeTask != null) {
            throw new IllegalStateException("A write task already exists");
        }
        
        this.writeTask = new WriteTask(this) {
            final PgProcessor self = (PgProcessor)this.proc;
            final SQLParser parser = newSQLParser(query);
            final SQLiteConnection conn = getConnection();
            
            // ResultSet remaining state
            SQLStatement curStmt;
            Statement curStat;
            ResultSet rs;

            @Override
            protected void write() throws IOException {
                boolean resetTask = true;
                try {
                    SQLStatement sqlStmt = null;
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
                            server.trace(log, "query string empty: {}", query);
                            self.needFlush = true;
                            sendEmptyQueryResponse();
                            return;
                        }
                    } else {
                        // Continue write remaining resultSet
                        for (; this.rs.next(); ) {
                            sendDataRow(this.rs, null);
                            if (canFlush()) {
                                enableWrite();
                                resetTask = false;
                                return;
                            }
                        }
                        IoUtils.close(this.rs);
                        this.rs = null;
                        IoUtils.close(this.curStat);
                        this.curStat = null;
                        sendCommandComplete(this.curStmt, 0, true);
                        
                        // try next
                        if (next=this.parser.hasNext()) {
                            sqlStmt = this.parser.next();
                        }
                    }
                    
                    for (; next; ) {
                        Statement stat = null;
                        try {
                            checkPerm(sqlStmt);
                            // execute SQL
                            String sql = getSQL(sqlStmt);
                            server.trace(log, "execute SQL: {}", sql);
                            boolean txBegin = tryBegin(sqlStmt);
                            boolean exeFail = true, result = false;
                            try {
                                stat = conn.createStatement();
                                result = stat.execute(sql);
                                if (!txBegin && sqlStmt.isTransaction()) {
                                    TransactionStatement txSql = (TransactionStatement)sqlStmt;
                                    if (txSql.isSavepoint()) {
                                        self.savepointStack.push(txSql);
                                    }
                                }
                                exeFail= false;
                            } finally {
                                if (txBegin && exeFail) {
                                    resetAutoCommit();
                                }
                            }
                            tryFinish(sqlStmt);
                            
                            if (result) {
                                ResultSet rs = stat.getResultSet();
                                ResultSetMetaData meta = rs.getMetaData();
                                sendRowDescription(meta);
                                for (; rs.next(); ) {
                                    sendDataRow(rs, null);
                                    if (canFlush()) {
                                        this.curStmt = sqlStmt;
                                        this.curStat = stat;
                                        this.rs = rs;
                                        enableWrite();
                                        resetTask = false;
                                        return;
                                    }
                                }
                                sendCommandComplete(sqlStmt, 0, result);
                            } else {
                                int n = stat.getUpdateCount();
                                sendCommandComplete(sqlStmt, n, result);
                            }
                            
                            // try next
                            if (next=this.parser.hasNext()) {
                                sqlStmt = this.parser.next();
                            }
                        } finally {
                            if (resetTask) {
                                IoUtils.close(stat);
                            }
                        }
                    } // For statements
                } catch (SQLParseException e) {
                    sendErrorResponse(e);
                } catch (SQLException e) {
                    if (isCanceled(e)) {
                        sendCancelQueryResponse();
                    } else {
                        sendErrorResponse(e);
                    }
                } finally {
                    if (resetTask) {
                        self.writeTask = null;
                        IoUtils.close(this.rs);
                        this.rs = null;
                        IoUtils.close(this.curStat);
                        this.curStat = null;
                        IoUtils.close(parser);
                    }
                }
                
                if (resetTask) {
                    self.needFlush = true;
                    sendReadyForQuery();
                }
            }
        };
        
        this.writeTask.run();  
    }
    
    protected boolean authOk() throws IOException {
        SQLiteConnection conn = null;
        boolean failed = true;
        try {
            this.authMethod = null;
            
            conn = server.newSQLiteConnection(this.user, this.databaseName, false);
            if (server.isTrace()) {
                server.trace(log, "sqliteConn init: autocommit {}", conn.getAutoCommit());
            }
            setConnection(conn);
            sendAuthenticationOk();
            failed = false;
            return true;
        } catch (SQLException cause) {
            sendErrorResponse(cause);
            stop();
            server.traceError(log, "Can't init database", cause);
            return false;
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
            server.trace(log, "Destroy the named '{}' prepared and it's any portal", name);
            IoUtils.close(p.prep);
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
    
    private boolean tryBegin(SQLStatement sql) throws SQLException {
        boolean success = false;
        SQLiteConnection conn = getConnection();
        if (sql.isTransaction() && conn.getAutoCommit()) {
            TransactionStatement txSql = (TransactionStatement)sql;
            if (txSql.isBegin() || txSql.isSavepoint()) {
                server.trace(log, "tx: begin");
                this.savepointStack.clear();
                conn.getConnectionConfig().setAutoCommit(false);
                this.savepointStack.push(txSql);
                success = true;
            } 
        }
        if (server.isTrace()) {
            server.trace(log, "sqliteConn execute: autocommit {} ->", conn.getAutoCommit());
        }
        
        return success;
    }
    
    private void tryFinish(SQLStatement sql) throws SQLException {
        SQLiteConnection conn = getConnection();
        if (sql.isTransaction()) {
            TransactionStatement txSql = (TransactionStatement)sql;
            String command = sql.getCommand();
            switch (command) {
            case "COMMIT":
            case "END":
            case "ROLLBACK":
                if (txSql.hasSavepoint()) {
                    break;
                }
                conn.getConnectionConfig().setAutoCommit(true);
                this.savepointStack.clear();
                server.trace(log, "tx: finish");
                break;
            case "RELEASE":
                boolean autoCommit = this.savepointStack.isEmpty();
                String savepoint = txSql.getSavepointName();
                for (; !this.savepointStack.isEmpty(); ) {
                    TransactionStatement spSql = this.savepointStack.peek();
                    if (spSql.isBegin()) {
                        break;
                    }
                    this.savepointStack.pop();
                    String target = spSql.getSavepointName();
                    server.trace(log, "tx: release savepoint {}", target);
                    if (target.equalsIgnoreCase(savepoint)) {
                        autoCommit = this.savepointStack.isEmpty();
                        break;
                    }
                }
                if (autoCommit) {
                    conn.getConnectionConfig().setAutoCommit(true);
                    server.trace(log, "tx: finish");
                }
                break;
            }
        }
        
        boolean autoCommit = conn.getAutoCommit();
        if (autoCommit) {
            detachMetaDb(conn);
        }
        
        if (server.isTrace()) {
            server.trace(log, "sqliteConn execute: autocommit {} <-", autoCommit);
        }
    }
    
    private void resetAutoCommit() {
        getConnection().getConnectionConfig()
        .setAutoCommit(true);
        this.savepointStack.clear();
    }
    
    private static boolean isCanceled(SQLException e) {
        return (e.getErrorCode() == SQLiteErrorCode.SQLITE_INTERRUPT.code);
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
        case "SELECT":
        case "CALL":
        case "PRAGMA":
            writeString("SELECT");
            break;
        case "BEGIN":
            writeString("BEGIN");
            break;
        default:
            server.trace(log, "check CommandComplete tag for command {}", command);
            writeStringPart(sql.isQuery()? "SELECT": "UPDATE ");
            writeString(updateCount + "");
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
        
        sendErrorResponse(new SQLException(message, "28000", error.code));
        stop();
    }
    
    private void sendErrorResponse(SQLParseException e) throws IOException {
        SQLiteErrorCode code = SQLiteErrorCode.SQLITE_ERROR;
        String message = format("SQL error, %s", e.getMessage());
        SQLException sqlError = new SQLException(message, "42000", code.code);
        sendErrorResponse(sqlError);
    }
    
    private void sendErrorResponse(SQLException e) throws IOException {
        server.traceError(log, "send an error message", e);
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
            server.trace(log, "sqliteConn ready: autocommit {}", autocommit);
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
                byte[] data = rs.getString(column).getBytes(getEncoding());
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
         * The prepared statement.
         */
        PreparedStatement prep;

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
    
}
