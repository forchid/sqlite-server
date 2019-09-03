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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConnection;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.core.CoreResultSet;
import org.sqlite.server.Processor;
import org.sqlite.util.DateTimeUtils;
import org.sqlite.util.IoUtils;
import org.sqlite.util.SQLReader;
import org.sqlite.util.SecurityUtils;
import org.sqlite.util.StringUtils;

/**The PG protocol handler.
 * 
 * @author little-pan
 * @since 2019-09-01
 *
 */
public class PgProcessor extends Processor implements Runnable {
    
    static final Logger log = LoggerFactory.getLogger(PgProcessor.class);
    private static final boolean INTEGER_DATE_TYPES = false;
    
    // auth method
    private static final int AUTH_REQ_OK = 0;
    private static final int AUTH_REQ_PASSWORD = 3;
    private static final int AUTH_REQ_MD5 = 5;
    
    private final int secret;
    private AuthMethod authMethod;
    
    private DataInputStream dataIn, dataBuf;
    private OutputStream out;
    
    private int messageType;
    private ByteArrayOutputStream outBuf;
    private DataOutputStream dataOut;
    
    private String userName;
    private String databaseName;
    private String clientEncoding = "UTF-8";
    private String dateStyle = "ISO, MDY";
    
    private boolean initDone;
    private final HashMap<String, Prepared> prepared = new HashMap<>();
    private final HashMap<String, Portal> portals = new HashMap<>();

    /**
     * @param socket
     * @param processId
     * @param server
     */
    protected PgProcessor(Socket socket, int processId, PgServer server) {
        super(socket, processId, server);
        this.secret = (int)SecurityUtils.secureRandomLong();
    }
    
    public PgServer getServer() {
        return (PgServer)this.server;
    }
    
    protected SQLiteConnection getConnection() {
        return this.session.getConnection();
    }

    @Override
    public void run() {
        try {
            server.trace(log, "Connect");
            // buffer is very important for performance!
            int bufferSize = IoUtils.BUFFER_SIZE;
            InputStream in = new BufferedInputStream(socket.getInputStream(), bufferSize << 2);
            out = new BufferedOutputStream(socket.getOutputStream(), bufferSize << 4);
            dataIn = new DataInputStream(in);
            while (!isStopped()) {
                // Optimize flush()
                if (process()) {
                    out.flush();
                }
            }
        } catch (EOFException e) {
            // more or less normal disconnect
        } catch (Exception e) {
            server.traceError(log, getName(), "fatal", e);
        } finally {
            server.trace(log, "Disconnect");
            close();
        }
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
    
    private boolean process() throws IOException {
        PgServer server = getServer();
        
        int x;
        if (this.initDone) {
            x = this.dataIn.read();
            if (x < 0) {
                stop();
                return true;
            }
        } else {
            x = 0;
        }
        
        int len = this.dataIn.readInt();
        len -= 4;
        server.trace(log, "x {}(c) {}, len {}", (char)x, x, len);
        
        byte[] data = new byte[len];
        this.dataIn.readFully(data, 0, len);
        this.dataBuf = new DataInputStream(new ByteArrayInputStream(data, 0, len));
        
        boolean flush = false;
        switch (x) {
        case 0:
            server.trace(log, "Init");
            int version = readInt();
            if (version == 80877102) {
                server.trace(log, "CancelRequest");
                int pid = readInt();
                int key = readInt();
                PgProcessor processor = (PgProcessor)server.getProcessor(this.id);
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
                    server.trace(log, "Invalid CancelRequest: pid={}, key={}", pid, key);
                }
                close();
            } else if (version == 80877103) {
                server.trace(log, "SSLRequest");
                out.write('N');
                flush = true;
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
                        if (!server.getUser().equals(this.userName)){
                            sendErrorAuth();
                            flush = true;
                            break;
                        }
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
                sendAuthenticationMessage();
                flush = true;
                initDone = true;
            }
            break;
        case 'p': {
            server.trace(log, "PasswordMessage");
            flush = true;
            
            String password = readString();
            if (!this.authMethod.equals(password)) {
                sendErrorAuth();
                break;
            }
            this.authMethod = null;
            
            try {
                SQLiteConnection conn = server.newSQLiteConnection(this.databaseName);
                this.session.setConnection(conn);
                sendAuthenticationOk();
            } catch (SQLException cause) {
                stop();
                server.traceError(log, "Can't init database", cause);
            }
            break;
        }
        case 'P': {
            server.trace(log, "Parse");
            Prepared p = new Prepared();
            p.name = readString();
            p.sql = getSQL(readString());
            server.trace(log, "name {}, SQL {}", p.name, p.sql);
            int paramTypesCount = readShort();
            int[] paramTypes = null;
            if (paramTypesCount > 0) {
                paramTypes = new int[paramTypesCount];
                for (int i = 0; i < paramTypesCount; i++) {
                    paramTypes[i] = readInt();
                }
            }
            try {
                p.prep = getConnection().prepareStatement(p.sql);
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
                prepared.put(p.name, p);
                sendParseComplete();
            } catch (SQLException e) {
                sendErrorResponse(e);
                flush = true;
            }
            break;
        }
        case 'B': {
            server.trace(log, "Bind");
            Portal portal = new Portal();
            portal.name = readString();
            String prepName = readString();
            Prepared prep = prepared.get(prepName);
            if (prep == null) {
                sendErrorResponse("Prepared not found");
                flush = true;
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
                flush = true;
                break;
            }
            int resultCodeCount = readShort();
            portal.resultColumnFormat = new int[resultCodeCount];
            for (int i = 0; i < resultCodeCount; i++) {
                portal.resultColumnFormat[i] = readShort();
            }
            sendBindComplete();
            break;
        }
        case 'C': {
            server.trace(log, "Close");
            flush = true;
            char type = (char) readByte();
            String name = readString();
            if (type == 'S') {
                Prepared p = prepared.remove(name);
                if (p != null) {
                    IoUtils.close(p.prep);
                }
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
            char type = (char) readByte();
            String name = readString();
            if (type == 'S') {
                Prepared p = prepared.get(name);
                if (p == null) {
                    sendErrorResponse("Prepared not found: " + name);
                    flush = true;
                } else {
                    try {
                        sendParameterDescription(p.prep.getParameterMetaData(), p.paramType);
                        sendRowDescription(p.prep.getMetaData());
                    } catch (SQLException e) {
                        sendErrorResponse(e);
                        flush = true;
                    }
                }
            } else if (type == 'P') {
                Portal p = portals.get(name);
                if (p == null) {
                    sendErrorResponse("Portal not found: " + name);
                    flush = true;
                } else {
                    PreparedStatement prep = p.prep.prep;
                    try {
                        ResultSetMetaData meta = prep.getMetaData();
                        sendRowDescription(meta);
                    } catch (SQLException e) {
                        sendErrorResponse(e);
                        flush = true;
                    }
                }
            } else {
                server.trace(log, "expected S or P, got {}", type);
                sendErrorResponse("expected S or P");
                flush = true;
            }
            break;
        }
        case 'E': {
            server.trace(log, "Execute");
            String name = readString();
            Portal p = portals.get(name);
            if (p == null) {
                sendErrorResponse("Portal not found: " + name);
                flush = true;
                break;
            }
            int maxRows = readShort();
            Prepared prepared = p.prep;
            PreparedStatement prep = prepared.prep;
            server.trace(log, "execute SQL {}", prepared.sql);
            try {
                prep.setMaxRows(maxRows);
                String command = parseCommandType(prepared.sql);
                
                txTryBegin(command);
                boolean result = prep.execute();
                txTryFinish(command);
                
                if (result) {
                    try {
                        ResultSet rs = prep.getResultSet();
                        // the meta-data is sent in the prior 'Describe'
                        while (rs.next()) {
                            sendDataRow(rs, p.resultColumnFormat);
                        }
                        sendCommandComplete(command, prepared.sql, 0, result);
                    } catch (SQLException e) {
                        sendErrorResponse(e);
                        flush = true;
                    }
                } else {
                    int n = prep.getUpdateCount();
                    sendCommandComplete(command, prepared.sql, n, result);
                }
            } catch (SQLException e) {
                if (isCanceled(e)) {
                    sendCancelQueryResponse();
                } else {
                    sendErrorResponse(e);
                }
                flush = true;
            }
            break;
        }
        case 'S': {
            server.trace(log, "Sync");
            flush = true;
            sendReadyForQuery();
            break;
        }
        case 'Q': {
            server.trace(log, "Query");
            flush = true;
            String query = readString();
            try (SQLReader reader = new SQLReader(new StringReader(query))){
                Connection conn = getConnection();
                while (true) {
                    Statement stat = null;
                    String s = null;
                    try {
                        s = reader.readStatement();
                        if (s == null) {
                            break;
                        }
                        s = getSQL(s);
                        stat = conn.createStatement();
                        String command = parseCommandType(s);
                        
                        txTryBegin(command);
                        server.trace(log, "execute SQL {}", s);
                        boolean result = stat.execute(s);
                        txTryFinish(command);
                        
                        if (result) {
                            ResultSet rs = stat.getResultSet();
                            ResultSetMetaData meta = rs.getMetaData();
                            try {
                                sendRowDescription(meta);
                                while (rs.next()) {
                                    sendDataRow(rs, null);
                                }
                                sendCommandComplete(command, s, 0, result);
                            } catch (SQLException e) {
                                sendErrorResponse(e);
                                break;
                            }
                        } else {
                            int n = stat.getUpdateCount();
                            sendCommandComplete(command, s, n, result);
                        }
                    } catch (SQLException e) {
                        if (stat != null && isCanceled(e)) {
                            sendCancelQueryResponse();
                        } else {
                            sendErrorResponse(e);
                        }
                        break;
                    } finally {
                        IoUtils.close(stat);
                    }
                }
            }
            sendReadyForQuery();
            break;
        }
        case 'X': {
            server.trace(log, "Terminate");
            close();
            break;
        }
        default:
            server.trace(log, "Unsupported: {} ({})", x, (char) x);
            flush = true;
            break;
        }
        
        return flush;
    }
    
    private void txTryBegin(String command) {
        SQLiteConnection conn = getConnection();
        if ("BEGIN".equals(command)) {
            server.trace(log, "tx begin");
            conn.getConnectionConfig().setAutoCommit(false);
        }
    }
    
    private void txTryFinish(String command) {
        SQLiteConnection conn = getConnection();
        switch (command) {
        case "COMMIT":
        case "END":
        case "ROLLBACK":
            conn.getConnectionConfig().setAutoCommit(true);
            server.trace(log, "tx finish");
            break;
        }
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
    
    private String getSQL(String s) {
        server.trace(log, "sql {}", s);
        
        String lower = StringUtils.toLowerEnglish(s);
        if (lower.startsWith("show max_identifier_length")) {
            s = "select 63";
        } else if (lower.startsWith("set client_encoding to")) {
            s = "set DATESTYLE ISO";
        }
        
        return s;
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
        switch (getServer().getAuthMethod()) {
        case "password":
            sendAuthenticationCleartextPassword();
            break;
        default: // md5
            sendAuthenticationMD5Password();
            break;
        }
        getServer().trace(log, "authMethod {}", this.authMethod);
    }
    
    private void sendAuthenticationCleartextPassword() throws IOException {
        PgServer server = getServer();
        this.authMethod = new AuthPassword(server.getPassword());
        
        startMessage('R');
        writeInt(AUTH_REQ_PASSWORD);
        sendMessage();
    }
    
    private void sendAuthenticationMD5Password() throws IOException {
        PgServer server = getServer();
        MD5Password md5 = new MD5Password(server.getUser(), server.getPassword());
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
    
    private void sendCommandComplete(String command, String sql, int updateCount, boolean resultSet)
            throws IOException {
        startMessage('C');
        switch (command) {
        case "INSERT":
            writeStringPart("INSERT 0 ");
            writeString(Integer.toString(updateCount));
            break;
        case "UPDATE":
            writeStringPart("UPDATE ");
            writeString(Integer.toString(updateCount));
            break;
        case "DELETE":
            writeStringPart("DELETE ");
            writeString(Integer.toString(updateCount));
            break;
        case "SELECT":
        case "CALL":
            writeString("SELECT");
            break;
        case "BEGIN":
            writeString("BEGIN");
            break;
        default:
            server.trace(log, "check CommandComplete tag for command {}", sql);
            writeStringPart(resultSet? "SELECT": "UPDATE ");
            writeString(Integer.toString(updateCount));
        }
        sendMessage();
    }
    
    private void sendErrorAuth() throws IOException {
        SQLiteErrorCode error = SQLiteErrorCode.SQLITE_AUTH;
        sendErrorResponse(new SQLException(error.message, "28000", error.code));
        stop();
    }
    
    private void sendErrorResponse(SQLException e) throws IOException {
        server.traceError(log, "send error", e);
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
        int len = buff.length;
        this.dataOut = new DataOutputStream(this.out);
        this.dataOut.write(this.messageType);
        this.dataOut.writeInt(len + 4);
        this.dataOut.write(buff);
    }
    
    private void sendNoData() throws IOException {
        startMessage('n');
        sendMessage();
    }
    
    private void sendParameterDescription(ParameterMetaData meta, int[] paramTypes) 
            throws IOException, SQLException {
        int count = meta.getParameterCount();
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
            if (conn.getAutoCommit()) {
                // idle
                c = 'I';
            } else {
                // in a transaction block
                c = 'T';
            }
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
    
    private void startMessage(int newMessageType) {
        this.messageType = newMessageType;
        this.outBuf = new ByteArrayOutputStream();
        this.dataOut = new DataOutputStream(this.outBuf);
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
    
    private String parseCommandType(String sql) {
        String command;
        int i = sql.indexOf(' ');
        if (i == -1) {
            if (sql.endsWith(";")) {
                sql = sql.substring(0, sql.length() - 1);
            }
            command = sql;
        } else {
            command = sql.substring(0, i);
        }
        command = StringUtils.toUpperEnglish(command);
        
        getServer().trace(log, "command {}", command);
        return command;
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
        String sql;

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
