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
package org.sqlite.server.pg.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.sql.InsertSelectStatement;
import org.sqlite.sql.SQLContext;
import org.sqlite.sql.SQLParseException;
import org.sqlite.sql.SQLParser;
import org.sqlite.sql.SQLStatement;
import org.sqlite.sql.Transaction;
import org.sqlite.util.IoUtils;

import static org.sqlite.util.ConvertUtils.*;

/** Support the "INSERT INTO... {VALUES()... | SELECT ...} RETURNING ..." statement of PostgreSQL style.
 * The execution process is: <p>
 * 1. [begin immediate] if not in transaction <br>
 * 2. lowID: select max(_rowid_) from TABLE, maybe NULL(note that "select last_insert_rowid()" 
 *is inappropriate for empty table or "insert into ... select ...") <br>
 * 3. INSERT rows in VALUES clause <br>
 * 4. highID: select last_insert_rowid() if there are rows inserted <br>
 * 5. select RETURNING-columns where (_rowid_ between lowID and highID) or (lowID is NULL and _rowid_ <= highID) <br>
 * 6. [commit] if not in transaction <br>
 * </p>
 * <p>
 * <b>Note</b>: The behavior is undefined if specify both ID and returning.
 * </p>
 * 
 * @author little-pan
 * @since 2019-09-28
 *
 */
public class InsertReturningStatement extends InsertSelectStatement {
    static Logger log = LoggerFactory.getLogger(InsertReturningStatement.class);
    
    static final int INIT = 0, TX_BEGIN = 1, SELECT_LOWID = 2, INSERT_ROWS = 3, 
            SELECT_HIGHID = 4, SELECT_RETURNING = 5, TX_END = 6;
    
    private int step = INIT;
    protected boolean resultSet;
    protected Long lowId;
    protected Long highId;
    
    protected String schemaName;
    protected String tableName;
    protected String returningColumns;
    protected SQLStatement returningSelect;
    
    protected SQLStatement maxRowidSelect;
    protected SQLStatement lastRowidSelect;
    
    public InsertReturningStatement(String sql) {
        super(sql);
    }
    
    @Override
    public PreparedStatement prepare() throws SQLException, IllegalStateException {
        PreparedStatement ps = super.prepare();
        prepareReturningSelects();
        return ps;
    }
    
    @Override
    public void preExecute(int maxRows) throws SQLException, IllegalStateException {
        super.preExecute(maxRows);
        
        if (!this.prepared) {
            prepareReturningSelects();
            return;
        }
    }
    
    @Override
    protected boolean doExecute(int maxRows) throws SQLException {
        SQLContext context = this.context;
        PreparedStatement ps;
        ResultSet rs;
        int rows = 0;
        
        switch (this.step) {
        case INIT:
            this.resultSet = false;
            this.lowId = this.highId = null;
            boolean autoCommit = context.isAutoCommit();
            if (autoCommit && !inImplicitTx()) {
                this.step = TX_BEGIN;
                context.trace(log, "Step. INIT -> TX_BEGIN: autoCommit = {}", autoCommit);
            } else {
                this.step = SELECT_LOWID;
                context.trace(log, "Step. INIT -> SELECT_LOWID: autoCommit = {}", autoCommit);
            }
        case TX_BEGIN:
            if (TX_BEGIN == this.step) {
                // Transaction control for multiple-step operations
                assert context.isAutoCommit() && !inImplicitTx();
                context.dbWriteLock();
                super.execute("begin immediate");
                context.setTransaction(new Transaction(context, true));
                this.step = SELECT_LOWID;
                context.trace(log, "Step. TX_BEGIN -> SELECT_LOWID: start an implicit tx");
            }
        case SELECT_LOWID:
            this.maxRowidSelect.execute(1);
            rs = this.maxRowidSelect.getResultSet();
            rs.next();
            this.lowId = rs.getLong(1) + 1L;
            if (this.lowId < 1L) {
                throw convertError(SQLiteErrorCode.SQLITE_FULL);
            }
            if (rs.wasNull()) {
                this.lowId = null;
            }
            rs.close();
            this.step = INSERT_ROWS;
            context.trace(log, "Step. SELECT_LOWID -> INSERT_ROWS: lowId = {}", this.lowId);
        case INSERT_ROWS:
            super.doExecute(maxRows);
            rows = getUpdateCount();
            if (rows > 0) {
                this.step = SELECT_HIGHID;
                context.trace(log, "Step. INSERT_ROWS -> SELECT_HIGHID: lowId = {}, updateCount = {}", 
                        this.lowId, rows);
            } else {
                this.step = SELECT_RETURNING;
                context.trace(log, "Step. INSERT_ROWS -> SELECT_RETURNING: lowId = {}, updateCount = {}", 
                        this.lowId, rows);
            }
        case SELECT_HIGHID:
            if (SELECT_HIGHID == this.step) {
                this.lastRowidSelect.execute(1);
                rs = this.lastRowidSelect.getResultSet();
                rs.next();
                this.highId = rs.getLong(1);
                rs.close();
                if (this.lowId != null && this.highId < this.lowId) {
                    throw convertError(SQLiteErrorCode.SQLITE_FULL);
                }
            }
            this.step = SELECT_RETURNING;
            context.trace(log, "Step. SELECT_HIGHID -> SELECT_RETURNING: lowId = {}, highId = {}", 
                    this.lowId, this.highId);
        case SELECT_RETURNING:
            ps = this.returningSelect.getPreparedStatement();
            // Set parameters
            if (this.lowId == null) {
                ps.setNull(1, Types.BIGINT);
                ps.setNull(3, Types.BIGINT);
            } else {
                long lowId = this.lowId;
                ps.setLong(1, lowId);
                ps.setLong(3, lowId);
            }
            if (this.highId == null) {
                ps.setNull(2, Types.BIGINT);
                ps.setNull(4, Types.BIGINT);
            } else {
                long highId = this.highId;
                ps.setLong(2, highId);
                ps.setLong(4, highId);
            }
            rows = getUpdateCount();
            // Do execute
            this.resultSet = this.returningSelect.execute(rows);
            if (inImplicitTx()) {
                this.step = TX_END; // called until complete()
                context.trace(log, "Step. SELECT_RETURNING -> TX_END: lowId = {}, highId = {}, resultSet = {}", 
                        this.lowId, this.highId, this.resultSet);
            } else {
                this.step = INIT;
                context.trace(log, "Step. SELECT_RETURNING -> INIT: lowId = {}, highId = {}, resultSet = {}", 
                        this.lowId, this.highId, this.resultSet);
            }
            return this.resultSet;
        default:
            throw new IllegalStateException("Unknown step: " + this.step);
        }
    }
    
    @Override
    public void complete(boolean success) throws IllegalStateException {
        if (inImplicitTx()) {
            if (success && this.step != TX_END) {
                throw new IllegalStateException("step not the step TX_END: " + this.step);
            }
            super.complete(success);
            this.step = INIT;
            this.context.trace(log, "Step. TX_END -> INIT: lowId = {}, highId = {}, resultSet = {}", 
                    this.lowId, this.highId, this.resultSet);
        } else {
            if (success && this.step != INIT) {
                throw new IllegalStateException("step not the step INIT: " + this.step);
            }
            super.complete(success);
        }
    }
    
    protected void prepareReturningSelects() throws SQLException {
        if (this.returningColumns != null && this.returningSelect == null) {
            this.returningSelect = getReturningSelectSQL();
            this.returningSelect.setContext(getContext());
            this.returningSelect.prepare();
            
            this.maxRowidSelect = getMaxRowidSelectSQL();
            this.maxRowidSelect.setContext(getContext());
            this.maxRowidSelect.prepare();
            
            String sql = "select last_insert_rowid()";
            this.lastRowidSelect = new SQLStatement(sql, "SELECT", true);
            this.lastRowidSelect.setContext(getContext());
            this.lastRowidSelect.prepare();
        }
    }
    
    protected SQLStatement getMaxRowidSelectSQL() throws SQLParseException {
        String sql, f;
        if (this.schemaName == null) {
            f = "select max(_rowid_) from '%s'";
            sql = String.format(f, this.tableName);
        } else {
            f = "select max(_rowid_) from '%s'.'%s'";
            sql = String.format(f, this.schemaName, this.tableName);
        }
        // check
        try (SQLParser parser = new SQLParser(sql, true)) {
            SQLStatement stmt = parser.next();
            if ("SELECT".equals(stmt.getCommand()) && !parser.hasNext()) {
                return stmt;
            }
        }
        
        if (this.schemaName == null) {
            throw new SQLParseException(this.tableName);
        }
        throw new SQLParseException(this.schemaName + "." + this.tableName);
    }
    
    protected SQLStatement getReturningSelectSQL() throws SQLParseException {
        if (this.returningColumns == null) {
            throw new SQLParseException("No returning columns");
        }
        
        String f = "select %s from '%s' where (_rowid_ between ? and ?) or (? is null and _rowid_ <= ?)";
        String sql;
        if (this.schemaName == null) {
            sql = String.format(f, this.returningColumns, this.tableName);
        } else {
            String schemaSQL = String.format("'%s'.", this.schemaName);
            sql = String.format(f, this.returningColumns, schemaSQL, this.tableName);
        }
        try (SQLParser parser = new SQLParser(sql, true)) {
            SQLStatement stmt = parser.next();
            if ("SELECT".equals(stmt.getCommand()) && !parser.hasNext()) {
                return stmt;
            }
        }
        
        throw new SQLParseException(this.returningColumns);
    }
    
    @Override
    public ResultSet getResultSet() throws SQLException {
        if (this.resultSet) {
            return this.returningSelect.getResultSet();
        }
        
        throw convertError(SQLiteErrorCode.SQLITE_INTERNAL);
    }
    
    @Override
    public ResultSetMetaData getPreparedMetaData() throws SQLException {
        if (this.prepared) {
            return this.returningSelect.getPreparedMetaData();
        }
        
        throw convertError(SQLiteErrorCode.SQLITE_INTERNAL);
    }
    
    @Override
    public void close() {
        IoUtils.close(this.maxRowidSelect);
        IoUtils.close(this.lastRowidSelect);
        IoUtils.close(this.returningSelect);
        super.close();
    }
    
    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getReturningColumns() {
        return returningColumns;
    }

    public void setReturningColumns(String returningColumns) {
        this.returningColumns = returningColumns;
    }
    
}
