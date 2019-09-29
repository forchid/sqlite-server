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
package org.sqlite.sql;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.util.IoUtils;

/**SQL statement.
 * 
 * @author little-pan
 * @since 2019-09-04
 *
 */
public class SQLStatement implements AutoCloseable {
    static final Logger log = LoggerFactory.getLogger(SQLStatement.class);
    
    protected final String command;
    protected final String sql;
    
    protected SQLContext context;
    protected Statement jdbcStatement;
    protected boolean prepared;
    private boolean open = true;
    protected boolean implicitTx;
    
    protected boolean query;
    protected boolean comment;
    protected boolean empty;
    
    public SQLStatement(String sql) {
        this(sql, "");
        this.empty = true;
    }
    
    public SQLStatement(String sql, String command){
        this.sql = sql;
        this.command = command;
    }
    
    public SQLStatement(String sql, String command, boolean query){
        this.sql = sql;
        this.command = command;
        this.query = query;
    }
    
    public String getSQL() {
        return this.sql;
    }
    
    public String getExecutableSQL() throws SQLException {
        return getSQL();
    }
    
    public String getCommand() {
        return this.command;
    }
    
    public boolean isQuery() {
        return this.query;
    }
    
    public void setQuery(boolean query) {
        this.query = query;
    }
    
    public boolean isTransaction() {
        return false;
    }
    
    public boolean isComment() {
        return this.comment;
    }
    
    public void setComment(boolean comment) {
        this.comment = comment;
    }
    
    public SQLContext getContext() {
        return this.context;
    }
    
    public void setContext(SQLContext context) {
        this.context = context;
    }
    
    public boolean isEmpty() {
        return this.empty;
    }
    
    public void setEmpty(boolean empty) {
        this.empty = empty;
    }
    
    public boolean isPrepared() {
        return this.prepared;
    }
    
    public void setPrepared(boolean prepared) {
        this.prepared = prepared;
    }
    
    public PreparedStatement prepare() throws SQLException, IllegalStateException {
        if (this.jdbcStatement != null) {
            throw new IllegalStateException(this.command + " statement prepared");
        }
        if (this.empty) {
            throw new IllegalStateException("Empty statement can't be prepared");
        }
        checkPermission();
        this.context.checkReadOnly(this);
        
        Connection conn = this.context.getConnection();
        String sql = getExecutableSQL();
        PreparedStatement ps = conn.prepareStatement(sql);
        this.jdbcStatement = ps;
        this.prepared = true;
        return ps;
    }
    
    protected Statement getJdbcStatement() {
        return this.jdbcStatement;
    }
    
    public PreparedStatement getPreparedStatement() {
        return (PreparedStatement)this.jdbcStatement;
    }
    
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return this.getPreparedStatement().getParameterMetaData();
    }
    
    public ResultSetMetaData getPreparedMetaData() throws SQLException {
        return this.getPreparedStatement().getMetaData();
    }
    
    public ResultSet getResultSet() throws SQLException {
        return this.getJdbcStatement().getResultSet();
    }
    
    public int getUpdateCount() throws SQLException {
        return this.getJdbcStatement().getUpdateCount();
    }
    
    protected void checkPermission() throws SQLException {
        if (this.isEmpty()) {
            return;
        }
        
        this.context.checkPermission(this);
    }
    
    public void preExecute(int maxRows) throws SQLException, IllegalStateException {
        if (this.prepared) {
            PreparedStatement ps = getPreparedStatement();
            ps.setMaxRows(maxRows);
        } else {
            if (this.jdbcStatement == null) {
                checkPermission();
                this.context.checkReadOnly(this);
                Connection conn = this.context.getConnection();
                this.jdbcStatement = conn.createStatement();
            }
        }
    }
    
    public boolean execute(int maxRows) throws SQLException, IllegalStateException {
        if (!this.open) {
            throw new IllegalStateException(this.command + " statement closed");
        }
        
        preExecute(maxRows);
        boolean rs = doExecute(maxRows);
        postExecute(rs);
        
        return rs;
    }
    
    protected boolean doExecute(int maxRows) throws SQLException {
        SQLContext context = this.context;
        boolean resultSet;
        
        final boolean autoCommit = context.isAutoCommit();
        context.trace(log, "tx: autoCommit {} ->", autoCommit);
        
        final boolean writable = !isQuery() && !context.isReadOnly();
        if (writable && !context.holdsDbWriteLock()) {
            context.dbWriteLock();
        }
        
        context.trace(log, "execute sql \"{}\"", this);
        if (this.prepared) {
            // Execute batch prepared statement in an explicit transaction for ACID
            if (autoCommit && writable && !this.implicitTx) {
                execute("begin immediate");
                this.implicitTx = true;
                context.trace(log, "tx: begin an implicit transaction");
            }
            PreparedStatement ps = getPreparedStatement();
            resultSet = ps.execute();
        } else {
            String sql = getExecutableSQL();
            resultSet = this.jdbcStatement.execute(sql);
        }
        
        return resultSet;
    }
    
    protected void postExecute(boolean resultSet) throws SQLException {
        
    }
    
    /**
     * Cleanup work after handling result of SQL execution
     * @throws IllegalStateException if SQL context closed or access metaDb error
     */
    public void postResult() throws IllegalStateException {
        if (this.prepared) {
            return;
        }
        
        complete(true);
    }
    
    /**Cleanup work after the whole execution of this statement complete
     * 
     * @param success the execution of this statement is successful or not
     * @throws IllegalStateException if SQL context closed or access metaDb error etc
     */
    public void complete(boolean success) throws IllegalStateException {
        SQLContext context = this.context;
        final boolean autoCommit = context.isAutoCommit();
        context.trace(log, "tx: autoCommit {} <-", autoCommit);
        
        if (this.implicitTx) {
            assert autoCommit;
            try {
                if (success) {
                    execute("commit");
                    context.trace(log, "tx: commit an implicit transaction");
                } else {
                    execute("rollback");
                    context.trace(log, "tx: rollback an implicit transaction");
                }
                this.implicitTx = false;
            } catch (SQLException e) {
                throw new IllegalStateException("Can't complete an implicit transaction", e);
            }
        }
        
        if (autoCommit) {
            context.dbWriteUnlock();
            context.transactionComplelete();
        }
    }
    
    /**
     * Handle SQL execution exception
     * @param e SQLException when SQL execution
     * @return true if the execution exception handled, otherwise false
     */
    public boolean executionException(SQLException e) {
        return false;
    }
    
    public boolean isOpen() {
        return this.open;
    }
    
    @Override
    public String toString() {
        return this.sql;
    }

    @Override
    public void close() {
        IoUtils.close(jdbcStatement);
        this.jdbcStatement = null;
        this.context = null;
        this.open = false;
    }

    protected boolean execute(String sql) throws SQLException {
        Connection conn = this.context.getConnection();
        Statement s = conn.createStatement();
        boolean resultSet = s.execute(sql);
        s.close();
        return resultSet;
    }
    
    protected int executeUpdate(String sql) throws SQLException {
        Connection conn = this.context.getConnection();
        Statement s = conn.createStatement();
        s.execute(sql);
        int n = s.getUpdateCount();
        s.close();
        return n;
    }
    
    protected ResultSet executeQuery(String sql) throws SQLException {
        Connection conn = this.context.getConnection();
        Statement s = conn.createStatement();
        s.execute(sql);
        return s.getResultSet();
    }
    
}
