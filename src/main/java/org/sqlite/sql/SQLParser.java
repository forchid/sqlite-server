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

import static java.lang.Character.*;

import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.sqlite.server.MetaStatement;
import org.sqlite.server.sql.SelectSleepStatement;
import org.sqlite.server.sql.meta.AlterUserStatement;
import org.sqlite.server.sql.meta.CreateDatabaseStatement;
import org.sqlite.server.sql.meta.CreateUserStatement;
import org.sqlite.server.sql.meta.DropDatabaseStatement;
import org.sqlite.server.sql.meta.DropUserStatement;
import org.sqlite.server.sql.meta.GrantStatement;
import org.sqlite.server.sql.meta.RevokeStatement;
import org.sqlite.server.sql.meta.ShowDatabasesStatement;
import org.sqlite.server.sql.meta.ShowGrantsStatement;
import org.sqlite.util.IoUtils;
import org.sqlite.util.StringUtils;

/**A simple SQL parser.
 * 
 * @author little-pan
 * @since 2019-09-04
 *
 */
public class SQLParser implements Iterator<SQLStatement>, Iterable<SQLStatement>, AutoCloseable {
    
    // Protocol -> authentication method list
    static final Map<String, String[]> authMethods;
    
    static {
        Map<String, String[]> methods = new HashMap<>();
        methods.put("pg", new String[]{"md5", "password", "trust"});
        authMethods = Collections.unmodifiableMap(methods);
    }
    
    protected final SQLReader reader;
    protected int bi, ei;
    protected String sql;
    
    private boolean nextCalled;
    
    public SQLParser(String sqls) {
        this(sqls, false);
    }
    
    public SQLParser(String sqls, boolean ignoreNestedBlockComment) {
        this.reader = new SQLReader(sqls, ignoreNestedBlockComment);
    }
    
    public SQLParser(Reader reader) {
        this(reader, false);
    }
    
    public SQLParser(Reader reader, boolean ignoreNestedBlockComment) {
        this.reader = new SQLReader(reader, ignoreNestedBlockComment);
    }
    
    @Override
    public Iterator<SQLStatement> iterator() {
        return this;
    }
    
    @Override
    public boolean hasNext() {
        if (this.sql == null) {
            this.sql = this.reader.readStatement();
        }
        return (this.sql != null);
    }

    @Override
    public SQLStatement next() {
        SQLStatement stmt = parse();
        this.nextCalled = true;
        if (stmt == null) {
            throw new NoSuchElementException();
        }
        
        return stmt;
    }

    @Override
    public void remove() {
        if (this.nextCalled) {
            this.nextCalled = false;
            return;
        }
        
        throw new IllegalStateException();
    }
    
    public boolean isOpen () {
        return this.reader.isOpen();
    }
    
    public boolean isIgnoreNestedBlockComment() {
        return this.reader.isIgnoreNestedBlockComment();
    }
    
    @Override
    public void close() {
        try {
            IoUtils.close(this.reader);
        } finally {
            reset();
        }
    }
    
    protected void reset() {
        this.bi = 0;
        this.ei = 0;
        this.sql = null;
    }
    
    protected SQLStatement parse() {
        try {
            if(hasNext()) {
               return parseStatement();
            }
            
            return null;
        } finally {
            reset();
        }
    }
    
    protected SQLStatement parseStatement() {
        String s = this.sql;
        int len = s.length();
        
        skipSpacesIf();
        if ((this.ei == len) || 
                (this.ei + 1 == len && ';' == s.charAt(this.ei))) {
            return new SQLStatement(s);
        }
        
        this.bi = this.ei;
        for (;;) {
            char c = s.charAt(this.ei++);
            switch (c) {
            case '-':
                nextChar('-');
                return parseLineComment();
            case '/':
                nextChar('*');
                return parseBlockComment();
            case 'a':
            case 'A':
                c = nextChar();
                if ('l' == c || 'L' == c) {
                    return parseAlter();
                }
                if ('n' == c || 'N' == c) {
                    return parseAnalyze();
                }
                if ('t' == c || 'T' == c) {
                    return parseAttach();
                }
                throw syntaxError();
            case 'b':
            case 'B':
                return parseBegin(false);
            case 'c':
            case 'C':
                c = nextChar();
                if ('o' == c || 'O' == c) {
                    return parseCommit();
                }
                if ('r' == c || 'R' == c) {
                    return parseCreate();
                }
                throw syntaxError();
            case 'd':
            case 'D':
                c = nextChar();
                if ('e' == c || 'E' == c) {
                    c = nextChar();
                    if ('l' == c || 'L' == c) {
                        return parseDelete();
                    }
                    if ('t' == c || 'T' == c) {
                        return parseDetach();
                    }
                    throw syntaxError();
                }
                if ('r' == c || 'R' == c) {
                    return parseDrop();
                }
                throw syntaxError();
            case 'e':
            case 'E':
                c = nextChar();
                if ('n' == c || 'N' == c) {
                    return parseEnd();
                }
                if ('x' == c || 'X' == c) {
                    return parseExplain();
                }
                throw syntaxError();
            case 'g':
            case 'G':
                return parseGrant();
            case 'i':
            case 'I':
                return parseInsert();
            case 'p':
            case 'P':
                return parsePragma();
            case 'r':
            case 'R':
                c = nextChar();
                if ('e' == c || 'E' == c) {
                    c = nextChar();
                    if ('i' == c || 'I' == c) {
                        return parseReindex();
                    }
                    if ('l' == c || 'L' == c) {
                        return parseRelease();
                    }
                    if ('p' == c || 'P' == c) {
                        return parseReplace();
                    }
                    if ('v' == c || 'V' == c) {
                        return parseRevoke();
                    }
                    throw syntaxError();
                }
                if ('o' == c || 'O' == c) {
                    return parseRollback();
                }
                throw syntaxError();
            case 's':
            case 'S':
                c = nextChar();
                switch (c) {
                case 'a':
                case 'A':
                    return parseSavepoint();
                case 'e':
                case 'E':
                    return parseSelect();
                case 'h':
                case 'H':
                    return parseShow();
                case 't':
                case 'T':
                    return parseBegin(true);
                default:
                    throw syntaxError();
                }
            case 'u':
            case 'U':
                return parseUpdate();
            case 'v':
            case 'V':
                return parseVacuum();
            // error!
            default:
                throw syntaxError();
            }
        }
    }
    
    protected SQLStatement parseAlter() {
        nextString("ter");
        skipIgnorable();
        if (nextStringIf("user") != -1) {
            skipIgnorable();
            return parseAlterUser();
        }
        return new SQLStatement(this.sql, "ALTER");
    }
    
    protected SQLStatement parseAlterUser() {
        AlterUserStatement stmt = new AlterUserStatement(this.sql);
        boolean failed = true;
        try {
            stmt.setUser(nextString());
            skipIgnorableIf();
            nextChar('@');
            skipIgnorableIf();
            stmt.setHost(nextString());
            
            skipIgnorable();
            if (nextStringIf("with") != -1) {
                skipIgnorable();
            }
            
            for (;;) {
                if (nextStringIf("superuser") != -1) {
                    stmt.setSa(true);
                    if (skipIgnorableIf() != -1) {
                        continue;
                    }
                    if (nextEnd()) {
                        break;
                    }
                    stmt.close();
                    throw syntaxError();
                }
                if (nextStringIf("nosuperuser") != -1) {
                    stmt.setSa(false);
                    if (skipIgnorableIf() != -1) {
                        continue;
                    }
                    if (nextEnd()) {
                        break;
                    }
                    stmt.close();
                    throw syntaxError();
                }
                if (nextStringIf("identified") != -1) {
                    skipIgnorable();
                    if (nextStringIf("by") != -1) {
                        skipIgnorable();
                        stmt.setPassword(nextString());
                        if (skipIgnorableIf() != -1) {
                            continue;
                        }
                        if (nextEnd()) {
                            break;
                        }
                        stmt.close();
                        throw syntaxError();
                    } else {
                        nextString("with");
                        skipIgnorable();
                        String proto = "pg";
                        nextString(proto);
                        stmt.setProtocol(proto);
                        if (skipIgnorableIf() == -1 || nextEnd()) {
                            break;
                        }
                        for (String auth: authMethods.get(proto)) {
                            if (nextStringIf(auth) != -1) {
                                stmt.setAuthMethod(auth);
                                if (skipIgnorableIf() != -1 || nextEnd()) {
                                    break;
                                }
                                stmt.close();
                                throw syntaxError();
                            }
                        }
                        continue;
                    }
                }
                
                if (nextEnd()) {
                    break;
                }
                
                stmt.close();
                throw syntaxError();
            }
            
            if (stmt.getPassword() != null && !"pg".equals(stmt.getProtocol())) {
                throw syntaxError("Unknown auth protocol: " + stmt.getProtocol());
            }
            
            failed = false;
            return stmt;
        } finally {
            if (failed) {
                stmt.close();
            }
        }
    }
    
    protected SQLStatement parseAnalyze() {
        nextString("alyze");
        return new SQLStatement(this.sql, "ANALYZE");
    }
    
    protected SQLStatement parseBegin(boolean standard) {
        nextString(standard? "art": "egin");
        TransactionStatement stmt = new TransactionStatement(this.sql, "BEGIN");
        if (nextEnd()) {
            if (standard) {
                stmt.close();
                throw syntaxError("No \"transaction\" specified after \"start\"");
            }
            stmt.setDeferred(true);
        } else {
            // Parse: [deferred | immediate | exclusive] [work | transaction]
            boolean txBehaviorSet = false, txOccurs = false, next = true;
            for (; next; ) {
                char c = nextChar();
                switch (c) {
                case 't':
                case 'T':
                    nextString("ransaction");
                    txOccurs = true;
                    next = false;
                    break;
                case 'w':
                case 'W':
                    nextString("ork");
                    next = false;
                    break;
                case 'd':
                case 'D':
                    if (txBehaviorSet) {
                        stmt.close();
                        throw syntaxError();
                    }
                    nextString("eferred");
                    stmt.setDeferred(true);
                    txBehaviorSet = true;
                    break;
                case 'e':
                case 'E':
                    if (txBehaviorSet) {
                        stmt.close();
                        throw syntaxError();
                    }
                    nextString("xclusive");
                    stmt.setExclusive(true);
                    txBehaviorSet = true;
                    break;
                case 'i':
                case 'I':
                    if (txBehaviorSet) {
                        stmt.close();
                        throw syntaxError();
                    }
                    nextString("mmediate");
                    stmt.setImmediate(true);
                    txBehaviorSet = true;
                    break;
                default:
                    backChar();
                    next = false;
                    break;
                }
                
                if (next) next = !nextEnd();
            }
            if (standard && !txOccurs) {
                throw syntaxError("No \"transaction\" specified after \"start\"");
            }
            if (!txBehaviorSet) {
                stmt.setDeferred(true);
            }
            // Parse: transaction mode
            if (!nextEnd()) {
                for (;;) {
                    char c = nextChar();
                    switch (c) {
                    case 'i':
                    case 'I':
                        // Parse: isolation level
                        nextString("solation");
                        skipIgnorable();
                        nextString("level");
                        skipIgnorable();
                        c = nextChar();
                        switch (c) {
                        case 'r':
                        case 'R':
                            if (nextStringIf("ead") != -1) {
                                skipIgnorable();
                                if (nextStringIf("uncommitted") != -1) {
                                    stmt.setIsolationLevel(TransactionStatement.READ_UNCOMMITTED);
                                    break;
                                } else if (nextStringIf("committed") != -1) {
                                    stmt.setIsolationLevel(TransactionStatement.READ_COMMITTED);
                                    break;
                                }
                            } else if (nextStringIf("epeatable") != -1) {
                                skipIgnorable();
                                if (nextStringIf("read") != -1) {
                                    stmt.setIsolationLevel(TransactionStatement.REPEATABLE_READ);
                                    break;
                                }
                            }
                            stmt.close();
                            throw syntaxError();
                        case 's':
                        case 'S':
                            nextString("erializable");
                            stmt.setIsolationLevel(TransactionStatement.SERIALIZABLE);
                            break;
                        default:
                            stmt.close();
                            throw syntaxError(); 
                        }
                        break;
                    case 'r':
                    case 'R':
                        // Parse: read only | read write
                        nextString("ead");
                        skipIgnorable();
                        if (nextStringIf("only") != -1) {
                            stmt.setReadOnly(true);
                            break;
                        } else if (nextStringIf("write") != -1) {
                            stmt.setReadOnly(false);
                            break;
                        }
                        stmt.close();
                        throw syntaxError(); 
                    default:
                        stmt.close();
                        throw syntaxError(); 
                    }
                    
                    if (nextEnd()) {
                        break;
                    }
                    if (nextCharIf(',') != -1) {
                        skipIgnorableIf();
                        continue;
                    }
                    stmt.close();
                    throw syntaxError(); 
                } // for
            }
        }
        
        return stmt;
    }

    protected SQLStatement parseAttach() {
        String dbName, schemaName;
        
        nextString("tach");
        
        skipIgnorable();
        if (nextStringIf("database") != -1) {
            skipIgnorable();
        }
        dbName = nextString();
        
        skipIgnorable();
        nextString("as");
        
        skipIgnorable();
        schemaName = nextString();
        
        AttachStatement stmt = new AttachStatement(this.sql);
        stmt.setDbName(dbName);
        stmt.setSchemaName(schemaName);
        
        return stmt;
    }
    
    protected SQLStatement parseCommit() {
        nextString("mmit");
        return new TransactionStatement(this.sql, "COMMIT");
    }

    protected SQLStatement parseCreate() {
        nextString("eate");
        if (skipIgnorableIf() != -1) {
            if (nextStringIf("user") != -1) {
                skipIgnorable();
                return parseCreateUser();
            } else if (nextStringIf("database") != -1 
                    || nextStringIf("schema")   != -1) {
                skipIgnorable();
                return parseCreateDatabase();
            }
        }
        return new SQLStatement(this.sql, "CREATE");
    }
    
    protected SQLStatement parseCreateDatabase() {
        CreateDatabaseStatement stmt = new CreateDatabaseStatement(this.sql);
        if (nextStringIf("if") != -1) {
            skipIgnorable();
            nextString("not");
            skipIgnorable();
            nextString("exists");
            skipIgnorable();
            stmt.setQuite(true);
        }
        stmt.setDb(nextString());
        if (!nextEnd()) {
            if (nextStringIf("location") != -1 || nextStringIf("directory") != -1) {
                skipIgnorable();
                stmt.setDir(nextString());
                if (!nextEnd()) {
                    throw syntaxError(); 
                }
            } else {
                throw syntaxError();
            }
        }
        
        return stmt;
    }
    
    protected SQLStatement parseCreateUser() {
        CreateUserStatement stmt = new CreateUserStatement(this.sql);
        boolean failed = true;
        try {
            stmt.setUser(nextString());
            skipIgnorableIf();
            nextChar('@');
            skipIgnorableIf();
            stmt.setHost(nextString());
            
            skipIgnorable();
            if (nextStringIf("with") != -1) {
                skipIgnorable();
            }
            for (;;) {
                if (nextStringIf("superuser") != -1) {
                    skipIgnorableIf();
                    stmt.setSa(true);
                    continue;
                }
                if (nextStringIf("nosuperuser") != -1) {
                    skipIgnorableIf();
                    stmt.setSa(false);
                    continue;
                }
                if (nextStringIf("identified") != -1) {
                    skipIgnorable();
                    if (nextStringIf("by") != -1) {
                        skipIgnorable();
                        stmt.setPassword(nextString());
                    } else {
                        nextString("with");
                        skipIgnorable();
                        String proto = "pg";
                        nextString(proto);
                        stmt.setProtocol(proto);
                        skipIgnorable();
                        // authentication method
                        boolean hasAuth = false;
                        for (String auth: authMethods.get(proto)) {
                            if (nextStringIf(auth) != -1) {
                                stmt.setAuthMethod(auth);
                                hasAuth = true;
                                break;
                            }
                        }
                        if (!hasAuth) {
                            stmt.close();
                            throw syntaxError();
                        }
                    }
                    if (skipIgnorableIf() != -1) {
                        continue;
                    }
                    if (nextEnd()) {
                        break;
                    }
                    stmt.close();
                    throw syntaxError();
                }
                
                if (nextEnd()) {
                    break;
                }
                stmt.close();
                throw syntaxError();
            }
            
            // check
            if (stmt.getPassword() == null && !"trust".equals(stmt.getAuthMethod())) {
                String proto = stmt.getProtocol(), auth = stmt.getAuthMethod();
                throw syntaxError("No password when identified with %s %s", proto, auth);
            }
            
            failed = false;
            return stmt;
        } finally {
            if (failed) {
                stmt.close();
            }
        }
    }
    
    protected SQLStatement parseDrop() {
        nextString("op");
        skipIgnorable();
        if (nextStringIf("user") != -1) {
            skipIgnorable();
            return parseDropUser();
        } else if (nextStringIf("database") != -1 || nextStringIf("schema") != -1) {
            skipIgnorable();
            return parseDropDatabase();
        }
        
        return new SQLStatement(this.sql, "DROP");
    }
    
    protected SQLStatement parseDropDatabase() {
        DropDatabaseStatement stmt = new DropDatabaseStatement(this.sql);
        boolean failed = true;
        try {
            if (nextStringIf("if") != -1) {
                skipIgnorable();
                nextString("exists");
                skipIgnorable();
                stmt.setQuiet(true);
            }
            stmt.setDb(nextString());
            if (!nextEnd()) {
                throw syntaxError();
            }
            
            failed = false;
            return stmt;
        } finally {
            if (failed) {
                stmt.close();
            }
        }
    }
    
    protected SQLStatement parseDropUser() {
        DropUserStatement stmt = new DropUserStatement(this.sql);
        for (;;) {
            String user = nextString();
            skipIgnorableIf();
            nextChar('@');
            skipIgnorableIf();
            String host = nextString();
            String proto= MetaStatement.PROTO_DEFAULT;
            if (skipIgnorableIf() != -1 && nextStringIf("identified") != -1) {
                skipIgnorable();
                nextString("with");
                skipIgnorable();
                proto = StringUtils.toLowerEnglish(nextString());
            }
            stmt.addUser(host, user, proto);
            if (!nextEnd()) {
                nextChar(',');
                skipIgnorableIf();
                continue;
            }
            
            break;
        }
        
        return stmt;
    }

    protected SQLStatement parseDetach() {
        DetachStatement stmt;
        nextString("ach");
        
        skipIgnorable();
        if (nextStringIf("database") != -1) {
            skipIgnorable();
        }
        String schemaName = nextString();
        
        stmt = new DetachStatement(this.sql, "DETACH");
        stmt.setSchemaName(schemaName);
        return stmt;
    }

    protected SQLStatement parseDelete() {
        nextString("ete");
        return new SQLStatement(this.sql, "DELETE");
    }
    
    protected SQLStatement parseGrant() {
        nextString("rant");
        
        GrantStatement stmt = new GrantStatement(this.sql);
        skipIgnorable();
        boolean hasPriv = false;
        for (;;) {
            String priv = null;
            for (String p: GrantStatement.getPrivileges()) {
                if (nextStringIf(p) == -1) {
                    continue;
                }
                priv = p;
                break;
            }
            if (priv == null) {
                break;
            }
            
            hasPriv = true;
            stmt.addPrivilege(priv);
            boolean hasSpace = skipIgnorableIf() != -1;
            if ("all".equalsIgnoreCase(priv)) {
                if (hasSpace && nextStringIf("privileges") != -1) {
                    skipIgnorableIf();
                }
            }
            if (nextCharIf(',') != -1) {
                skipIgnorableIf();
                hasPriv = false;
                continue;
            }
            
            break;
        }
        if (!hasPriv) {
            throw syntaxError();
        }
        
        nextString("on");
        skipIgnorable();
        if (nextStringIf("database") != -1 || nextStringIf("schema") != -1) {
            skipIgnorable();
        }
        boolean skipSpace = false;
        for (;;) {
            String granted = nextString();
            stmt.addGranted(granted);
            skipSpace = skipIgnorableIf() != -1;
            if (nextCharIf(',') == -1) {
                break;
            }
            skipIgnorableIf();
        }
        if (!skipSpace) {
            skipIgnorable();
        }
        nextString("to");
        skipIgnorable();
        for (;;) {
            String user = nextString();
            skipIgnorableIf();
            nextChar('@');
            skipIgnorableIf();
            String host = nextString();
            stmt.addGrantee(host, user);
            skipIgnorableIf();
            if (nextCharIf(',') == -1) {
                break;
            }
            skipIgnorableIf();
        }
        
        return stmt;
    }
    
    protected SQLStatement parseSavepoint() {
        nextString("vepoint");
        skipIgnorable();
        String savepointName = nextString();
        return new TransactionStatement(this.sql, "SAVEPOINT", savepointName);
    }
    
    protected SQLStatement parseSelect() {
        nextString("lect");
        if (skipIgnorableIf() != -1) {
            // Parse for supporting simple sleep()
            char bc = 0, lc = 0, qc = 0;
            int deep = 0;
            boolean next = true;
            for (; next;) {
                char c = nextChar();
                switch (c) {
                case '/':
                    if ('*' == nextChar() && lc == 0 && qc == 0) {
                        if (bc == 0) {
                            bc = '/';
                        }
                        ++deep;
                    }
                    break;
                case '*':
                    if ('/' == nextChar() && bc != 0) {
                        --deep;
                        if (deep == 0) {
                            bc = 0;
                        }
                    }
                    break;
                case '-':
                    if ('-' == nextChar() && bc == 0 && qc == 0) {
                        lc = '-';
                    }
                    break;
                case '\n':
                    if (lc != 0) {
                        lc = 0;
                    }
                    break;
                case '\'':
                case '"':
                    if (bc == 0 && lc == 0) {
                        if (qc == 0) {
                            qc = c;
                        } else if (qc == c) {
                            qc = 0;
                        }
                    }
                    break;
                case 's':
                case 'S':
                    if ((bc == 0 && lc == 0 && qc == 0) && (nextStringIf("leep") != -1)) {
                        return parseSelectSleep();
                    }
                    break;
                default:
                    break;
                }
                
                if (next) next = !nextEnd();
            }
        }
        
        return new SQLStatement(this.sql, "SELECT", true);
    }
    
    protected SQLStatement parseSelectSleep() {
        char c;
        
        skipIgnorableIf();
        nextChar('(');
        skipIgnorableIf();
        c = nextSignedNumberChar();
        String sec = nextSignedNumber(c); // Here loosely parse: sleep() check
        skipIgnorableIf();
        nextChar(')');
        if(!nextEnd()) {
            throw syntaxError();
        }
        // OK
        final int second;
        try {
            second = Integer.decode(sec);
            if (second < 0) {
                throw new SQLParseException("Incorrect arguments in call to 'sleep'");
            }
        } catch (NumberFormatException e) {
            throw new SQLParseException("Incorrect arguments in call to 'sleep'", e);
        }
        SelectSleepStatement stmt = new SelectSleepStatement(this.sql);
        stmt.setSecond(second);
        return stmt;
    }
    
    protected SQLStatement parseShow() {
        nextString("ow");
        skipIgnorable();
        if (nextStringIf("grants") != -1) {
            return parseShowGrants();
        } else if (nextStringIf("databases") != -1) {
            return parseShowDatabases(false);
        } else if (nextStringIf("all") != -1) {
            skipIgnorable();
            nextString("databases");
            return parseShowDatabases(true);
        }
        
        throw syntaxError();
    }
    
    protected SQLStatement parseShowDatabases(boolean all) {
        if (nextEnd()) {
            ShowDatabasesStatement stmt = new ShowDatabasesStatement(this.sql);
            stmt.setAll(all);
            return stmt;
        }
        
        throw syntaxError();
    }
    
    protected SQLStatement parseShowGrants() {
        ShowGrantsStatement stmt = new ShowGrantsStatement(this.sql);
        if (nextEnd()) {
            stmt.setCurrentUser(true);
        } else {
            nextString("for");
            skipIgnorable();
            if (nextStringIf("current_user") != -1) {
                if (nextEnd()) {
                    stmt.setCurrentUser(true);
                } else {
                    skipIgnorableIf();
                    nextChar('(');
                    skipIgnorableIf();
                    nextChar(')');
                    if (nextEnd()) {
                        stmt.setCurrentUser(true);
                    } else {
                        throw syntaxError();
                    }
                }
            } else {
                String user = nextString();
                if (!nextEnd()) {
                    nextChar('@');
                    skipIgnorableIf();
                    String host = nextString();
                    if (nextEnd()) {
                        stmt.setHost(host);
                    } else {
                        throw syntaxError();
                    }
                }
                stmt.setUser(user);
            }
        }
        
        return stmt;
    }

    protected SQLStatement parseRelease() {
        nextString("ease");
        skipIgnorable();
        if (nextStringIf("savepoint") != -1) {
            skipIgnorable();
        }
        String savepointName = nextString();
        return new TransactionStatement(this.sql, "RELEASE", savepointName);
    }
    
    protected SQLStatement parseRevoke() {
        nextString("oke");
        
        RevokeStatement stmt = new RevokeStatement(this.sql);
        skipIgnorable();
        boolean hasPriv = false;
        for (;;) {
            String priv = null;
            for (String p: GrantStatement.getPrivileges()) {
                if (nextStringIf(p) == -1) {
                    continue;
                }
                priv = p;
                break;
            }
            if (priv == null) {
                break;
            }
            
            hasPriv = true;
            stmt.addPrivilege(priv);
            boolean hasSpace = skipIgnorableIf() != -1;
            if ("all".equalsIgnoreCase(priv)) {
                if (hasSpace && nextStringIf("privileges") != -1) {
                    skipIgnorableIf();
                }
            }
            if (nextCharIf(',') != -1) {
                skipIgnorableIf();
                hasPriv = false;
                continue;
            }
            
            break;
        }
        if (!hasPriv) {
            throw syntaxError();
        }
        
        nextString("on");
        skipIgnorable();
        if (nextStringIf("database") != -1 || nextStringIf("schema") != -1) {
            skipIgnorable();
        }
        boolean skipSpace = false;
        for (;;) {
            String revoked = nextString();
            stmt.addRevoked(revoked);
            skipSpace = skipIgnorableIf() != -1;
            if (nextCharIf(',') == -1) {
                break;
            }
            skipIgnorableIf();
        }
        if (!skipSpace) {
            skipIgnorable();
        }
        nextString("from");
        skipIgnorable();
        for (;;) {
            String user = nextString();
            skipIgnorableIf();
            nextChar('@');
            skipIgnorableIf();
            String host = nextString();
            stmt.addGrantee(host, user);
            skipIgnorableIf();
            if (nextCharIf(',') == -1) {
                break;
            }
            skipIgnorableIf();
        }
        
        return stmt;
    }

    protected SQLStatement parseRollback() {
        nextString("llback");
        
        skipIgnorableIf();
        if (nextStringIf("transaction") != -1) {
            skipIgnorableIf();
        }
        
        if (nextStringIf("to") != -1) {
            skipIgnorable();
            if (nextStringIf("savepoint") != -1) {
                skipIgnorable();
            }
            String savepointName = nextString();
            return new TransactionStatement(this.sql, "ROLLBACK", savepointName); 
        }
        
        return new TransactionStatement(this.sql, "ROLLBACK");
    }

    protected SQLStatement parseVacuum() {
        nextString("acuum");
        return new SQLStatement(this.sql, "VACUUM");
    }

    protected SQLStatement parseUpdate() {
        nextString("pdate");
        return new SQLStatement(this.sql, "UPDATE");
    }

    protected SQLStatement parseReplace() {
        nextString("lace");
        return new SQLStatement(this.sql, "REPLACE");
    }

    protected SQLStatement parseReindex() {
        nextString("ndex");
        return new SQLStatement(this.sql, "REINDEX");
    }

    protected SQLStatement parsePragma() {
        PragmaStatement stmt;
        String schemaName = null;
        String name, value;
        
        nextString("ragma");
        
        // parse [schema-name.]pragma-name
        skipIgnorable();
        name = nextString();
        skipIgnorableIf();
        if (nextCharIf('.') != -1) {
            skipIgnorableIf();
            schemaName = name;
            name = nextString();
            skipIgnorableIf();
        }
        
        if (nextCharIf('=') != -1) {
            skipIgnorableIf();
            value = nextExpr();
            stmt = new PragmaStatement(this.sql, "PRAGMA");
            stmt.setValue(value);
        } else if(nextCharIf('(') != -1) {
            skipIgnorableIf();
            value = nextExpr();
            skipIgnorableIf();
            nextChar(')');
            stmt = new PragmaStatement(this.sql, "PRAGMA");
            stmt.setValue(value);
        } else {
            skipIgnorableIf();
            nextCharIf(';');
            stmt = new PragmaStatement(this.sql, "PRAGMA", true);
        }
        
        stmt.setSchemaName(schemaName);
        stmt.setName(name);
        return stmt;
    }

    protected SQLStatement parseInsert() {
        nextString("nsert");
        return new SQLStatement(this.sql, "INSERT");
    }

    protected SQLStatement parseExplain() {
        nextString("plain");
        return new SQLStatement(this.sql, "EXPLAIN", true);
    }

    protected SQLStatement parseEnd() {
        nextString("d");
        return new TransactionStatement(this.sql, "END");
    }
    
    protected SQLStatement parseBlockComment() {
        nextBlockComment();
        if (this.ei >= this.sql.length()) {
            SQLStatement stmt = new SQLStatement(this.sql);
            stmt.setComment(true);
            return stmt;
        }
        
        return parseStatement();
    }

    protected SQLStatement parseLineComment() {
        nextLineComment();
        if (this.ei >= this.sql.length()) {
            SQLStatement stmt = new SQLStatement(this.sql);
            stmt.setComment(true);
            return stmt;
        }
        
        return parseStatement();
    }
    
    protected void backChar() {
        if (this.ei == 0) {
            throw new IllegalStateException("No previous char");
        }
        --this.ei;
    }

    protected boolean nextEnd() {
        skipIgnorableIf();
        return (nextCharIf(';') != -1 || this.ei == this.sql.length());
    }
    
    protected String nextSignedNumber(char pfx) {
        String s = this.sql;
        int len = s.length();
        int i = this.ei;
        boolean decimal = ('.' == pfx);
        boolean zerohdd = ('0' == pfx);
        boolean hexnumb = false;
        
        if (i < len) {
            StringBuilder sb = new StringBuilder();
            sb.append(pfx);
            for (; i < len; ++i) {
                char c = s.charAt(i);
                if ((c >= '0' && c <= '9')) {
                    sb.append(c);
                    continue;
                }
                if ((c >= 'a' && c <= 'f')||(c >= 'A' && c <= 'F')) {
                    if (hexnumb) {
                        sb.append(c);
                        continue;
                    }
                    break;
                }
                
                if ('.' == c) {
                    if (decimal || hexnumb) {
                        this.ei = i;
                        throw syntaxError();
                    }
                    decimal = true;
                    sb.append(c);
                    continue;
                }
                
                if (('x' == c || 'X' == c) && zerohdd && sb.length() == 1) {
                    hexnumb = true;
                    sb.append(c);
                    continue;
                }
                
                break;
            }
            
            this.ei = i;
            if (sb.length() == 1) {
                if (pfx < '0' || pfx > '9') {
                    throw syntaxError();
                }
            }
            return sb.toString();
        }
        
        if (pfx >= '0' && pfx <= '9') {
            return pfx + "";
        }
        
        throw syntaxError();
    }
    
    protected String nextExpr() {
        String s = this.sql;
        int len = s.length();
        int i = this.ei;
        
        if (i < len) {
            char c = s.charAt(i++);
            if (isSignedNumberPrefix(c)) {
                this.ei = i;
                return nextSignedNumber(c);
            } else {
                return nextString();
            }
        }
        
        throw syntaxError();
    }
    
    protected char nextSignedNumberChar() {
        String s = this.sql;
        int len = s.length();
        int i = this.ei;
        
        if (i < len) {
            char c = s.charAt(i++);
            if (isSignedNumberPrefix(c)) {
                this.ei = i;
                return c;
            }
        }
        
        throw syntaxError();
    }
    
    protected boolean isSignedNumberPrefix(char c) {
        return ('-' == c || '+' == c || '.' == c || (c >= '0' && c <= '9'));
    }
    
    protected String nextString() {
        return nextString(false);
    }
    
    protected String nextString(boolean quoted) {
        String s = this.sql;
        int len = s.length();
        int i = this.ei;
        
        if (i < len) {
            StringBuilder sb = new StringBuilder();
            char c = s.charAt(i++);
            char q = c;
            if ('\'' == q || '"' == q) {
                boolean ok = false;
                for (; i < len;) {
                    c = s.charAt(i++);
                    if (q == c) {
                        ok = true;
                        break;
                    }
                    sb.append(c);
                }
                this.ei = i;
                if (!ok) {
                    throw syntaxError(); 
                }
                return sb.toString();
            }
            
            if (quoted) {
                throw syntaxError();
            }
            
            if ('_' == c || '$' == c
                    || (c >= 'a' && c <= 'z') 
                    || (c >= 'A' && c <= 'Z')) {
                sb.append(c);
                for (; i < len; ) {
                    c = s.charAt(i);
                    if ('_' == c || '$' == c
                            || (c >= 'a' && c <= 'z') 
                            || (c >= 'A' && c <= 'Z')
                            || (c >= '0' && c <= '9')) {
                        ++i;
                        sb.append(c);
                        continue;
                    }
                    break;
                }
                this.ei = i;
                return sb.toString();
            }
        }
        
        throw syntaxError();
    }
    
    protected int nextStringIf(String s) {
        return nextStringIf(s, true);
    }
    
    protected int nextStringIf(String s, boolean ignoreCase) {
        int i = 0, len = s.length();
        boolean forward= false;
        for (; i < len; ) {
            if(nextCharIf(s.charAt(i), ignoreCase, forward, i) == -1) {
                break;
            }
            ++i;
        }
        if (i == len) {
            return (this.ei += i);
        }
        
        return -1;
    }
    
    protected int nextCharIf(char c) {
        return nextCharIf(c, true, true, 0);
    }
    
    protected int nextCharIf(char c, boolean ignoreCase, boolean forward, int offset) {
        int len = this.sql.length();
        int i = this.ei + offset;
        if (i < len) {
            char a = this.sql.charAt(i++);
            if (a == c || (ignoreCase && toLowerCase(a) == toLowerCase(c))) {
                if (forward) {
                    return (this.ei = i);
                }
                return i;
            }
        }
        
        return -1;
    }
    
    protected void nextString(String s) {
        nextString(s, true);
    }
    
    protected void nextString(String s, boolean ignoreCase) {
        for (int i = 0, len = s.length(); i < len; ++i) {
            nextChar(s.charAt(i), ignoreCase);
        }
    }

    protected void nextChar(char c) {
        nextChar(c, true);
    }
    
    protected void nextChar(char c, boolean ignoreCase) {
        boolean forward = true;
        if (nextCharIf(c, ignoreCase, forward, 0) == -1) {
            throw syntaxError();
        }
    }
    
    protected char nextChar() {
        if (this.ei < this.sql.length()) {
            return this.sql.charAt(this.ei++);
        }
        
        throw syntaxError();
    }
    
    protected void skipComments() {
        if (skipCommentsIf() == -1) {
            throw syntaxError();
        }
    }
    
    protected int skipCommentsIf() {
        boolean ok = false;
        for (; nextBlockCommentIf() != -1 
                || nextLineCommentIf() != -1;){
            ok = true;
        }
        
        if (ok) {
            return this.ei;
        }
        
        return -1;
    }
    
    protected int nextLineCommentIf() {
        String s = this.sql;
        int len = s.length();
        int i = this.ei;
        
        if (i < len) {
            char c = s.charAt(i++);
            if ('-' == c && (i < len && '-' == s.charAt(i++))) {
                this.ei = i;
                return nextLineComment();
            }
        }
        
        return -1;
    }

    protected int nextLineComment() {
        String s = this.sql;
        int len = s.length();
        int i = this.ei;
        
        for (; i < len;) {
            char c = s.charAt(i++);
            if ('\n' == c) {
                break;
            }
        }
        
        return (this.ei = i);
    }

    protected int nextBlockCommentIf() {
        String s = this.sql;
        int len = s.length();
        int i = this.ei;
        
        if (i < len) {
            char c = s.charAt(i++);
            if ('/' == c && (i < len && '*' == s.charAt(i++))) {
                this.ei = i;
                return nextBlockComment();
            }
        }
        
        return -1;
    }
    
    protected int nextBlockComment() {
        String s = this.sql;
        int len = s.length();
        int i = this.ei;
        int deep = 1;
        
        for (;i < len;) {
            char c = s.charAt(i++);
            // feature: SQL-99 nested block comment
            if ('/' == c && (i < len && '*' == s.charAt(i))) {
                ++i;
                ++deep;
                continue;
            }
            
            if ('*' == c && (i < len && '/' == s.charAt(i)) 
                    && (--deep == 0)) {
                ++i;
                break;
            }
        }
        
        return (this.ei = i);
    }
    
    protected void skipSpaces() {
        if (skipSpacesIf() == -1) {
            throw syntaxError();
        }
    }

    protected int skipSpacesIf() {
        String s = this.sql;
        int i = this.ei;
        int len = s.length();
        
        for (; i < len; ) {
            char c = s.charAt(i);
            if (c > 32 && c != 127) {
                if (i == this.ei) {
                    return -1;
                }
                return (this.ei = i);
            }
            ++i;
        }
        if (i == this.ei) {
            return -1;
        }
       
        return (this.ei = i);
    }
    
    protected void skipIgnorable() {
        if (skipIgnorableIf() == -1) {
            throw syntaxError();
        }
    }
    
    protected int skipIgnorableIf() {
        boolean ok = false;
        for (; skipSpacesIf() != -1 || skipCommentsIf() != -1;) {
            ok = true;
        }
        
        if (ok) {
            return this.ei;
        }
        
        return -1;
    }
    
    protected SQLParseException syntaxError() {
        return new SQLParseException(this.sql.substring(0, this.ei) + "^");
    }

    protected SQLParseException syntaxError(String format, Object ... args) {
        return new SQLParseException(String.format(format, args));
    }
    
}
