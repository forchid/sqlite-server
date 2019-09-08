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
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.sqlite.server.util.IoUtils;

/**A simple SQL parser.
 * 
 * @author little-pan
 * @since 2019-09-04
 *
 */
public class SQLParser implements Iterator<SQLStatement>, Iterable<SQLStatement>, AutoCloseable {
    
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
                return parseBegin();
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
                    throw syntaxError();
                }
                if ('o' == c || 'O' == c) {
                    return parseRollback();
                }
                throw syntaxError();
            case 's':
            case 'S':
                c = nextChar();
                if ('e' == c || 'E' == c) {
                    return parseSelect();
                }
                if ('a' == c || 'A' == c) {
                    return parseSavepoint();
                }
                throw syntaxError();
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
    
    protected SQLStatement parseSavepoint() {
        nextString("vepoint");
        skipIgnorable();
        String savepointName = nextString();
        return new TransactionStatement(this.sql, "SAVEPOINT", savepointName);
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

    protected SQLStatement parseSelect() {
        nextString("lect");
        return new SQLStatement(this.sql, "SELECT", true);
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

    protected SQLStatement parseDrop() {
        nextString("op");
        return new SQLStatement(this.sql, "DROP");
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

    protected SQLStatement parseCreate() {
        nextString("eate");
        return new SQLStatement(this.sql, "CREATE");
    }

    protected SQLStatement parseCommit() {
        nextString("mmit");
        return new TransactionStatement(this.sql, "COMMIT");
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

    protected SQLStatement parseBegin() {
        nextString("egin");
        return new TransactionStatement(this.sql, "BEGIN");
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
    
    protected SQLStatement parseAnalyze() {
        nextString("alyze");
        return new SQLStatement(this.sql, "ANALYZE");
    }

    protected SQLStatement parseAlter() {
        nextString("ter");
        return new SQLStatement(this.sql, "ALTER");
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
            if ('-' == c || '+' == c || '.' == c || (c >= '0' && c <= '9')) {
                this.ei = i;
                return nextSignedNumber(c);
            } else {
                return nextString();
            }
        }
        
        throw syntaxError();
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
            if ('/' == c && (i < len && '*' == s.charAt(i++))) {
                ++deep;
                continue;
            }
            
            if ('*' == c && (i < len && '/' == s.charAt(i++)) 
                    && (--deep == 0)) {
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

}
