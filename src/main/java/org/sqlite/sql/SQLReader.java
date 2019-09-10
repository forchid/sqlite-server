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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayDeque;
import java.util.Deque;

import org.sqlite.util.IoUtils;

/**SQL statement reader.
 * 
 * @author little-pan
 * @since 2019-09-01
 *
 */
public class SQLReader implements Closeable {
    
    protected final Deque<String> buffer = new ArrayDeque<>();
    protected final BufferedReader reader;
    protected final boolean ignoreNbc;
    private boolean open;
    
    public SQLReader(String sqls) {
        this(sqls, false);
    }
    
    public SQLReader(String sqls, boolean ignoreNestedBlockComment) {
        this(new StringReader(sqls), ignoreNestedBlockComment);
    }
    
    public SQLReader(Reader reader) {
        this(reader, false);
    }
    
    public SQLReader(Reader reader, boolean ignoreNestedBlockComment) {
        this.reader = new BufferedReader(reader);
        this.open = true;
        this.ignoreNbc = ignoreNestedBlockComment;
    }
    
    public String readStatement() throws SQLParseException {
        if (this.buffer.size() > 0) {
            return this.buffer.poll();
        }
        if (!isOpen()) {
            return null;
        }
        
        try {
            StringBuilder sb = new StringBuilder();
            boolean blk = false, qot = false;
            boolean inbc = false;
            char q = 0;
            int blkDeep = 0;
            
            for (String line = reader.readLine(); 
                    this.open = (line != null); 
                    line = reader.readLine()) {
                // Padding line separator
                if (sb.length() > 0) {
                    sb.append('\n');
                }
                
                // Parse line ->
                for (int i = 0, len = line.length(); i < len; ++i) {
                    char c = line.charAt(i);
                    if (!inbc) {
                        sb.append(c);
                    }
                    
                    // statements separated by ';'
                    switch (c) {
                    case ';':
                        if (blk || qot) {
                            continue;
                        }
                        this.buffer.offer(sb.toString());
                        sb.setLength(0);
                        break;
                    case '\'':
                    case '"':
                        if (blk || (qot && q != c)) {
                            continue;
                        }
                        if (qot) {
                            qot = false;
                            q  = 0;
                        } else {
                            qot = true;
                            q  = c;
                        }
                        break;
                    case '/':
                        if (i < len - 1 && line.charAt(i + 1) == '*') {
                            if (qot) {
                                continue;
                            }
                            blk = true;
                            // feature: SQL-99 nested block comment
                            ++blkDeep;
                            if (this.ignoreNbc && blkDeep == 2) {
                                // ignore '/*' in nested block
                                inbc = true;
                                sb.setLength(sb.length() - 1);
                            }
                            if (!inbc) {
                                sb.append('*');
                            }
                            ++i;
                            continue;
                        }
                        break;
                    case '*':
                        if (i < len - 1 && line.charAt(i + 1) == '/') {
                            if (!blk) {
                                continue;
                            }
                            blk = (--blkDeep > 0);
                            if (!inbc) {
                                sb.append('/'); 
                            }
                            if (this.ignoreNbc && blkDeep == 1) {
                                inbc = false;
                            }
                            ++i;
                            continue;
                        }
                        break;
                    case '-':
                        if (i < len - 1 && line.charAt(i + 1) == '-') {
                            if (blk || qot) {
                                continue;
                            }
                            // skip to EOL
                            sb.append(line.substring(i + 1));
                            i = len - 1; 
                            continue;
                        }
                        break;
                    default:
                        // normal
                        break;
                    }
                }
                if (qot) {
                    throw new SQLParseException("Unexpected sql string end: " + sb + "^");
                }
                // Parse line <-
                
                if (blk || this.buffer.size() == 0 || sb.length() > 0) {
                    continue;
                }
                break;
            }
            
            if (sb.length() > 0) {
                this.buffer.offer(sb.toString());
                sb.setLength(0);
                close();
            }
            return this.buffer.poll();
        } catch (IOException e) {
            throw new SQLParseException("Can't read sql statement", e);
        }
    }

    @Override
    public void close() {
        this.open = false;
        IoUtils.close(this.reader);
    }
    
    public boolean isOpen() {
        return this.open;
    }
    
    public boolean isIgnoreNestedBlockComment() {
        return this.ignoreNbc;
    }
    
}
