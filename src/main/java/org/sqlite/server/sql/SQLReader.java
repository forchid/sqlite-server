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
package org.sqlite.server.sql;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayDeque;
import java.util.Deque;

import org.sqlite.server.util.IoUtils;

/**SQL statement reader.
 * 
 * @author little-pan
 * @since 2019-09-01
 *
 */
public class SQLReader implements Closeable {
    
    protected final Deque<String> buffer = new ArrayDeque<>();
    protected final BufferedReader reader;
    private boolean open;
    
    public SQLReader(Reader reader) {
        this.reader = new BufferedReader(reader);
        this.open = true;
    }
    
    public String readStatement() throws IOException {
        if (this.buffer.size() > 0) {
            return this.buffer.poll();
        }
        if (!isOpen()) {
            return null;
        }
        
        StringBuilder sb = new StringBuilder();
        boolean blk = false, qot = false;
        char q = 0;
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
                sb.append(c);
                
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
                        if (blk || qot) {
                            continue;
                        }
                        blk = true;
                        sb.append('*');
                        ++i;
                        continue;
                    }
                    break;
                case '*':
                    if (i < len - 1 && line.charAt(i + 1) == '/') {
                        if (!blk) {
                            continue;
                        }
                        blk = false;
                        sb.append('/');
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
                throw new IllegalStateException("Unexpected sql string end: " + sb + "^");
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
    }

    @Override
    public void close() {
        this.open = false;
        IoUtils.close(this.reader);
    }
    
    public boolean isOpen() {
        return this.open;
    }
    
}
