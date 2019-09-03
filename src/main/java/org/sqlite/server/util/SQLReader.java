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
package org.sqlite.server.util;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;

/**SQL statement reader.
 * 
 * @author little-pan
 * @since 2019-09-01
 *
 */
public class SQLReader implements Closeable {
    
    BufferedReader reader;
    
    public SQLReader(Reader reader) {
        this.reader = new BufferedReader(reader);
    }
    
    public String readStatement() throws IOException {
        StringBuilder sb = new StringBuilder();
        boolean blockc = false;
        for (String sql = null, line = reader.readLine(); 
                line != null; 
                line = reader.readLine()) {
            line = line.trim();
            
            if (blockc) {
                if (line.endsWith("*/")) {
                    blockc = false;
                }
                continue;
            }
            
            if (line.startsWith("/*")) {
                blockc = line.length() < 4 || !line.endsWith("*/");
                continue;
            }
            
            if (line.startsWith("--")) {
                continue;
            }
            
            sb.append(sb.length() == 0?"":"\r\n").append(line);
            if (line.endsWith(";")) {
                sql = sb.toString();
                if (sql.equals(";")) {
                    sb.setLength(0);
                    sql = null;
                    continue;
                }
                return sql;
            }
        }
        
        if (sb.length() == 0) {
            return null;
        }
        return sb.toString();
    }

    @Override
    public void close() {
        IoUtils.close(this.reader);
    }
    
}
