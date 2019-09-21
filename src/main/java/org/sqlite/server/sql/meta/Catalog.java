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
package org.sqlite.server.sql.meta;

import org.sqlite.util.StringUtils;

/** The database catalog that includes databases and data directories.
 * 
 * @author little-pan
 * @since 2019-09-19
 *
 */
public class Catalog {
    
    private String db;
    private String dir; // server data dir: null for flexible movement
    private long size;  // db size
    
    public Catalog() {
        
    }
    
    public Catalog(String db) {
        this(db, null);
    }
    
    public Catalog(String db, String dir) {
        this.db = db;
        this.dir = dir;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = StringUtils.toLowerEnglish(db);
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }
    
}
