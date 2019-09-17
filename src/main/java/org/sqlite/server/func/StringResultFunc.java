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
package org.sqlite.server.func;

import java.sql.SQLException;

import org.sqlite.Function;

/** String result function
 * 
 * @author little-pan
 * @since 2019-09-17
 *
 */
public class StringResultFunc extends Function {
    
    protected final String result;
    protected final String name;
    
    public StringResultFunc(String name, String result) {
        this.name = name;
        this.result = result;
    }

    @Override
    protected void xFunc() throws SQLException {
        super.result(this.result);
    }

    public String getName() {
        return this.name;
    }
    
}

