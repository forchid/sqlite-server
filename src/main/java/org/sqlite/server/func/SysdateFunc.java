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

import static java.lang.String.*;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.sqlite.Function;
import org.sqlite.SQLiteErrorCode;

/** Function sysdate([fsp]), not thread-safe.
 * 
 * @author little-pan
 * @since 2019-09-18
 *
 */
public class SysdateFunc extends Function {
    
    public static final int MAX_FSP = 3;
    public static final int MIN_FSP = 0;
    
    protected final DateFormat baseFormater;
    protected final DateFormat fspFormater;
    
    public SysdateFunc() {
        this.baseFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        this.fspFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    }

    @Override
    protected void xFunc() throws SQLException {
        int  args = super.args();
        int fsp = MIN_FSP;
        
        // init and check
        if (args > 1) {
            SQLiteErrorCode error = SQLiteErrorCode.SQLITE_ERROR;
            throw new SQLException("sysdate() arguments too many", "42000", error.code);
        }
        if (args == 1) {
            fsp = super.value_int(0);
        }
        if (fsp < MIN_FSP) {
            String message = format("Too-little precision %s specified for 'sysdate', minimum is %s", fsp, MIN_FSP);
            throw new SQLException(message, "42000", SQLiteErrorCode.SQLITE_ERROR.code);
        }
        if (fsp > MAX_FSP) {
            String message = format("Too-big precision %s specified for 'sysdate', maximum is %s", fsp, MAX_FSP);
            throw new SQLException(message, "42000", SQLiteErrorCode.SQLITE_ERROR.code);
        }
        
        // Do format
        String result;
        if (fsp == 0) {
            result = this.baseFormater.format(new Date());
        } else {
            result = this.fspFormater.format(new Date());
            int diff = MAX_FSP - fsp;
            if (diff > 0) {
                result = result.substring(0, result.length() - diff);
            }
        }
        super.result(result);
    }

}
