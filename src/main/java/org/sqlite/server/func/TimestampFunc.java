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

import static java.lang.String.format;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.sqlite.Function;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.server.SQLiteServer;
import org.sqlite.util.DateTimeUtils;

/** Function clock_timestamp() and sysdate([fsp]).
 * 
 * @author little-pan
 * @since 2019-09-18
 *
 */
public class TimestampFunc extends Function {
    
    protected final SQLiteServer server;
    protected final String name;
    protected final int fspdef;
    
    public TimestampFunc(SQLiteServer server, String name) {
        this(server, name, 0);
    }
    
    public TimestampFunc(SQLiteServer server, String name, int fspdef) {
        if (fspdef < 0 || fspdef > 9) {
            throw new IllegalArgumentException("fspdef: " + fspdef);
        }
        this.server = server;
        this.name = name;
        this.fspdef = fspdef;
    }

    @Override
    protected void xFunc() throws SQLException {
        int  args = super.args();
        int fsp = this.fspdef;
        
        // init and check
        if (args > 1) {
            SQLiteErrorCode error = SQLiteErrorCode.SQLITE_ERROR;
            throw new SQLException(this.name+"() arguments too many", "42000", error.code);
        }
        if (args == 1) {
            fsp = super.value_int(0);
        }
        
        String result;
        try {
            result = getTimestamp(fsp);
        } catch (IllegalArgumentException e) {
            String message = e.getMessage();
            throw new SQLException(message, "42000", SQLiteErrorCode.SQLITE_ERROR.code);
        }
        
        super.result(result);
    }
    
    public String getName() {
        return this.name;
    }
    
    public int getFspdef() {
        return this.fspdef;
    }
    
    public String getTimestamp() throws IllegalArgumentException {
        return (getTimestamp(this.fspdef));
    }
    
    protected String getTimestamp(int fsp) throws IllegalArgumentException {
        String timeZone = DateTimeUtils.getTimeZoneID(false);
        DateFormat formater;
        String result;
        
        if (fsp < 0) {
            String message = format("Too-little precision %s specified for '%s', minimum is 0", fsp, this.name);
            throw new IllegalArgumentException(message);
        }
        if (fsp > 9) {
            String message = format("Too-big precision %s specified for '%s', maximum is 9", fsp, this.name);
            throw new IllegalArgumentException(message);
        }
        
        if (fsp == 0) {
            formater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            result = formater.format(new Date()) + timeZone;
        } else {
            long curNanos = System.nanoTime();
            long elapsedNanos = curNanos - this.server.getStartNanos();
            long startMillis = this.server.getStartMillis();
            long curMillis = startMillis + (elapsedNanos / 1000000L);
            long fs = (((curMillis % 1000L) * 1000000L + (elapsedNanos % 1000000L))% 1000000000L) / multi(fsp);
            formater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            result = formater.format(new Date(curMillis));
            result = String.format("%s.%0"+fsp+"d%s", result, fs, timeZone);
        }
        
        return result;
    }
    
    static long multi(int fsp) {
        switch (fsp) {
        case 1:
            return 100000000L;
        case 2:
            return 10000000L;
        case 3:
            return 1000000L;
        case 4:
            return 100000L;
        case 5:
            return 10000L;
        case 6:
            return 1000L;
        case 7:
            return 100L;
        case 8:
            return 10L;
        case 9:
            return 1L;
        default:
            throw new IllegalArgumentException("fsp: " + fsp);
        }
    }
    
}
