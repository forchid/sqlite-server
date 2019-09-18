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
package org.sqlite.util;

import java.sql.SQLException;
import java.util.TimeZone;

import org.sqlite.TestBase;

import static org.sqlite.util.DateTimeUtils.*;

/**
 * @author little-pan
 * @since 2019-09-18
 *
 */
public class DateTimeUtilsTest extends TestBase {
    
    public static void main(String args[]) throws SQLException {
        new DateTimeUtilsTest().test();
    }

    @Override
    public void test() throws SQLException {
        TimeZone tz;
        
        tz = TimeZone.getTimeZone("GMT+08:00");
        assertTrue("GMT+08:00".equals(getTimeZoneID(tz, true)));
        assertTrue("+08:00".equals(getTimeZoneID(tz, false)));
        
        tz = TimeZone.getTimeZone("GMT-08:00");
        assertTrue("GMT-08:00".equals(getTimeZoneID(tz, true)));
        assertTrue("-08:00".equals(getTimeZoneID(tz, false)));
        
        tz = TimeZone.getTimeZone("GMT-08:10");
        assertTrue("GMT-08:10".equals(getTimeZoneID(tz, true)));
        assertTrue("-08:10".equals(getTimeZoneID(tz, false)));
        
        tz = TimeZone.getTimeZone("GMT-08:01");
        assertTrue("GMT-08:01".equals(getTimeZoneID(tz, true)));
        assertTrue("-08:01".equals(getTimeZoneID(tz, false)));
        
        tz = TimeZone.getTimeZone("GMT+00:00");
        assertTrue("GMT+00:00".equals(getTimeZoneID(tz, true)));
        assertTrue("+00:00".equals(getTimeZoneID(tz, false)));
    }

}
