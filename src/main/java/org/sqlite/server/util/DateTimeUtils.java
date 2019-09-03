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

/**
 * @author little-pan
 * @since 2019-09-01
 *
 */
public final class DateTimeUtils {
    
    private static final int SHIFT_YEAR = 9;
    private static final int SHIFT_MONTH = 5;
    
    private DateTimeUtils() {}

    public static long absoluteDayFromDateValue(long dateValue) {
        long y = yearFromDateValue(dateValue);
        int m = monthFromDateValue(dateValue);
        int d = dayFromDateValue(dateValue);
        long a = absoluteDayFromYear(y);
        a += ((367 * m - 362) / 12) + d - 1;
        if (m > 2) {
            a--;
            if ((y & 3) != 0 || (y % 100 == 0 && y % 400 != 0)) {
                a--;
            }
        }
        return a;
    }
    
    public static long absoluteDayFromYear(long year) {
        long a = 365 * year - 719_528;
        if (year >= 0) {
            a += (year + 3) / 4 - (year + 99) / 100 + (year + 399) / 400;
        } else {
            a -= year / -4 - year / -100 + year / -400;
        }
        return a;
    }
    
    public static int yearFromDateValue(long x) {
        return (int) (x >>> SHIFT_YEAR);
    }
    
    public static int monthFromDateValue(long x) {
        return (int) (x >>> SHIFT_MONTH) & 15;
    }
    
    public static int dayFromDateValue(long x) {
        return (int) (x & 31);
    }
}
