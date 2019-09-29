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

import java.sql.SQLException;

import org.sqlite.TestBase;
import org.sqlite.sql.SQLReader;

/**Test SQL reader.
 * 
 * @author little-pan
 * @since 2019-09-04
 *
 */
public class SQLReaderTest extends TestBase {
    
    protected boolean ignoreNbc;
    
    public static void main(String args[]) throws SQLException {
        new SQLReaderTest().test();
    }
    
    @Override
    protected void doTest() throws SQLException {
        sqlTest("");
        sqlTest(" ", " ");
        sqlTest("\n");
        sqlTest(";", ";");
        sqlTest(" ;", " ;");
        sqlTest(" ; ", " ;", " ");
        
        sqlTest("select 1;", "select 1;");
        sqlTest("select 1", "select 1");
        sqlTest("select 1;select 2", "select 1;", "select 2");
        sqlTest("select 1;select 2 ; select 3", 
                "select 1;", "select 2 ;", " select 3");
        
        sqlTest("-- select 1", "-- select 1");
        sqlTest("--select 1", "--select 1");
        sqlTest("-- sql\nselect 1", "-- sql\nselect 1");
        sqlTest("--sql\nselect 1", "--sql\nselect 1");
        sqlTest("-- sql\nselect 1;", "-- sql\nselect 1;");
        sqlTest("-- sql\n-- select 1;", "-- sql\n-- select 1;");
        sqlTest("select 1-- sql", "select 1-- sql");
        sqlTest("select 1;-- sql", "select 1;", "-- sql");
        sqlTest("select '1;';-- sql", "select '1;';", "-- sql");
        sqlTest("-- sql\nselect '1;';-- sql", "-- sql\nselect '1;';", "-- sql");
        
        sqlTest("/*sql*/\nselect 1", "/*sql*/\nselect 1");
        sqlTest("/*sql*/\nselect 1;", "/*sql*/\nselect 1;");
        sqlTest("/*sql*/\n--select 1;", "/*sql*/\n--select 1;");
        sqlTest("/*sql*/\n/*select 1;*/", "/*sql*/\n/*select 1;*/");
        sqlTest("/**/select /*test*/1", "/**/select /*test*/1");
        sqlTest("/**/select /*test*/'/**/1'", "/**/select /*test*/'/**/1'");
        sqlTest("/**/select /*test*/'/**/ 1'", "/**/select /*test*/'/**/ 1'");
        sqlTest("/**/select /*test*/'/*/**/*/ 1'", "/**/select /*test*/'/*/**/*/ 1'");
        
        sqlTest("/**/select /*test*/1;select 2/*sql*/", 
                "/**/select /*test*/1;", "select 2/*sql*/");
        sqlTest("/*'*/select /*test;*/1;select 2/*sql*/", 
                "/*'*/select /*test;*/1;", "select 2/*sql*/");
        
        sqlTest("/**/select /*test*/1;select 2/*sql*/;select 3--sql", 
                "/**/select /*test*/1;", "select 2/*sql*/;", "select 3--sql");
        sqlTest("/*';--*/select /*-test*/1;select 2/*sql**/;select 3--/*sql*/", 
                "/*';--*/select /*-test*/1;", "select 2/*sql**/;", "select 3--/*sql*/");
        
        sqlTest("/**/select /*test*/1;\nselect 2/*sql*/", 
                "/**/select /*test*/1;", "select 2/*sql*/");
        sqlTest("/*s--q'l\"*/select /*test*/'/*1-\"';\nselect \"2/*;'-'--*/\"/*sql*/", 
                "/*s--q'l\"*/select /*test*/'/*1-\"';", "select \"2/*;'-'--*/\"/*sql*/");
        
        sqlTest("/*/**/*/", "/*/**/*/");
        sqlTest("/*b/**/*/", "/*b/**/*/");
        sqlTest("/*b/*b*/*/", "/*b/*b*/*/");
        sqlTest("/*b/*b*/b*/", "/*b/*b*/b*/");
        sqlTest("/*select 1;/*select 2;*/select 3*/", "/*select 1;/*select 2;*/select 3*/");
        sqlTest("/*select 1;/*select 2;*/select 3;*/", "/*select 1;/*select 2;*/select 3;*/");
        sqlTest("/*select 1;/*select 2;*/select 3;*/ ", "/*select 1;/*select 2;*/select 3;*/ ");
        sqlTest("/*select 1;/*select 2;*/select 3;*/ -- c", "/*select 1;/*select 2;*/select 3;*/ -- c");
        sqlTest("/*select 1;/*select 2;*/select 3;*/ select 1", "/*select 1;/*select 2;*/select 3;*/ select 1");
        
        this.ignoreNbc = true;
        sqlTest("");
        sqlTest(" ", " ");
        sqlTest("\n");
        sqlTest(";", ";");
        sqlTest(" ;", " ;");
        sqlTest(" ; ", " ;", " ");
        
        sqlTest("select 1;", "select 1;");
        sqlTest("select 1", "select 1");
        sqlTest("select 1;select 2", "select 1;", "select 2");
        sqlTest("select 1;select 2 ; select 3", 
                "select 1;", "select 2 ;", " select 3");
        
        sqlTest("-- select 1", "-- select 1");
        sqlTest("--select 1", "--select 1");
        sqlTest("-- sql\nselect 1", "-- sql\nselect 1");
        sqlTest("--sql\nselect 1", "--sql\nselect 1");
        sqlTest("-- sql\nselect 1;", "-- sql\nselect 1;");
        sqlTest("-- sql\n-- select 1;", "-- sql\n-- select 1;");
        sqlTest("select 1-- sql", "select 1-- sql");
        sqlTest("select 1;-- sql", "select 1;", "-- sql");
        sqlTest("select '1;';-- sql", "select '1;';", "-- sql");
        sqlTest("-- sql\nselect '1;';-- sql", "-- sql\nselect '1;';", "-- sql");
        
        sqlTest("/*sql*/\nselect 1", "/*sql*/\nselect 1");
        sqlTest("/*sql*/\nselect 1;", "/*sql*/\nselect 1;");
        sqlTest("/*sql*/\n--select 1;", "/*sql*/\n--select 1;");
        sqlTest("/*sql*/\n/*select 1;*/", "/*sql*/\n/*select 1;*/");
        sqlTest("/**/select /*test*/1", "/**/select /*test*/1");
        sqlTest("/**/select /*test*/'/**/1'", "/**/select /*test*/'/**/1'");
        sqlTest("/**/select /*test*/'/**/ 1'", "/**/select /*test*/'/**/ 1'");
        sqlTest("/**/select /*test*/'/*/**/*/ 1'", "/**/select /*test*/'/*/**/*/ 1'");
        
        sqlTest("/**/select /*test*/1;select 2/*sql*/", 
                "/**/select /*test*/1;", "select 2/*sql*/");
        sqlTest("/*'*/select /*test;*/1;select 2/*sql*/", 
                "/*'*/select /*test;*/1;", "select 2/*sql*/");
        
        sqlTest("/**/select /*test*/1;select 2/*sql*/;select 3--sql", 
                "/**/select /*test*/1;", "select 2/*sql*/;", "select 3--sql");
        sqlTest("/*';--*/select /*-test*/1;select 2/*sql**/;select 3--/*sql*/", 
                "/*';--*/select /*-test*/1;", "select 2/*sql**/;", "select 3--/*sql*/");
        
        sqlTest("/**/select /*test*/1;\nselect 2/*sql*/", 
                "/**/select /*test*/1;", "select 2/*sql*/");
        sqlTest("/*s--q'l\"*/select /*test*/'/*1-\"';\nselect \"2/*;'-'--*/\"/*sql*/", 
                "/*s--q'l\"*/select /*test*/'/*1-\"';", "select \"2/*;'-'--*/\"/*sql*/");
        
        sqlTest("/*/**/*/", "/**/");
        sqlTest("/*b/**/*/", "/*b*/");
        sqlTest("/*b/*b*/*/", "/*b*/");
        sqlTest("/*b/*b*/b*/", "/*bb*/");
        sqlTest("/*select 1;/*select 2;*/select 3*/", "/*select 1;select 3*/");
        sqlTest("/*select 1;/*select 2;*/select 3;*/", "/*select 1;select 3;*/");
        sqlTest("/*select 1;/*select 2;*/select 3;*/ ", "/*select 1;select 3;*/ ");
        sqlTest("/*select 1;/*select 2;*/select 3;*/ -- c", "/*select 1;select 3;*/ -- c");
        sqlTest("/*select 1;/*select 2;*/select 3;*/ select 1", "/*select 1;select 3;*/ select 1");
        sqlTest("/*/*/**/*/*/", "/**/");
        sqlTest("/*b/*/**/*//*b*/*/", "/*b*/");
        sqlTest("/*b/*b/**/*/*/", "/*b*/");
        sqlTest("/*b/*b/*b*/*/b*/", "/*bb*/");
        sqlTest("/*select 1;/*/*select 4;*/select 2;*//*select 5;*/select 3*/", "/*select 1;select 3*/");
        sqlTest("/*select 1;/*/*select 4;*/select 2;*//*select 5;*/select 3;*/", "/*select 1;select 3;*/");
        sqlTest("/*select 1;/*/*select 4;*/select 2;*//*select 5;*/select 3;*/ ", "/*select 1;select 3;*/ ");
        sqlTest("/*select 1;/*/*select 4;*/select 2;*//*select 5;*/select 3;*/ -- c", "/*select 1;select 3;*/ -- c");
        sqlTest("/*select 1;/*/*select 4;*/select 2;*//*select 5;*/select 3;*/ select 1", "/*select 1;select 3;*/ select 1");
        sqlTest("/*select 1;/*/*select 4;*/select 2;*/ /*select 5;*/select 3;*/ select 1", "/*select 1; select 3;*/ select 1");
    }

    protected void sqlTest(String sqls, String ... results) throws SQLException {
        try (SQLReader reader = new SQLReader(sqls, this.ignoreNbc)) {
            String stmt;
            // test per stmt
            for (String res : results) {
                stmt = reader.readStatement();
                boolean failed = true;
                try {
                    assertTrue(res == stmt || res.equals(stmt));
                    failed = false;
                } finally {
                    if (failed) {
                        error("SQL error: %s", stmt);
                    }
                }
            }
            // EOF
            stmt = reader.readStatement();
            assertTrue(stmt == null);
        }
    }
    
}

