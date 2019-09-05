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

import java.sql.SQLException;
import java.util.NoSuchElementException;

import org.sqlite.server.TestBase;

/**
 * @author little-pan
 * @since 2019-09-05
 *
 */
public class TestSQLParser extends TestBase {
    
    public static void main(String args[]) throws SQLException {
        new TestSQLParser().test();
    }

    @Override
    public void test() throws SQLException {
        emptyTest(";");
        emptyTest(" ;");
        emptyTest("; ");
        emptyTest(" ; ");
        emptyTest("/*;*/;");
        emptyTest("/*;*/; --");
        
        commentTest("-- sql/*sql*/");
        commentTest("/*sql--*/");
        commentTest("/*sql--*/--");
        commentTest("/*sql--*/  --");
        commentTest("/*sql--*/\n--");
        
        selectTest("select 1", 1);
        selectTest("select 1;", 1);
        selectTest("Select 1;", 1);
        selectTest("sElect 1;", 1);
        selectTest("selecT 1;", 1);
        selectTest(" select 1-- sql", 1);
        selectTest("/**/select 1;", 1);
        selectTest("/*sql*/select 1;", 1);
        selectTest("/*sql*/select/*;*/ 1/*'*/;", 1);
        selectTest("/*sql*/select/*;*/ 1-- sql", 1);
        selectTest("/*sql*/select/*;*/ 1;select/*\"*/ 2-- sql", 2);
        
        updateTest("update t set a = 1", 1);
        updateTest("Update t set a = 1", 1);
        updateTest("updatE t set a = 1;/**/uPdate t set b=2;", 2);
        updateTest(" Update t set a = 1-- sql", 1);
        updateTest("/**/Update t set a = 1;", 1);
        updateTest("/*sql*/Update t set a = 1;", 1);
        updateTest("/*sql*/update/*;*/t set/*'*/a=1;", 1);
        updateTest("/*sql*/update/*;*/ t set a= 1-- sql", 1);
        updateTest("/*sql*/update/*;*/ t set a= 1;update t set/*\"*/ b=2-- sql", 2);
        
        insertTest("insert into t(a) values(1)", 1);
        insertTest("Insert into t(a) values(1)", 1);
        insertTest("inSerT into t(a) values(1);/**/insert into t(a) values(2);", 2);
        insertTest(" iNsert into t(a) values(1)-- sql", 1);
        insertTest("/**/insert into t(a) values(1);", 1);
        insertTest("/*sql*/insert into t(a) values(1);", 1);
        insertTest("/*sql*/insert/*;*/into t (a)/*'*/values(1);", 1);
        insertTest("/*sql*/insert/*;*/ into t(a) values(1)-- sql", 1);
        insertTest("/*sql*/insert/*;*/ into t(a) values(1); insert into t(a) /*\"*/values(2)-- sql", 2);
        
        deleteTest("delete from t where id =1", 1);
        deleteTest("Delete from t where id = 1", 1);
        deleteTest("deLete from t where id =1;/**/deletE from t where id = 2;", 2);
        deleteTest(" Delete from t where id =1-- sql", 1);
        deleteTest("/**/delete from t where id =1;", 1);
        deleteTest("/*sql*/delete from t where id =1;", 1);
        deleteTest("/*sql*/delete/*;*/from t /*'*/where id=1;", 1);
        deleteTest("/*sql*/delete/*;*/ from t where id=1-- sql", 1);
        deleteTest("/*sql*/delete/*;*/from t where id=1; DeleTe from t /*\"*/ where id=2-- sql", 2);
        
        txBeginTest("begin", 1);
        txBeginTest("begin;", 1);
        txBeginTest(" begin;", 1);
        txBeginTest("Begin;", 1);
        txBeginTest(" Begin ;", 1);
        txBeginTest("/*tx*/begin--", 1);
        txBeginTest("/*tx*/ begin--", 1);
        txBeginTest("/*tx*/begin --", 1);
        txBeginTest("/*tx*/begin/*tx*/--", 1);
        txBeginTest("/*tx*/begin/*tx*/--;", 1);
        txBeginTest("begin; begin", 2);
        txBeginTest("begin;Begin;", 2);
        txBeginTest(" begin;beGin;", 2);
        txBeginTest("Begin;/*tx*/begin;", 2);
        txBeginTest(" Begin ;begin;", 2);
        txBeginTest("/*tx*/begin;begin--;", 2);
        txBeginTest("begin;/*tx*/ begin--", 2);
        txBeginTest("Begin;/*tx*/begin --", 2);
        txBeginTest("begiN;/*tx*/begin/*tx*/--", 2);
        txBeginTest("begIn;/*tx*/begin/*tx*/--;", 2);
        txBeginTest("begIn transaction;/*tx*/begin/*tx*/--;", 2);
        
        txCommitTest("commit", 1);
        txCommitTest("commit transaction", 1);
        txCommitTest("commit;", 1);
        txCommitTest(" commit;", 1);
        txCommitTest("Commit;", 1);
        txCommitTest(" Commit ;", 1);
        txCommitTest("/*tx*/commit--", 1);
        txCommitTest("/*tx*/ commit--", 1);
        txCommitTest("/*tx*/commit --", 1);
        txCommitTest("/*tx*/commit/*tx*/--", 1);
        txCommitTest("/*tx*/commit/*tx*/--;", 1);
        txCommitTest("commit; commit", 2);
        txCommitTest("commit;Commit;", 2);
        txCommitTest(" commit;comMit;", 2);
        txCommitTest("Commit;/*tx*/commit;", 2);
        txCommitTest(" Commit ;commit;", 2);
        txCommitTest("/*tx*/commit;commit--;", 2);
        txCommitTest("commit;/*tx*/ commit--", 2);
        txCommitTest("commit;/*tx*/commit --", 2);
        txCommitTest("commiT;/*tx*/commit/*tx*/--", 2);
        txCommitTest("commIt;/*tx*/commit/*tx*/--;", 2);
        
        txEndTest("end", 1);
        txEndTest("End transaction", 1);
        txEndTest("end;", 1);
        txEndTest(" END;", 1);
        txEndTest("End;", 1);
        txEndTest(" End ;", 1);
        txEndTest("/*tx*/end--", 1);
        txEndTest("/*tx*/ end--", 1);
        txEndTest("/*tx*/end --", 1);
        txEndTest("/*tx*/end/*tx*/--", 1);
        txEndTest("/*tx*/end/*tx*/--;", 1);
        txEndTest("end; end", 2);
        txEndTest("end;End;", 2);
        txEndTest(" end;eNd;", 2);
        txEndTest("End;/*tx*/end;", 2);
        txEndTest(" End ;end;", 2);
        txEndTest("/*tx*/end;end--;", 2);
        txEndTest("end;/*tx*/ end--", 2);
        txEndTest("end;/*tx*/end transaction --", 2);
        txEndTest("enD transaction;/*tx*/end/*tx*/--", 2);
        txEndTest("end;/*tx*/end/*tx*/--;", 2);
        
        txSavepointTest("savepoint a", 1);
        txSavepointTest("savepoint 'a';", 1);
        txSavepointTest(" savepoint \"a\";", 1);
        txSavepointTest("Savepoint a;", 1);
        txSavepointTest(" Savepoint a ;", 1);
        txSavepointTest("/*tx*/savepoint a--", 1);
        txSavepointTest("/*tx*/ savepoint a--", 1);
        txSavepointTest("/*tx*/savepoint a --", 1);
        txSavepointTest("/*tx*/savepoint a/*tx*/--", 1);
        txSavepointTest("/*tx*/savepoint a/*tx*/--;", 1);
        txSavepointTest("savepoint a; savepoint b", 2);
        txSavepointTest("savepoint a;Savepoint b;", 2);
        txSavepointTest(" savepoint a;savEpoint b;", 2);
        txSavepointTest("Savepoint a;/*tx*/savepoint b;", 2);
        txSavepointTest(" Savepoint a ;savepoint b;", 2);
        txSavepointTest("/*tx*/savepoint a;savepoint b--;", 2);
        txSavepointTest("savepoint a;/*tx*/ savepoint b--", 2);
        txSavepointTest("savePoint a;/*tx*/savepoint b --", 2);
        txSavepointTest("sAvepoint a;/*tx*/savepoint b/*tx*/--", 2);
        txSavepointTest("saVepoint a;/*tx*/savepoint b/*tx*/--;", 2);
        
        txReleaseTest("release a", 1);
        txReleaseTest("release 'a';", 1);
        txReleaseTest(" release \"a\";", 1);
        txReleaseTest(" release savepoint \"a\";", 1);
        txReleaseTest("Release savepoint/*tx*/ a;", 1);
        txReleaseTest(" rElease a ;", 1);
        txReleaseTest("/*tx*/release a--", 1);
        txReleaseTest("/*tx*/ release a--", 1);
        txReleaseTest("/*tx*/release a --", 1);
        txReleaseTest("/*tx*/release a/*tx*/--", 1);
        txReleaseTest("/*tx*/release a/*tx*/--;", 1);
        txReleaseTest("release a; release b", 2);
        txReleaseTest("release a;Release b;", 2);
        txReleaseTest(" release a;Release savEpoint b;", 2);
        txReleaseTest("release a;/*tx*/release b;", 2);
        txReleaseTest(" release a ;release b;", 2);
        txReleaseTest("/*tx*/release savepoint a;release b--;", 2);
        txReleaseTest("release a;/*tx*/ release savepoint b--", 2);
        txReleaseTest("release a;/*tx*/release b --", 2);
        txReleaseTest("release a;/*tx*/release /*tx*/savepoint b/*tx*/--", 2);
        txReleaseTest("release a;/*tx*/release b/*tx*/--;", 2);
        
        txRollbackTest("rollback", 1, false);
        txRollbackTest("rollback to 'a';", 1, true);
        txRollbackTest(" Rollback to savepoint \"a\";", 1, true);
        txRollbackTest(" rollback to savepoint \"a\";", 1, true);
        txRollbackTest(" rollback transaction to savepoint \"a\";", 1, true);
        txRollbackTest("rOllback to/*tx*/ a;", 1, true);
        txRollbackTest(" roLlback ;", 1, false);
        txRollbackTest(" roLlback transaction;", 1, false);
        txRollbackTest("/*tx*/rollback to a--", 1, true);
        txRollbackTest("/*tx*/ rollback --", 1, false);
        txRollbackTest("/*tx*/rollback to a --", 1, true);
        txRollbackTest("/*tx*/rollback transaction to a --", 1, true);
        txRollbackTest("/*tx*/rollback to a/*tx*/--", 1, true);
        txRollbackTest("/*tx*/rollback/*tx*/--;", 1, false);
        txRollbackTest("rollback to a; rollback to b", 2, true);
        txRollbackTest("rollback;Rollback;", 2, false);
        txRollbackTest(" rollback to a;rollBack to savEpoint b;", 2, true);
        txRollbackTest("rollback;/*tx*/rollback;", 2, false);
        txRollbackTest(" rollback to a ;rollback to b;", 2, true);
        txRollbackTest("/*tx*/rollback to savepoint a;rollback to b--;", 2, true);
        txRollbackTest("rollback to a;/*tx*/ rollback to savepoint b--", 2, true);
        txRollbackTest("rollback;/*tx*/rollback --", 2, false);
        txRollbackTest("rollback to a;/*tx*/rollback to /*tx*/savepoint b/*tx*/--", 2, true);
        txRollbackTest("rollback to a;/*tx*/rollback to b/*tx*/--;", 2, true);
    }
    
    private void emptyTest(String sql) {
        SQLParser parser = new SQLParser(sql);
        SQLStatement stmt = parser.next();
        printfln("Test SQL %s", stmt);
        assertTrue(!stmt.isComment());
        assertTrue("".equals(stmt.getCommand()));
        assertTrue(stmt.isEmpty());
        assertTrue(!stmt.isQuery());
        assertTrue(!stmt.isTransaction());
    }
    
    private void commentTest(String sql) {
        SQLParser parser = new SQLParser(sql);
        SQLStatement stmt = parser.next();
        printfln("Test SQL %s", stmt);
        assertTrue(stmt.isComment());
        assertTrue("".equals(stmt.getCommand()));
        assertTrue(stmt.isEmpty());
        assertTrue(!stmt.isQuery());
        assertTrue(!stmt.isTransaction());
    }

    private void selectTest(String sqls, int stmts) {
        SQLParser parser = new SQLParser(sqls);
        int i = 0;
        for (SQLStatement stmt: parser) {
            printfln("Test SELECT %s", stmt);
            assertTrue("SELECT".equals(stmt.getCommand()));
            assertTrue(stmt.isQuery());
            assertTrue(!stmt.isEmpty());
            assertTrue(!stmt.isTransaction());
            assertTrue(!stmt.isComment());
            ++i;
            parser.remove();
        }
        assertTrue(i == stmts);
        assertTrue(!parser.hasNext());
        try {
            parser.next();
            fail();
        } catch (NoSuchElementException e) {
            // pass
        }
        parser.remove();
        try {
            parser.remove();
            fail();
        } catch (IllegalStateException e) {
            // pass
        }
    }
    
    private void updateTest(String sqls, int stmts) {
        SQLParser parser = new SQLParser(sqls);
        int i = 0;
        for (SQLStatement stmt: parser) {
            printfln("Test UPDATE %s", stmt);
            assertTrue("UPDATE".equals(stmt.getCommand()));
            assertTrue(!stmt.isQuery());
            assertTrue(!stmt.isEmpty());
            assertTrue(!stmt.isTransaction());
            assertTrue(!stmt.isComment());
            ++i;
            parser.remove();
        }
        assertTrue(i == stmts);
        assertTrue(!parser.hasNext());
        try {
            parser.next();
            fail();
        } catch (NoSuchElementException e) {
            // pass
        }
        parser.remove();
        try {
            parser.remove();
            fail();
        } catch (IllegalStateException e) {
            // pass
        }
    }
    
    private void insertTest(String sqls, int stmts) {
        SQLParser parser = new SQLParser(sqls);
        int i = 0;
        for (SQLStatement stmt: parser) {
            printfln("Test INSERT %s", stmt);
            assertTrue("INSERT".equals(stmt.getCommand()));
            assertTrue(!stmt.isQuery());
            assertTrue(!stmt.isEmpty());
            assertTrue(!stmt.isTransaction());
            assertTrue(!stmt.isComment());
            ++i;
            parser.remove();
        }
        assertTrue(i == stmts);
        assertTrue(!parser.hasNext());
        try {
            parser.next();
            fail();
        } catch (NoSuchElementException e) {
            // pass
        }
        parser.remove();
        try {
            parser.remove();
            fail();
        } catch (IllegalStateException e) {
            // pass
        }
    }
    
    private void deleteTest(String sqls, int stmts) {
        SQLParser parser = new SQLParser(sqls);
        int i = 0;
        for (SQLStatement stmt: parser) {
            printfln("Test DELETE %s", stmt);
            assertTrue("DELETE".equals(stmt.getCommand()));
            assertTrue(!stmt.isQuery());
            assertTrue(!stmt.isEmpty());
            assertTrue(!stmt.isTransaction());
            assertTrue(!stmt.isComment());
            ++i;
            parser.remove();
        }
        assertTrue(i == stmts);
        assertTrue(!parser.hasNext());
        try {
            parser.next();
            fail();
        } catch (NoSuchElementException e) {
            // pass
        }
        parser.remove();
        try {
            parser.remove();
            fail();
        } catch (IllegalStateException e) {
            // pass
        }
    }
    
    private void txBeginTest(String sqls, int stmts) {
        SQLParser parser = new SQLParser(sqls);
        int i = 0;
        for (SQLStatement stmt: parser) {
            printfln("Test BEGIN %s", stmt);
            assertTrue("BEGIN".equals(stmt.getCommand()));
            assertTrue(stmt instanceof TransactionStatement);
            assertTrue(!stmt.isQuery());
            assertTrue(!stmt.isEmpty());
            assertTrue(stmt.isTransaction());
            assertTrue(!stmt.isComment());
            TransactionStatement tx = (TransactionStatement)stmt;
            assertTrue(tx.isBegin());
            assertTrue(!tx.isCommit());
            assertTrue(!tx.isRelease());
            assertTrue(!tx.isRollback());
            assertTrue(!tx.isSavepoint());
            assertTrue(!tx.hasSavepoint());
            ++i;
            parser.remove();
        }
        assertTrue(i == stmts);
        assertTrue(!parser.hasNext());
        try {
            parser.next();
            fail();
        } catch (NoSuchElementException e) {
            // pass
        }
        parser.remove();
        try {
            parser.remove();
            fail();
        } catch (IllegalStateException e) {
            // pass
        }
    }
    
    private void txCommitTest(String sqls, int stmts) {
        SQLParser parser = new SQLParser(sqls);
        int i = 0;
        for (SQLStatement stmt: parser) {
            printfln("Test COMMIT %s", stmt);
            assertTrue("COMMIT".equals(stmt.getCommand()));
            assertTrue(stmt instanceof TransactionStatement);
            assertTrue(!stmt.isQuery());
            assertTrue(!stmt.isEmpty());
            assertTrue(stmt.isTransaction());
            assertTrue(!stmt.isComment());
            TransactionStatement tx = (TransactionStatement)stmt;
            assertTrue(!tx.isBegin());
            assertTrue(tx.isCommit());
            assertTrue(!tx.isRelease());
            assertTrue(!tx.isRollback());
            assertTrue(!tx.isSavepoint());
            assertTrue(!tx.hasSavepoint());
            ++i;
            parser.remove();
        }
        assertTrue(i == stmts);
        assertTrue(!parser.hasNext());
        try {
            parser.next();
            fail();
        } catch (NoSuchElementException e) {
            // pass
        }
        parser.remove();
        try {
            parser.remove();
            fail();
        } catch (IllegalStateException e) {
            // pass
        }
    }
    
    private void txEndTest(String sqls, int stmts) {
        SQLParser parser = new SQLParser(sqls);
        int i = 0;
        for (SQLStatement stmt: parser) {
            printfln("Test END %s", stmt);
            assertTrue("END".equals(stmt.getCommand()));
            assertTrue(stmt instanceof TransactionStatement);
            assertTrue(!stmt.isQuery());
            assertTrue(!stmt.isEmpty());
            assertTrue(stmt.isTransaction());
            assertTrue(!stmt.isComment());
            TransactionStatement tx = (TransactionStatement)stmt;
            assertTrue(!tx.isBegin());
            assertTrue(tx.isCommit());
            assertTrue(!tx.isRelease());
            assertTrue(!tx.isRollback());
            assertTrue(!tx.isSavepoint());
            assertTrue(!tx.hasSavepoint());
            ++i;
            parser.remove();
        }
        assertTrue(i == stmts);
        assertTrue(!parser.hasNext());
        try {
            parser.next();
            fail();
        } catch (NoSuchElementException e) {
            // pass
        }
        parser.remove();
        try {
            parser.remove();
            fail();
        } catch (IllegalStateException e) {
            // pass
        }
    }
    
    private void txSavepointTest(String sqls, int stmts) {
        SQLParser parser = new SQLParser(sqls);
        int i = 0;
        for (SQLStatement stmt: parser) {
            printfln("Test SAVEPOINT %s", stmt);
            assertTrue("SAVEPOINT".equals(stmt.getCommand()));
            assertTrue(stmt instanceof TransactionStatement);
            assertTrue(!stmt.isQuery());
            assertTrue(!stmt.isEmpty());
            assertTrue(stmt.isTransaction());
            assertTrue(!stmt.isComment());
            TransactionStatement tx = (TransactionStatement)stmt;
            assertTrue(!tx.isBegin());
            assertTrue(!tx.isCommit());
            assertTrue(!tx.isRelease());
            assertTrue(!tx.isRollback());
            assertTrue(tx.isSavepoint());
            assertTrue(tx.hasSavepoint());
            ++i;
            parser.remove();
        }
        assertTrue(i == stmts);
        assertTrue(!parser.hasNext());
        try {
            parser.next();
            fail();
        } catch (NoSuchElementException e) {
            // pass
        }
        parser.remove();
        try {
            parser.remove();
            fail();
        } catch (IllegalStateException e) {
            // pass
        }
    }
    
    private void txReleaseTest(String sqls, int stmts) {
        SQLParser parser = new SQLParser(sqls);
        int i = 0;
        for (SQLStatement stmt: parser) {
            printfln("Test RELEASE %s", stmt);
            assertTrue("RELEASE".equals(stmt.getCommand()));
            assertTrue(stmt instanceof TransactionStatement);
            assertTrue(!stmt.isQuery());
            assertTrue(!stmt.isEmpty());
            assertTrue(stmt.isTransaction());
            assertTrue(!stmt.isComment());
            TransactionStatement tx = (TransactionStatement)stmt;
            assertTrue(!tx.isBegin());
            assertTrue(!tx.isCommit());
            assertTrue(tx.isRelease());
            assertTrue(!tx.isRollback());
            assertTrue(!tx.isSavepoint());
            assertTrue(tx.hasSavepoint());
            ++i;
            parser.remove();
        }
        assertTrue(i == stmts);
        assertTrue(!parser.hasNext());
        try {
            parser.next();
            fail();
        } catch (NoSuchElementException e) {
            // pass
        }
        parser.remove();
        try {
            parser.remove();
            fail();
        } catch (IllegalStateException e) {
            // pass
        }
    }
    
    private void txRollbackTest(String sqls, int stmts, boolean hasSavepoint) {
        SQLParser parser = new SQLParser(sqls);
        int i = 0;
        for (SQLStatement stmt: parser) {
            printfln("Test ROLLBACK %s", stmt);
            assertTrue("ROLLBACK".equals(stmt.getCommand()));
            assertTrue(stmt instanceof TransactionStatement);
            assertTrue(!stmt.isQuery());
            assertTrue(!stmt.isEmpty());
            assertTrue(stmt.isTransaction());
            assertTrue(!stmt.isComment());
            TransactionStatement tx = (TransactionStatement)stmt;
            assertTrue(!tx.isBegin());
            assertTrue(!tx.isCommit());
            assertTrue(!tx.isRelease());
            assertTrue(tx.isRollback());
            assertTrue(!tx.isSavepoint());
            assertTrue(tx.hasSavepoint() == hasSavepoint);
            ++i;
            parser.remove();
        }
        assertTrue(i == stmts);
        assertTrue(!parser.hasNext());
        try {
            parser.next();
            fail();
        } catch (NoSuchElementException e) {
            // pass
        }
        parser.remove();
        try {
            parser.remove();
            fail();
        } catch (IllegalStateException e) {
            // pass
        }
    }

}
