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
package org.sqlite.server.orm;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.hibernate.*;
import org.hibernate.cfg.*;
import org.sqlite.TestDbBase;

/**
 * @author little-pan
 * @since 2019-12-01
 *
 */
public class HibernateTest extends TestDbBase {
    
    protected SessionFactory sessionFactory;
    
    public static void main(String args[]) throws SQLException {
        new HibernateTest().test();
    }

    @Override
    protected void doTest() throws SQLException {
        try (Connection conn = getConnection()) {
            prepare(conn);
            
            Session session = openSession(conn);
            try {
                // Case-1: INSERT
                final Date createTime = new Date();
                Account bean = saveAccountTest(session, createTime);
                BigDecimal ba = bean.getBalance();
                assertTrue(bean.getId() == 1L);
                
                // Case-2: UPDATE
                final int addedAmount = 500;
                bean = updateAccountTest(session, bean.getId(), addedAmount);
                BigDecimal bb = bean.getBalance();
                assertTrue(bb.compareTo(ba.add(new BigDecimal(addedAmount))) == 0);
                
                // Case-3.1: SELECT by get()
                bean = (Account)session.get(Account.class,  bean.getId());
                bb = bean.getBalance();
                assertTrue(bb.compareTo(ba.add(new BigDecimal(addedAmount))) == 0);
                assertTrue(createTime.equals(bean.getCreateTime()));
                // Case-3.2: SELECT by get() and lock
                bean = (Account)session.get(Account.class,  bean.getId(), LockOptions.UPGRADE);
                bb = bean.getBalance();
                assertTrue(bb.compareTo(ba.add(new BigDecimal(addedAmount))) == 0);
                assertTrue(createTime.equals(bean.getCreateTime()));
                // Case-3.3: SELECT by HQL
                bean = (Account)session.createQuery("from Account a where a.id = :id")
                .setParameter("id", bean.getId())
                .uniqueResult();
                bb = bean.getBalance();
                assertTrue(bb.compareTo(ba.add(new BigDecimal(addedAmount))) == 0);
                assertTrue(createTime.equals(bean.getCreateTime()));
                // Case-3.4: SELECT by HQL and lock
                Transaction tx = session.beginTransaction();
                bean = (Account)session.createQuery("from Account a where a.id = :id")
                .setParameter("id", bean.getId())
                .setLockOptions(LockOptions.UPGRADE)
                .uniqueResult();
                bb = bean.getBalance();
                assertTrue(bb.compareTo(ba.add(new BigDecimal(addedAmount))) == 0);
                assertTrue(createTime.equals(bean.getCreateTime()));
                tx.commit();
                
                // Case-4: DELETE
                deleteAccountTest(session, bean);
                bean = (Account)session.get(Account.class,  bean.getId());
                assertTrue(bean == null);
                
            } finally {
                session.close();
                this.sessionFactory.close();
                this.sessionFactory = null;
            }
        }
    }
    
    private void deleteAccountTest(Session session, Account bean) {
        session.delete(bean);
    }

    private Account updateAccountTest(Session session, Long id, int addedAmount) {
        Transaction tx = session.beginTransaction();
        boolean failed = true;
        try {
            tx.begin();
            Account bean = (Account)session.get(Account.class, id);
            bean.setBalance(bean.getBalance().add(new BigDecimal(addedAmount)));
            tx.commit();
            failed = false;
            return bean;
        } finally {
            if (failed) {
                tx.rollback();
            }
        }
    }

    private Account saveAccountTest(Session session, Date createTime) {
        Account bean = new Account();
        bean.setName("little-pan");
        bean.setBalance(new BigDecimal(100000));
        bean.setCreateTime(createTime);
        session.save(bean);
        return bean;
    }
    
    private void prepare(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("drop table if exists accounts");
            
            stmt.executeUpdate("create table accounts("
                    + "id integer primary key autoincrement,"
                    + "name varchar(50) not null,"
                    + "balance decimal(12, 2) not null,"
                    + "create_time datetime not null)");
        }
    }

    protected Session openSession(Connection conn) {
        if (this.sessionFactory == null) {
            this.sessionFactory = buildSessionFactory();
        }
        return this.sessionFactory.openSession(conn);
    }
    
    protected SessionFactory buildSessionFactory() {
        Properties props = new Properties();
        props.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        // Force to use generated key instead of sequence in SQLite server!
        props.setProperty("hibernate.jdbc.use_get_generated_keys", "true");
        
        Configuration cfg = new Configuration();
        cfg.addAnnotatedClass(Account.class)
        .addProperties(props);
        
        return cfg.buildSessionFactory();
    }

}
