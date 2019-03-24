/**
 * A SQLite server based on the C/S architecture, little-pan (c) 2019
 */
package org.sqlite.jdbc.v3;

import java.sql.SQLException;
import java.sql.Savepoint;

/**
 * @author little-pan
 * @since 2019-03-24
 *
 */
public class JDBC3Savepoint implements Savepoint {

    final int id;

    final String name;

    public JDBC3Savepoint(int id) {
        this.id = id;
        this.name = null;
    }

    public JDBC3Savepoint(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getSavepointId() throws SQLException {
        return id;
    }

    public String getSavepointName() throws SQLException {
        return name == null ? String.format("SQLITE_SAVEPOINT_%s", id) : name;
    }

}
