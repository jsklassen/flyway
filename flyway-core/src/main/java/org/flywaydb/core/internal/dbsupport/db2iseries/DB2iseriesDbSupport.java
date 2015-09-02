/**
 * Copyright 2010-2015 Axel Fontaine
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.internal.dbsupport.db2iseries;

import org.flywaydb.core.internal.dbsupport.DbSupport;
import org.flywaydb.core.internal.dbsupport.JdbcTemplate;
import org.flywaydb.core.internal.dbsupport.Schema;
import org.flywaydb.core.internal.dbsupport.SqlStatementBuilder;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

/**
 * DB2 Support.
 */
public class DB2iseriesDbSupport extends DbSupport {
    /**
     * Creates a new instance.
     *
     * @param connection The connection to use.
     */
    public DB2iseriesDbSupport(Connection connection) {
//        connection.setAutoCommit(true);
        super(new JdbcTemplate(connection, Types.VARCHAR));
    }

    public String getDbName() {
        return "db2iseries";
    }

    public SqlStatementBuilder createSqlStatementBuilder() {
        return new DB2iseriesSqlStatementBuilder();
    }

    public String getScriptLocation() {
        return "com/googlecode/flyway/core/dbsupport/db2iseries/";
    }

    @Override
    protected String doGetCurrentSchema() throws SQLException {
        return jdbcTemplate.queryForString("select current_schema from sysibm.sysdummy1");
    }

    @Override
    protected void doSetCurrentSchema(Schema schema) throws SQLException {
        jdbcTemplate.execute("SET SCHEMA " + schema);
    }

    public String getCurrentUserFunction() {
		return "USER";
    }

    public boolean supportsDdlTransactions() {
        return true;
    }

    public String getBooleanTrue() {
        return "1";
    }

    public String getBooleanFalse() {
        return "0";
    }

    @Override
    public String doQuote(String identifier) {
//        return "\"" + identifier + "\"";
        return identifier;
    }

    @Override
    public Schema getSchema(String name) {
        return new DB2iseriesSchema(jdbcTemplate, this, name);
    }

    @Override
    public boolean catalogIsSchema() {
        return false;
    }
}
