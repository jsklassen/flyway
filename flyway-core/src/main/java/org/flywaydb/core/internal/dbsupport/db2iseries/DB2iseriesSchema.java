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

import org.flywaydb.core.internal.dbsupport.Function;
import org.flywaydb.core.internal.dbsupport.JdbcTemplate;
import org.flywaydb.core.internal.dbsupport.Schema;
import org.flywaydb.core.internal.dbsupport.Table;
import org.flywaydb.core.internal.dbsupport.Type;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * DB2 implementation of Schema.
 */
public class DB2iseriesSchema extends Schema<DB2iseriesDbSupport> {
    /**
     * Creates a new DB2 schema.
     *
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param dbSupport    The database-specific support.
     * @param name         The name of the schema.
     */
    public DB2iseriesSchema(JdbcTemplate jdbcTemplate, DB2iseriesDbSupport dbSupport, String name) {
        super(jdbcTemplate, dbSupport, name);
    }


    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForInt("SELECT COUNT(*) FROM qsys2.schemata WHERE upper(schema_name)=upper(?)", name) > 0;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        int objectCount = jdbcTemplate.queryForInt("select count(*) from qsys2.systables where upper(system_table_schema) = upper(?)", name);
        objectCount += jdbcTemplate.queryForInt("select count(*) from qsys2.systables where upper(table_schema) = upper(?)", name);
        objectCount += jdbcTemplate.queryForInt("select count(*) from qsys2.syssequences where upper(sequence_schema) = upper(?)", name);
        objectCount += jdbcTemplate.queryForInt("select count(*) from qsys2.sysindexes where upper(table_schema) = upper(?)", name);
        objectCount += jdbcTemplate.queryForInt("select count(*) from qsys2.sysroutines where upper(specific_schema) = upper(?)", name);
        return objectCount == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        throw new UnsupportedOperationException("Create Schema - is not supported in db2 on iseries");
    }

    @Override
    protected void doDrop() throws SQLException {
        throw new UnsupportedOperationException("Drop Schema - is not supported in db2 on iseries");
    }

    @Override
    protected void doClean() throws SQLException {
        // MQTs are dropped when the backing views or tables are dropped
        // Indexes in DB2 are dropped when the corresponding table is dropped

        // views
        for (String dropStatement : generateDropStatements(name, "V", "VIEW")) {
            jdbcTemplate.execute(dropStatement);
        }

        // aliases
        for (String dropStatement : generateDropStatements(name, "A", "ALIAS")) {
            jdbcTemplate.execute(dropStatement);
        }

        for (Table table : allTables()) {
            table.drop();
        }

        // slett testtabeller
//        for (String dropStatement : generateDropStatementsForTestTable(name, "T", "TABLE")) {
//            jdbcTemplate.execute(dropStatement);
//        }


        // tablespace
       // for (String dropStatement : generateDropStatementsForTablespace(name)) {
        //    jdbcTemplate.execute(dropStatement);
        //}

        // sequences
        for (String dropStatement : generateDropStatementsForSequences(name)) {
            jdbcTemplate.execute(dropStatement);
        }

        // procedures
        for (String dropStatement : generateDropStatementsForProcedures(name)) {
            jdbcTemplate.execute(dropStatement);
        }

        // functions
        for (String dropStatement : generateDropStatementsForFunctions(name)) {
            jdbcTemplate.execute(dropStatement);
        }

        // usertypes
//        for (String dropStatement : generateDropStatementsForUserTypes(name)) {
//            jdbcTemplate.execute(dropStatement);
//        }
    }

    /**
     * Generates DROP statements for the procedures in this schema.
     *
     * @param schema The schema of the objects.
     * @return The drop statements.
     * @throws java.sql.SQLException when the statements could not be generated.
     */
    //CAST_FUNCTION =function_type?
    private List<String> generateDropStatementsForProcedures(String schema) throws SQLException {
        String dropProcGenQuery = "select rtrim(routine_name) from qsys2.SYSROUTINES where function_type = 'N' " +
                " and ROUTINE_TYPE  = 'P' and routine_schema = '" + schema + "'";
        return buildDropStatements("DROP PROCEDURE", dropProcGenQuery, schema);
    }

    /**
     * Generates DROP statements for the functions in this schema.
     *
     * @param schema The schema of the objects.
     * @return The drop statements.
     * @throws java.sql.SQLException when the statements could not be generated.
     */
      //CAST_FUNCTION =function_type?
    private List<String> generateDropStatementsForFunctions(String schema) throws SQLException {
        String dropProcGenQuery = "select rtrim(routine_name) from qsys2.SYSROUTINES where function_type = 'N' " +
                " and ROUTINE_TYPE  = 'F' and routine_schema = '" + schema + "'";
        return buildDropStatements("DROP FUNCTION", dropProcGenQuery, schema);
    }

    /**
     * Generates DROP statements for the sequences in this schema.
     *
     * @param schema The schema of the objects.
     * @return The drop statements.
     * @throws java.sql.SQLException when the statements could not be generated.
     */
    private List<String> generateDropStatementsForSequences(String schema) throws SQLException {
        String dropSeqGenQuery = "select rtrim(sequence_name) from qsys2.SYSSEQUENCES where sequence_schema = '" + schema
                + "' and SEQTYPE='S'";
        return buildDropStatements("DROP SEQUENCE", dropSeqGenQuery, schema);
    }

    /**
     * Generates DROP statements for the tablespace in this schema.
     *
     * @param schema The schema of the objects.
     * @return The drop statements.
     * @throws java.sql.SQLException when the statements could not be generated.
     */
    //
    //private List<String> generateDropStatementsForTablespace(String schema) throws SQLException {
//        String dropTablespaceGenQuery = "select rtrim(NAME) FROM qsys2.SYSTABLESPACE where DBNAME = '" + schema + "'";
//        return buildDropStatements("DROP TABLESPACE", dropTablespaceGenQuery, schema);
    //}


    /**
     * Generates DROP statements for this type of table, representing this type of object in this schema.
     *
     * @param schema     The schema of the objects.
     * @param tableType  The type of table (Can be T, V, S, ...).
     * @param objectType The type of object.
     * @return The drop statements.
     * @throws java.sql.SQLException when the statements could not be generated.
     */
    //private List<String> generateDropStatementsForTestTable(String schema, String tableType, String objectType) throws SQLException {
//        String dropTablesGenQuery = "select rtrim(NAME) from qsys2.systables where TYPE='" + tableType + "' and creator = '"
//                + schema + "'";
//        return buildDropStatements("DROP " + objectType, dropTablesGenQuery, schema);
   // }

    /**
     * Generates DROP statements for the user defines types in this schema.
     *
     * @param schema The schema of the objects.
     * @return The drop statements.
     * @throws java.sql.SQLException when the statements could not be generated.
     */
//    private List<String> generateDropStatementsForUserTypes(String schema) throws SQLException {
//        String dropTablespaceGenQuery = "select rtrim(NAME) from qsys2.SYSDATATYPES where schema = '" + schema + "'";
//        return buildDropStatements("DROP TYPE", dropTablespaceGenQuery, schema);
//    }

    /**
     * Generates DROP statements for this type of table, representing this type of object in this schema.
     *
     * @param schema     The schema of the objects.
     * @param tableType  The type of table (Can be T, V, S, ...).
     * @param objectType The type of object.
     * @return The drop statements.
     * @throws java.sql.SQLException when the statements could not be generated.
     */
    private List<String> generateDropStatements(String schema, String tableType, String objectType) throws SQLException {
        String dropTablesGenQuery = "select rtrim(table_name) from qsys2.systables where table_type='" + tableType + "' and (table_schema = '"
                + schema + "' OR system_table_schema = '" + schema + "')";
        return buildDropStatements("DROP " + objectType, dropTablesGenQuery, schema);
    }

    /**
     * Builds the drop statements for database objects in this schema.
     *
     * @param dropPrefix The drop command for the database object (e.g. 'drop table').
     * @param query      The query to get all present database objects
     * @param schema     The schema for which to build the statements.
     * @return The statements.
     * @throws java.sql.SQLException when the drop statements could not be built.
     */
    private List<String> buildDropStatements(final String dropPrefix, final String query, String schema) throws SQLException {
        List<String> dropStatements = new ArrayList<String>();
        List<String> dbObjects = jdbcTemplate.queryForStringList(query);
        for (String dbObject : dbObjects) {
            dropStatements.add(dropPrefix + " " + dbSupport.quote(schema, dbObject));
        }
        return dropStatements;
    }

    @Override
    protected Table[] doAllTables() throws SQLException {
        List<String> tableNames = jdbcTemplate.queryForStringList(
                "select rtrim(table_name) from qsys2.systables where table_type='T' and table_schema = ?", name);
        Table[] tables = new Table[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            tables[i] = new DB2iseriesTable(jdbcTemplate, dbSupport, this, tableNames.get(i));
        }
        return tables;
    }

    @Override
    public Table getTable(String tableName) {
        return new DB2iseriesTable(jdbcTemplate, dbSupport, this, tableName);
    }

    @Override
    protected Type getType(String typeName) {
        return new DB2iseriesType(jdbcTemplate, dbSupport, this, typeName);
    }

    @Override
    public Function getFunction(String functionName, String... args) {
        return new DB2iseriesFunction(jdbcTemplate, dbSupport, this, functionName, args);
    }
}
