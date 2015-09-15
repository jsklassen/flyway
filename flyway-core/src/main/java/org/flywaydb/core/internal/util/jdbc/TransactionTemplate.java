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
package org.flywaydb.core.internal.util.jdbc;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.util.logging.Log;
import org.flywaydb.core.internal.util.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Spring-like template for executing transactions.
 */
public class TransactionTemplate {
    private static final Log LOG = LogFactory.getLog(TransactionTemplate.class);

    /**
     * The connection for the transaction.
     */
    private final Connection connection;

    /**
     * Whether to commit or keep building up statements in a transaction.
     */
    private final boolean commitOnSuccess;

    /**
     * Whether to roll back the transaction when an exception is thrown.
     */
    private final boolean rollbackOnException;

//    /**
//     * Creates a new transaction template for this connection.
//     *
//     * @param connection The connection for the transaction.
//     */
//    public TransactionTemplate(Connection connection) {
//        this(connection, true, true);
//    }

    /**
     * Creates a new transaction template for this connection.
     *
     * @param connection          The connection for the transaction.
     * @param rollbackOnException Whether to roll back the transaction when an exception is thrown.
     * @param commitOnSuccess
     */
    public TransactionTemplate(Connection connection, boolean rollbackOnException, boolean commitOnSuccess) {
        this.connection = connection;
        this.rollbackOnException = rollbackOnException;
        this.commitOnSuccess = commitOnSuccess;
    }

    /**
     * Executes this callback within a transaction.
     *
     * @param transactionCallback The callback to execute.
     * @return The result of the transaction code.
     */
    public <T> T execute(TransactionCallback<T> transactionCallback) {
        boolean oldAutocommit = true;
        try {
            oldAutocommit = connection.getAutoCommit();
            connection.setTransactionIsolation(0);
            LOG.debug("Autocommit was " + oldAutocommit);
            connection.setAutoCommit(false);
            LOG.debug("Autocommit was temporarily set to " + connection.getAutoCommit());
            LOG.debug("Beginning transaction..."+ transactionCallback.hashCode());
            connection.createStatement().execute("set transaction isolation level no COMMIT");
            T result = transactionCallback.doInTransaction();
            if (commitOnSuccess) {
                connection.commit();
                LOG.debug("Committing transaction..."+ transactionCallback.hashCode());
            }
            return result;
        } catch (SQLException e) {
            throw new FlywayException("Unable to commit transaction"+ transactionCallback.hashCode(), e);
        } catch (RuntimeException e) {
            if (rollbackOnException) {
                try {
                    LOG.debug("Rolling back transaction..." + transactionCallback.hashCode());
                    connection.rollback();
                    LOG.debug("Transaction rolled back"+ transactionCallback.hashCode());
                } catch (SQLException se) {
                    LOG.error("Unable to rollback transaction"+ transactionCallback.hashCode(), se);
                }
            } else if (commitOnSuccess) {
                try {
                    connection.commit();
                    //this is not hit for rollbackOnSuccess
                    LOG.debug("Committing transaction..." + transactionCallback.hashCode());
                } catch (SQLException se) {
                    LOG.error("Unable to commit transaction" + transactionCallback.hashCode(), se);
                }
            }
            throw e;
        }
        finally {
            try {
                connection.setAutoCommit(oldAutocommit);
            } catch (SQLException e) {
                LOG.error("Unable to restore autocommit to original value for connection", e);
            }
        }
    }
}
