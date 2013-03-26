/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hcatalog.hbase.snapshot;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * This interface provides APIs for implementing revision management.
 */
public interface RevisionManager {

    public static final String REVISION_MGR_IMPL_CLASS = "revision.manager.impl.class";

    /**
     * Initialize the revision manager.
     */
    public void initialize(Properties properties);

    /**
     * Opens the revision manager.
     *
     * @throws IOException
     */
    public void open() throws IOException;

    /**
     * Closes the revision manager.
     *
     * @throws IOException
     */
    public void close() throws IOException;

    /**
     * Start the write transaction.
     *
     * @param table
     * @param families
     * @return
     * @throws IOException
     */
    public Transaction beginWriteTransaction(String table, List<String> families)
            throws IOException;

    /**
     * Start the write transaction.
     *
     * @param table
     * @param families
     * @param keepAlive
     * @return
     * @throws IOException
     */
    public Transaction beginWriteTransaction(String table,
            List<String> families, long keepAlive) throws IOException;

    /**
     * Commit the write transaction.
     *
     * @param transaction
     * @throws IOException
     */
    public void commitWriteTransaction(Transaction transaction)
            throws IOException;

    /**
     * Abort the write transaction.
     *
     * @param transaction
     * @throws IOException
     */
    public void abortWriteTransaction(Transaction transaction)
            throws IOException;

    /**
     * Create the latest snapshot of the table.
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public TableSnapshot createSnapshot(String tableName) throws IOException;

    /**
     * Create the snapshot of the table using the revision number.
     *
     * @param tableName
     * @param revision
     * @return
     * @throws IOException
     */
    public TableSnapshot createSnapshot(String tableName, long revision)
            throws IOException;

    /**
     * Extends the expiration of a transaction by the time indicated by keep alive.
     *
     * @param transaction
     * @throws IOException
     */
    public void keepAlive(Transaction transaction) throws IOException;

}
