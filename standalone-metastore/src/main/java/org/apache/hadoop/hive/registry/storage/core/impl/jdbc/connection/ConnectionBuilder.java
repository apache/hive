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
package org.apache.hadoop.hive.registry.storage.core.impl.jdbc.connection;

import java.io.Serializable;
import java.sql.Connection;

/**
 * Establishes the connection to a database, and exposes the connection configuration of the concrete implementation. <br>
 * The generic type {@code &lt;T&gt;} represents the implementation's configuration object. For example, for HikariCP
 * {@code T} would be {@code HikariConfig}
 *
 * @see HikariCPConnectionBuilder
 */
public interface ConnectionBuilder<T> extends Serializable {
    /**
     * method must be idempotent.
     */
    void prepare();

    /**
     * @return a database connection over which the queries can be executed.
     */
    Connection getConnection();

    /**
     * @return the configuration used to establish connection to the database
     */
    T getConfig();

    /**
     * called once when the system is shutting down, should be idempotent.
     */
    void cleanup();
}
