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
package org.apache.hadoop.hive.metastore.txn.retryhandling;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Objects;

/**
 * Wraps the datasource and allows accees of the {@link NamedParameterJdbcTemplate}, {@link PlatformTransactionManager}
 * {@link Connection} objects associated with the wrapped datasource.
 */
public class DataSourceWrapper {
  
  private final DataSource dataSource;
  private final PlatformTransactionManager transactionManager;
  private final NamedParameterJdbcTemplate jdbcTemplate;

  /**
   * @return Returns the {@link PlatformTransactionManager} associated with the wrapped {@link DataSource}. Can be used to 
   * handle transactions programatically.
   */
  public PlatformTransactionManager getTransactionManager() {
    return transactionManager;
  }

  /**
   * @return Returns the {@link NamedParameterJdbcTemplate} associated with the wrapped {@link DataSource}.
   */
  public NamedParameterJdbcTemplate getJdbcTemplate() {
    return jdbcTemplate;
  }

  /**
   * Gets a connection form to the wrapped {@link DataSource}. Ensures that the same instance is returned all the time
   * within a particular transaction ({@link DataSourceUtils} is using {@link ThreadLocal}) to achieve it. 
   * @return Returns a connection to the datasource.
   */
  public Connection getConnection() {
    return DataSourceUtils.getConnection(dataSource);
  }

  /**
   * @return Returns a new {@link NamedParameterJdbcTemplate} instead of the default one. Useful if there is a need for 
   * custom fetch-size, max-rows or other specific values.
   */
  public NamedParameterJdbcTemplate createNewJdbcTemplate() {
    return new NamedParameterJdbcTemplate(dataSource);
  }

  /**
   * Creates a new isntance of the {@link DataSourceWrapper} class
   * @param dataSource The {@link DataSource} to wrap.
   */
  public DataSourceWrapper(DataSource dataSource) {
    this.dataSource = dataSource;
    this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
    this.transactionManager = new DataSourceTransactionManager(Objects.requireNonNull(jdbcTemplate.getJdbcTemplate().getDataSource()));
  }
}
