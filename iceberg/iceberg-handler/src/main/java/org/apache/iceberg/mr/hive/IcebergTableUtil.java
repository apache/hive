/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.iceberg.Table;
import org.apache.iceberg.mr.Catalogs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergTableUtil {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableUtil.class);

  private IcebergTableUtil() {

  }

  /**
   * Load the iceberg table either from the {@link QueryState} or through the configured catalog. Look for the table
   * object stored in the query state. If it's null, it means the table was not loaded yet within the same query
   * therefore we claim it through the Catalogs API and then store it in query state.
   * @param configuration a Hadoop configuration
   * @param properties controlling properties
   * @return
   */
  static Table getTable(Configuration configuration, Properties properties) {
    QueryState queryState = SessionState.get()
        .getQueryState(configuration.get(HiveConf.ConfVars.HIVEQUERYID.varname));
    Table table = null;
    String tableIdentifier = properties.getProperty(Catalogs.NAME);
    if (queryState != null) {
      table = (Table) queryState.getResource(tableIdentifier);
    } else {
      LOG.debug("QueryState is not available in SessionState. Loading {} from configured catalog.", tableIdentifier);
    }
    if (table == null) {
      table = Catalogs.loadTable(configuration, properties);
      if (queryState != null) {
        queryState.addResource(tableIdentifier, table);
      }
    }
    return table;
  }
}
