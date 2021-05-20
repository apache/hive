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

public class IcebergTableUtil {

  private IcebergTableUtil() {

  }

  /**
   * Load the iceberg table either from the {@link QueryState} or through the configured catalog.
   * @param configuration a Hadoop configuration
   * @param properties controlling properties
   * @return
   */
  static Table getTable(Configuration configuration, Properties properties) {
    // look for the table object stored in the query state. If it's null, it means the table was not loaded yet
    // within the same query therefore we claim it through the Catalogs API and then store it in query state.
    QueryState queryState = (QueryState) SessionState.get()
        .getQueryState(configuration.get(HiveConf.ConfVars.HIVEQUERYID.varname));
    Table table = null;
    if (queryState != null) {
      table = (Table) queryState.getTable(properties.getProperty(Catalogs.NAME));
    }
    if (table == null) {
      table = Catalogs.loadTable(configuration, properties);
      if (queryState != null) {
        queryState.addTable(properties.getProperty(Catalogs.NAME), table);
      }
    }
    return table;
  }
}
