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

import java.util.List;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.parse.PartitionTransform;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
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
   * @return an Iceberg table
   */
  static Table getTable(Configuration configuration, Properties properties) {
    Table table = null;
    QueryState queryState = null;
    String tableIdentifier = properties.getProperty(Catalogs.NAME);
    if (SessionState.get() != null) {
      queryState = SessionState.get().getQueryState(configuration.get(HiveConf.ConfVars.HIVEQUERYID.varname));
      if (queryState != null) {
        table = (Table) queryState.getResource(tableIdentifier);
      } else {
        LOG.debug("QueryState is not available in SessionState. Loading {} from configured catalog.", tableIdentifier);
      }
    } else {
      LOG.debug("SessionState is not available. Loading {} from configured catalog.", tableIdentifier);
    }

    if (table == null) {
      table = Catalogs.loadTable(configuration, properties);
      if (queryState != null) {
        queryState.addResource(tableIdentifier, table);
      }
    }

    return table;
  }

  /**
   * Create {@link PartitionSpec} based on the partition information stored in
   * {@link org.apache.hadoop.hive.ql.parse.PartitionTransform.PartitionTransformSpec}.
   * @param schema iceberg table schema
   * @param partitionTransformSpecList partition transform metadata
   * @return iceberg partition spec, always non-null
   */
  public static PartitionSpec spec(Schema schema,
      List<PartitionTransform.PartitionTransformSpec> partitionTransformSpecList) {
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    partitionTransformSpecList.forEach(spec -> {
      switch (spec.transformType) {
        case IDENTITY:
          builder.identity(spec.name);
          break;
        case YEAR:
          builder.year(spec.name);
          break;
        case MONTH:
          builder.month(spec.name);
          break;
        case DAY:
          builder.day(spec.name);
          break;
        case HOUR:
          builder.hour(spec.name);
          break;
        case TRUNCATE:
          builder.truncate(spec.name, spec.transformParam);
          break;
        case BUCKET:
          builder.bucket(spec.name, spec.transformParam);
          break;
      }
    });
    return builder.build();
  }
}
