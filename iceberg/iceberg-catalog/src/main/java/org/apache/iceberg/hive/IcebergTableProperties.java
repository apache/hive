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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class IcebergTableProperties {
  public static final String NAME = "name";
  public static final String LOCATION = "location";
  public static final String TABLE_DEFAULT_CONFIG_PREFIX = "iceberg.table-default.";
  public static final Set<String> PROPERTIES_TO_REMOVE = ImmutableSet.of(
      // We don't want to push down the metadata location props to Iceberg from HMS,
      // since the snapshot pointer in HMS would always be one step ahead
      BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
      BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP);

  private IcebergTableProperties() {

  }

  /**
   * Calculates the Iceberg table properties.
   * <ul>
   * <li>The base of the properties is the properties stored at the Hive Metastore for the given table
   * <li>We add the {@link IcebergTableProperties#LOCATION} as the table location
   * <li>We add the {@link IcebergTableProperties#NAME} as
   * TableIdentifier defined by the database name and table name
   * <li>We add the serdeProperties of the HMS table
   * <li>We remove some parameters that we don't want to push down to the Iceberg table props
   * </ul>
   * @param hmsTable Table for which we are calculating the properties
   * @return The properties we can provide for Iceberg functions
   */
  public static Properties getTableProperties(org.apache.hadoop.hive.metastore.api.Table hmsTable, Configuration conf) {
    Properties properties = new Properties();
    overrideIcebergDefaults(properties);

    getTableProperties(conf, IcebergCatalogProperties.getCatalogName(conf))
        .forEach(properties::setProperty);

    properties.putAll(toIcebergProperties(hmsTable.getParameters()));

    if (hmsTable.getSd() != null && hmsTable.getSd().getLocation() != null) {
      properties.putIfAbsent(LOCATION, hmsTable.getSd().getLocation());
    }

    properties.putIfAbsent(NAME, TableIdentifier.of(hmsTable.getDbName(), hmsTable.getTableName()).toString());

    SerDeInfo serdeInfo = hmsTable.getSd().getSerdeInfo();
    if (serdeInfo != null) {
      properties.putAll(toIcebergProperties(serdeInfo.getParameters()));
    }

    // Remove HMS table parameters we don't want to propagate to Iceberg
    PROPERTIES_TO_REMOVE.forEach(properties::remove);

    return properties;
  }

  private static void overrideIcebergDefaults(Properties properties) {
    properties.put(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true");
  }

  private static Properties toIcebergProperties(Map<String, String> parameters) {
    Properties properties = new Properties();
    parameters.entrySet().stream()
        .filter(e -> e.getKey() != null && e.getValue() != null)
        .forEach(e -> {
          String icebergKey = HMSTablePropertyHelper.translateToIcebergProp(e.getKey());
          properties.put(icebergKey, e.getValue());
        });
    return properties;
  }

  /**
   * Collect all the table specific configuration from the global hive configuration.
   * @param conf a Hadoop configuration
   * @param catalogName name of the catalog
   * @return complete map of catalog properties
   */
  public static Map<String, String> getTableProperties(Configuration conf, String catalogName) {
    Map<String, String> tableProperties = Maps.newHashMap();
    String namedCatalogTablePrefix = IcebergCatalogProperties.CATALOG_CONFIG_PREFIX + catalogName + ".table-default.";

    conf.forEach(config -> {
      if (config.getKey().startsWith(IcebergTableProperties.TABLE_DEFAULT_CONFIG_PREFIX)) {
        tableProperties.putIfAbsent(
            config.getKey().substring(IcebergTableProperties.TABLE_DEFAULT_CONFIG_PREFIX.length()),
            config.getValue());
      } else if (config.getKey().startsWith(namedCatalogTablePrefix)) {
        tableProperties.put(
            config.getKey().substring(namedCatalogTablePrefix.length()),
            config.getValue());
      }
    });

    return tableProperties;
  }
}
