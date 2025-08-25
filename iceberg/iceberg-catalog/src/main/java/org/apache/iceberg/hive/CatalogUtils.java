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

package org.apache.iceberg.hive;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

public class CatalogUtils {
  public static final String NAME = "name";
  public static final String LOCATION = "location";
  public static final String CATALOG_CONFIG_PREFIX = "iceberg.catalog.";
  public static final String CUSTOM_CATALOG_CONFIG_PREFIX = "iceberg.%s-catalog";
  public static final String CATALOG_CONFIG_TYPE = CATALOG_CONFIG_PREFIX + "type";
  public static final String CATALOG_WAREHOUSE_TEMPLATE = CUSTOM_CATALOG_CONFIG_PREFIX + ".warehouse";
  public static final String CATALOG_DEFAULT_CONFIG_PREFIX = "iceberg.catalog-default.";
  public static final Set<String> PROPERTIES_TO_REMOVE = ImmutableSet.of(
      // We don't want to push down the metadata location props to Iceberg from HMS,
      // since the snapshot pointer in HMS would always be one step ahead
      BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
      BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP);

  private CatalogUtils() {

  }

  /**
   * Calculates the properties we would like to send to the catalog.
   * <ul>
   * <li>The base of the properties is the properties stored at the Hive Metastore for the given table
   * <li>We add the {@link CatalogUtils#LOCATION} as the table location
   * <li>We add the {@link CatalogUtils#NAME} as
   * TableIdentifier defined by the database name and table name
   * <li>We add the serdeProperties of the HMS table
   * <li>We remove some parameters that we don't want to push down to the Iceberg table props
   * </ul>
   * @param hmsTable Table for which we are calculating the properties
   * @return The properties we can provide for Iceberg functions
   */
  public static Properties getCatalogProperties(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    Properties properties = new Properties();
    properties.putAll(toIcebergProperties(hmsTable.getParameters()));

    if (properties.get(LOCATION) == null && hmsTable.getSd() != null &&
        hmsTable.getSd().getLocation() != null) {
      properties.put(LOCATION, hmsTable.getSd().getLocation());
    }

    if (properties.get(NAME) == null) {
      properties.put(NAME, TableIdentifier.of(hmsTable.getDbName(),
          hmsTable.getTableName()).toString());
    }

    SerDeInfo serdeInfo = hmsTable.getSd().getSerdeInfo();
    if (serdeInfo != null) {
      properties.putAll(toIcebergProperties(serdeInfo.getParameters()));
    }

    // Remove HMS table parameters we don't want to propagate to Iceberg
    PROPERTIES_TO_REMOVE.forEach(properties::remove);

    return properties;
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
}
