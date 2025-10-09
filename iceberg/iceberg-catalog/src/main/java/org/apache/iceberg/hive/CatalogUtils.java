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
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class CatalogUtils {
  public static final String NAME = "name";
  public static final String LOCATION = "location";
  public static final String CATALOG_NAME = "iceberg.catalog";
  public static final String CATALOG_CONFIG_PREFIX = "iceberg.catalog.";
  public static final String CATALOG_WAREHOUSE_TEMPLATE = "iceberg.catalog.%s.warehouse";
  public static final String CATALOG_IMPL_TEMPLATE = "iceberg.catalog.%s.catalog-impl";
  public static final String CATALOG_DEFAULT_CONFIG_PREFIX = "iceberg.catalog-default.";
  public static final String ICEBERG_HADOOP_TABLE_NAME = "location_based_table";
  public static final String ICEBERG_DEFAULT_CATALOG_NAME = "default_iceberg";
  public static final String NO_CATALOG_TYPE = "no catalog";
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

  /**
   * Collect all the catalog specific configuration from the global hive configuration.
   * @param conf a Hadoop configuration
   * @param catalogName name of the catalog
   * @return complete map of catalog properties
   */
  public static Map<String, String> getCatalogProperties(Configuration conf, String catalogName) {
    Map<String, String> catalogProperties = Maps.newHashMap();
    String keyPrefix = CATALOG_CONFIG_PREFIX + catalogName;
    conf.forEach(config -> {
      if (config.getKey().startsWith(CatalogUtils.CATALOG_DEFAULT_CONFIG_PREFIX)) {
        catalogProperties.putIfAbsent(
            config.getKey().substring(CatalogUtils.CATALOG_DEFAULT_CONFIG_PREFIX.length()),
            config.getValue());
      } else if (config.getKey().startsWith(keyPrefix)) {
        catalogProperties.put(
            config.getKey().substring(keyPrefix.length() + 1),
            config.getValue());
      }
    });

    return catalogProperties;
  }

  public static String getCatalogName(Configuration conf) {
    return MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT);
  }

  public static String getCatalogType(Configuration conf) {
    return getCatalogType(conf, CatalogUtils.getCatalogName(conf));
  }

  public static boolean isHadoopTable(Configuration conf, Properties catalogProperties) {
    String catalogName = catalogProperties.getProperty(CATALOG_NAME);
    return ICEBERG_HADOOP_TABLE_NAME.equals(catalogName) || hadoopCatalog(conf, catalogProperties);
  }

  public static boolean hadoopCatalog(Configuration conf, Properties props) {
    return assertCatalogType(conf, props, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP, CatalogUtil.ICEBERG_CATALOG_HADOOP);
  }

  /**
   * Get Hadoop config key of a catalog property based on catalog name
   * @param catalogName catalog name
   * @param catalogProperty catalog property, can be any custom property,
   *                        a commonly used list of properties can be found
   *                        at {@link org.apache.iceberg.CatalogProperties}
   * @return Hadoop config key of a catalog property for the catalog name
   */
  public static String catalogPropertyConfigKey(String catalogName, String catalogProperty) {
    return String.format("%s%s.%s", CATALOG_CONFIG_PREFIX, catalogName, catalogProperty);
  }

  /**
   * Return the catalog type based on the catalog name.
   * <p>
   * See Catalogs documentation for catalog type resolution strategy.
   *
   * @param conf global hive configuration
   * @param catalogName name of the catalog
   * @return type of the catalog, can be null
   */
  public static String getCatalogType(Configuration conf, String catalogName) {
    if (!StringUtils.isEmpty(catalogName)) {
      String catalogType = conf.get(catalogPropertyConfigKey(
          catalogName, CatalogUtil.ICEBERG_CATALOG_TYPE));
      if (catalogName.equals(ICEBERG_HADOOP_TABLE_NAME)) {
        return NO_CATALOG_TYPE;
      } else {
        return catalogType;
      }
    } else {
      String catalogType = conf.get(CatalogUtil.ICEBERG_CATALOG_TYPE);
      if (catalogType != null && catalogType.equals(LOCATION)) {
        return NO_CATALOG_TYPE;
      } else {
        return catalogType;
      }
    }
  }

  public static String getCatalogImpl(Configuration conf, String catalogName) {
    return Optional.ofNullable(catalogName)
        .filter(StringUtils::isNotEmpty)
        .map(name -> String.format(CatalogUtils.CATALOG_IMPL_TEMPLATE, name))
        .map(conf::get)
        .orElse(null);
  }

  public static boolean assertCatalogType(Configuration conf, Properties props, String expectedType,
      String expectedImpl) {
    String catalogName = props.getProperty(CATALOG_NAME);
    String catalogType = Optional.ofNullable(CatalogUtils.getCatalogType(conf, catalogName))
        .orElseGet(() -> CatalogUtils.getCatalogType(conf, ICEBERG_DEFAULT_CATALOG_NAME));

    if (catalogType != null) {
      return expectedType.equalsIgnoreCase(catalogType);
    }

    String actualImpl = CatalogUtils.getCatalogProperties(conf, catalogName).get(CatalogProperties.CATALOG_IMPL);

    // Return true immediately if the strings are equal (this also handles both being null).
    if (StringUtils.equals(expectedImpl, actualImpl)) {
      return true;
    }

    // If they are not equal, but one of them is null, they can't be subtypes.
    if (expectedImpl == null || actualImpl == null) {
      return false;
    }

    // Now that we know both are non-null and not equal, check the class hierarchy.
    try {
      return Class.forName(expectedImpl).isAssignableFrom(Class.forName(actualImpl));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(String.format("Error checking if catalog %s is subtype of %s",
          catalogName, expectedImpl), e);
    }
  }
}
