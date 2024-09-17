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
package org.apache.hadoop.hive.metastore.tools.metatool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class IcebergTableMetadataHandler {
  static final private Logger LOG = LoggerFactory.getLogger(IcebergTableMetadataHandler.class.getName());
  // "hive" is the catalog name, it is something we can set up by ourselves
  private static final String CATALOG_NAME_IN_ICEBERG = "hive";
  private static final String CATALOG_CLASS = "org.apache.iceberg.hive.HiveCatalog";
  private final String mgdWarehouse, extWarehouse, uris;
  private final Configuration conf;
  private boolean isEnabled = false;

  private static final IcebergReflector IR;
  static {
    IcebergReflector ir = null;
    try {
      ir = new IcebergReflector();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
      LOG.warn("Could not find or instantiate class " + CATALOG_CLASS + ", cannot retrieve stats for iceberg tables.", e);
    }
    IR = ir;
  }

  public IcebergTableMetadataHandler(Configuration config) {
    this.conf = config;
    mgdWarehouse = MetastoreConf.getAsString(this.conf, MetastoreConf.ConfVars.WAREHOUSE);
    extWarehouse = MetastoreConf.getAsString(this.conf, MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL);
    uris = MetastoreConf.getAsString(this.conf, MetastoreConf.ConfVars.THRIFT_URIS);
  }

  public boolean isEnabled() {
    if (!isEnabled) {
      isEnabled = IR != null;
    }
    return isEnabled;
  }


  // create a HiveCatalog instance, use it to get all iceberg table identifiers
  public Map<String, MetadataTableSummary> getIcebergTables() {
    Map<String, MetadataTableSummary> metadataSummaryMap = new HashMap<>();
    if (!isEnabled()) return metadataSummaryMap;

    try {
      IcebergReflector.CatalogHandle catalog = IR.newCatalog();
      catalog.setConf(conf);
      //catalog.setConf(conf);
      Map<String, String> propertiesMap = new HashMap<>();

      // get iceberg properties and print them out to check
      LOG.info("Initializing iceberg handler with properties: warehouse:{} external warehouse:{} thrift uris:{}",
              mgdWarehouse, extWarehouse, uris);
      propertiesMap.put("warehouse", mgdWarehouse);
      propertiesMap.put("externalwarehouse", extWarehouse);
      propertiesMap.put("uri", uris);
      catalog.initialize(CATALOG_NAME_IN_ICEBERG, propertiesMap);

      // get all the name spaces
      Collection<?> listOfNamespaces = catalog.listNamespaces();
      for (Object namespace : listOfNamespaces) {
        Collection<?> identifierList = catalog.listTables(namespace);
        if (identifierList.isEmpty())
          continue;
        for (Object tblId : identifierList) {
          // fetch the metadata information for every iceberg table
          Object tbl = catalog.loadTable(tblId);
          MetadataTableSummary metadataTableSummary = catalog.collectMetadata(tbl);
          String tableName = metadataTableSummary.getTblName();

          metadataSummaryMap.putIfAbsent(tableName, metadataTableSummary);
          LOG.debug("Adding table: {} {}", tableName, metadataTableSummary);
        }
      }

    } catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
      isEnabled = false;
      LOG.warn("Could not find or instantiate class " + CATALOG_CLASS + ", cannot retrieve stats for iceberg tables.");
    }
    return metadataSummaryMap;
  }
}