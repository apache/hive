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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Map;

/**
 * Collects Iceberg table metadata statistics using reflection to avoid cyclic dependency.
 */
public class IcebergReflector {
  private static final String CATALOG_CLASS = "org.apache.iceberg.hive.HiveCatalog";
  /** The catalog class. */
  private final Class<?> catalogClass;
  /** The Catalog constructor. */
  private final Constructor<?> constructor;
  /** The Catalog.setConf() method.  */
  private final Method setConf;
  /** The Catalog.initialize() method. */
  private final Method initialize;
  /** The Catalog.listNamespaces() method. */
  private final Method listNamespaces;
  /** The Namespace.listTables() method. */
  private volatile Method listTablesMethod;
  /** The Catalog.loadTable() method. */
  private volatile Method loadTableMethod;
  /** The Table.operations() method. */
  private volatile Method operationsMethod;
  /** The *.current() method. */
  private volatile Method currentMethod;
  private volatile Method schemaMethod;
  private volatile Method columnsMethod;
  private volatile Method specMethod;
  private volatile Method fieldsMethod;
  private volatile Method currentSnapshotMethod;
  /** The Snapshot.summary() .*/
  private volatile Method summaryMethod;

  /**
   * Creates the catalog reflector.
   *
   * @throws ClassNotFoundException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  IcebergReflector() throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException {
    catalogClass = Class.forName(CATALOG_CLASS);
    // constructor
    this.constructor = catalogClass.getConstructor();
    this.setConf = catalogClass.getMethod("setConf", org.apache.hadoop.conf.Configuration.class);
    this.initialize =  catalogClass.getMethod("initialize", String.class, Map.class);
    this.listNamespaces = catalogClass.getMethod("listNamespaces");
  }

  CatalogHandle newCatalog() {
    try {
      Object catalog = constructor.newInstance();
      return new CatalogHandle(catalog);
    } catch (Throwable e) {
      // ignore
    }
    return null;
  }

  /**
   * A catalog instance.
   */
  class CatalogHandle {
    private final Object catalog;

    CatalogHandle(Object catalog) {
      this.catalog = catalog;
    }

    void setConf(Configuration conf) throws InvocationTargetException, IllegalAccessException {
      setConf.invoke(catalog, conf);
    }

    void initialize(String name, Map<String, String> properties) throws InvocationTargetException, IllegalAccessException {
      initialize.invoke(catalog, name, properties);
    }

    Collection<?> listNamespaces() throws InvocationTargetException, IllegalAccessException {
      return (Collection<?>) listNamespaces.invoke(catalog);
    }

    Collection<?> listTables(Object namespace) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
      if (listTablesMethod == null) {
        Class<?> namespaceClazz = namespace.getClass();
        listTablesMethod = catalogClass.getMethod("listTables", namespaceClazz);
      }
      return (Collection<?>) listTablesMethod.invoke(namespace);
    }

    Object loadTable(Object tableIdentifier) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
      if (loadTableMethod == null) {
        Class<?> identifierClazz = tableIdentifier.getClass();
        loadTableMethod = catalogClass.getMethod("loadTable", identifierClazz);
      }
      return loadTableMethod.invoke(tableIdentifier);
    }

    /**
     * Creates the metadata summary for a given Iceberg table.
     * @param table the table instance
     * @return a summary instance
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    MetadataTableSummary collectMetadata(Object table) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
      Method nameMethod = table.getClass().getMethod("name");
      String tableFullName = (String) nameMethod.invoke(table);
      String[] paths = tableFullName.split("\\.");
      String catalogName = paths[0];
      String databaseName = paths[1];
      String tableName = paths[2];
      if (operationsMethod == null) {
        operationsMethod = table.getClass().getMethod("operations");
      }
      Object operations = operationsMethod.invoke(table);
      if (currentMethod == null) {
        currentMethod = operations.getClass().getMethod("current");
      }
      Object meta = currentMethod.invoke(operations);
      if (schemaMethod == null) {
        schemaMethod = meta.getClass().getMethod("schema");
      }
      Object schema = schemaMethod.invoke(meta);
      if (columnsMethod == null) {
        columnsMethod = schema.getClass().getMethod("columns");
      }
      Collection<?> columns = (Collection<?>) columnsMethod.invoke(schema);
      if (specMethod == null) {
        specMethod = meta.getClass().getMethod("spec");
      }
      Object spec = specMethod.invoke(meta);
      if (fieldsMethod == null) {
        fieldsMethod = spec.getClass().getMethod("fields");
      }
      Collection<?> fields = (Collection<?>) fieldsMethod.invoke(spec);
      int columnCount=  columns.size();
      int partitionColumnCount = fields.size();

      if (currentSnapshotMethod == null) {
        currentSnapshotMethod = meta.getClass().getMethod("currentSnapshot");
      }
      Object snapshot = currentSnapshotMethod.invoke(meta);
      MetadataTableSummary metadataTableSummary = new MetadataTableSummary();
      // sometimes current snapshot could be null
      if (snapshot != null) {
        if (summaryMethod == null) {
          summaryMethod = snapshot.getClass().getDeclaredMethod("summary");
        }
        Map<String, String> summaryMap = (Map<String,String>) summaryMethod.invoke(snapshot);
        BigInteger totalSizeBytes = new BigInteger(summaryMap.get("total-files-size"));
        BigInteger totalRowsCount = new BigInteger(summaryMap.get("total-records"));
        BigInteger totalFilesCount = new BigInteger(summaryMap.get("total-data-files"));
        metadataTableSummary.setTotalSize(totalSizeBytes);
        metadataTableSummary.setSizeNumRows(totalRowsCount);
        metadataTableSummary.setSizeNumFiles(totalFilesCount);
      }
      metadataTableSummary.setCtlgName(catalogName);
      metadataTableSummary.setDbName(databaseName);
      metadataTableSummary.setTblName(tableName);
      metadataTableSummary.setColCount(columnCount);
      metadataTableSummary.setPartitionCount(partitionColumnCount);
      return metadataTableSummary;
    }
  }
}

