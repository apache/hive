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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.parse.PartitionTransformSpec;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
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
    String metaTable = properties.getProperty("metaTable");
    String tableName = properties.getProperty(Catalogs.NAME);
    if (metaTable != null) {
      properties.setProperty(Catalogs.NAME, tableName + "." + metaTable);
    }

    String tableIdentifier = properties.getProperty(Catalogs.NAME);
    return SessionStateUtil.getResource(configuration, tableIdentifier).filter(o -> o instanceof Table)
        .map(o -> (Table) o).orElseGet(() -> {
          LOG.debug("Iceberg table {} is not found in QueryState. Loading table from configured catalog",
              tableIdentifier);
          Table tab = Catalogs.loadTable(configuration, properties);
          SessionStateUtil.addResource(configuration, tableIdentifier, tab);
          return tab;
        });
  }

  /**
   * Constructs the table properties needed for the Iceberg table loading by retrieving the information from the
   * hmsTable. It then calls {@link IcebergTableUtil#getTable(Configuration, Properties)} with these properties.
   * @param configuration a Hadoop configuration
   * @param hmsTable the HMS table
   * @return the Iceberg table
   */
  static Table getTable(Configuration configuration, org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    Properties properties = new Properties();
    properties.setProperty(Catalogs.NAME, TableIdentifier.of(hmsTable.getDbName(), hmsTable.getTableName()).toString());
    properties.setProperty(Catalogs.LOCATION, hmsTable.getSd().getLocation());
    if (hmsTable.getParameters().containsKey(InputFormatConfig.CATALOG_NAME)) {
      properties.setProperty(
          InputFormatConfig.CATALOG_NAME, hmsTable.getParameters().get(InputFormatConfig.CATALOG_NAME));
    }
    return getTable(configuration, properties);
  }

  /**
   * Create {@link PartitionSpec} based on the partition information stored in
   * {@link PartitionTransformSpec}.
   * @param configuration a Hadoop configuration
   * @param schema iceberg table schema
   * @return iceberg partition spec, always non-null
   */
  public static PartitionSpec spec(Configuration configuration, Schema schema) {
    List<PartitionTransformSpec> partitionTransformSpecList = SessionStateUtil
            .getResource(configuration, hive_metastoreConstants.PARTITION_TRANSFORM_SPEC)
        .map(o -> (List<PartitionTransformSpec>) o).orElseGet(() -> null);

    if (partitionTransformSpecList == null) {
      LOG.debug("Iceberg partition transform spec is not found in QueryState.");
      return null;
    }

    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    partitionTransformSpecList.forEach(spec -> {
      switch (spec.getTransformType()) {
        case IDENTITY:
          builder.identity(spec.getColumnName());
          break;
        case YEAR:
          builder.year(spec.getColumnName());
          break;
        case MONTH:
          builder.month(spec.getColumnName());
          break;
        case DAY:
          builder.day(spec.getColumnName());
          break;
        case HOUR:
          builder.hour(spec.getColumnName());
          break;
        case TRUNCATE:
          builder.truncate(spec.getColumnName(), spec.getTransformParam().get());
          break;
        case BUCKET:
          builder.bucket(spec.getColumnName(), spec.getTransformParam().get());
          break;
      }
    });
    return builder.build();
  }

  public static void updateSpec(Configuration configuration, Table table) {
    // get the new partition transform spec
    PartitionSpec newPartitionSpec = spec(configuration, table.schema());
    if (newPartitionSpec == null) {
      LOG.debug("Iceberg Partition spec is not updated due to empty partition spec definition.");
      return;
    }

    List<String> newPartitionNames =
        newPartitionSpec.fields().stream().map(PartitionField::name).collect(Collectors.toList());
    List<String> currentPartitionNames = table.spec().fields().stream().map(PartitionField::name)
        .collect(Collectors.toList());
    List<String> intersectingPartitionNames =
        currentPartitionNames.stream().filter(newPartitionNames::contains).collect(Collectors.toList());

    // delete those partitions which are not present among the new partion spec
    UpdatePartitionSpec updatePartitionSpec = table.updateSpec();
    currentPartitionNames.stream().filter(p -> !intersectingPartitionNames.contains(p))
        .forEach(updatePartitionSpec::removeField);
    updatePartitionSpec.apply();

    // add new partitions which are not yet present
    List<PartitionTransformSpec> partitionTransformSpecList = SessionStateUtil
        .getResource(configuration, hive_metastoreConstants.PARTITION_TRANSFORM_SPEC)
        .map(o -> (List<PartitionTransformSpec>) o).orElseGet(() -> null);
    IntStream.range(0, partitionTransformSpecList.size())
        .filter(i -> !intersectingPartitionNames.contains(newPartitionSpec.fields().get(i).name()))
        .forEach(i -> {
          PartitionTransformSpec spec = partitionTransformSpecList.get(i);
          switch (spec.getTransformType()) {
            case IDENTITY:
              updatePartitionSpec.addField(spec.getColumnName());
              break;
            case YEAR:
              updatePartitionSpec.addField(Expressions.year(spec.getColumnName()));
              break;
            case MONTH:
              updatePartitionSpec.addField(Expressions.month(spec.getColumnName()));
              break;
            case DAY:
              updatePartitionSpec.addField(Expressions.day(spec.getColumnName()));
              break;
            case HOUR:
              updatePartitionSpec.addField(Expressions.hour(spec.getColumnName()));
              break;
            case TRUNCATE:
              updatePartitionSpec.addField(Expressions.truncate(spec.getColumnName(), spec.getTransformParam().get()));
              break;
            case BUCKET:
              updatePartitionSpec.addField(Expressions.bucket(spec.getColumnName(), spec.getTransformParam().get()));
              break;
          }
        });

    updatePartitionSpec.commit();
  }

  public static boolean isBucketed(Table table) {
    return table.spec().fields().stream().anyMatch(f -> f.transform().toString().startsWith("bucket["));
  }
}
