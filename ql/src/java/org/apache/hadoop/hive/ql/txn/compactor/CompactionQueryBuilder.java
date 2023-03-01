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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.DDLPlanUtils;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.DirectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Builds query strings that help with query-based compaction of CRUD and insert-only tables.
 */
class CompactionQueryBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionQueryBuilder.class.getName());

  // required fields, set in constructor
  private final Operation operation;
  private final String resultTableName;

  // required for some types of compaction. Required...
  private Table sourceTab; // for Create and for Insert in CRUD and insert-only major
  private StorageDescriptor storageDescriptor; // for Create in insert-only
  private String location; // for Create
  private ValidWriteIdList validWriteIdList; // for Alter/Insert in minor and CRUD
  private AcidDirectory dir; // for Alter in minor
  private Partition sourcePartition; // for Insert in major and insert-only minor
  private String sourceTabForInsert; // for Insert
  private int numberOfBuckets;  //for rebalance
  private String orderByClause;  //for rebalance

  // settable booleans
  private boolean isPartitioned; // for Create
  private boolean isBucketed; // for Create in CRUD
  private boolean isDeleteDelta; // for Alter in CRUD minor

  // internal use only, for legibility
  private final boolean insertOnly;
  private final CompactionType compactionType;

  enum Operation {
    CREATE, ALTER, INSERT, DROP
  }

  /**
   * Set source table – the table to compact.
   * Required for Create operations and for Insert operations in crud and insert-only major
   * compaction.
   *
   * @param sourceTab table to compact, not null
   */
  CompactionQueryBuilder setSourceTab(Table sourceTab) {
    this.sourceTab = sourceTab;
    return this;
  }

  /**
   * Set the StorageDescriptor of the table or partition to compact.
   * Required for Create operations in insert-only compaction.
   *
   * @param storageDescriptor StorageDescriptor of the table or partition to compact, not null
   */
  CompactionQueryBuilder setStorageDescriptor(StorageDescriptor storageDescriptor) {
    this.storageDescriptor = storageDescriptor;
    return this;
  }

  /**
   * Set the location of the temp tables.
   * Used for Create operations.
   *
   * @param location location of the temp tables
   */
  CompactionQueryBuilder setLocation(String location) {
    this.location = location;
    return this;
  }

  /**
   * Set list of valid write ids.
   * Required for Alter and Insert operations in crud minor compaction.
   *
   * @param validWriteIdList list of valid write ids, not null
   */
  CompactionQueryBuilder setValidWriteIdList(ValidWriteIdList validWriteIdList) {
    this.validWriteIdList = validWriteIdList;
    return this;
  }

  /**
   * Set Acid Directory.
   * Required for Alter operations in minor compaction.
   *
   * @param dir Acid Directory, not null
   */
  CompactionQueryBuilder setDir(AcidDirectory dir) {
    this.dir = dir;
    return this;
  }

  /**
   * Set partition to compact, if we are compacting a partition.
   * Required for Insert operations in major and insert-only minor compaction.
   */
  CompactionQueryBuilder setSourcePartition(Partition sourcePartition) {
    this.sourcePartition = sourcePartition;
    return this;
  }

  /**
   * Set table to select from.
   * Required for Insert operations.
   *
   * @param sourceTabForInsert name of table to select from, not null
   */
  CompactionQueryBuilder setSourceTabForInsert(String sourceTabForInsert) {
    this.sourceTabForInsert = sourceTabForInsert;
    return this;
  }

  /**
   * Sets the target number of implicit buckets for a rebalancing compaction
   * @param numberOfBuckets The target number of buckets
   */
  public CompactionQueryBuilder setNumberOfBuckets(int numberOfBuckets) {
    this.numberOfBuckets = numberOfBuckets;
    return this;
  }

  /**
   * Sets the order by clause for a rebalancing compaction. It will be used to re-order the data in the table during
   * the compaction.
   * @param orderByClause The ORDER BY clause to use for data reordering.
   */
  public CompactionQueryBuilder setOrderByClause(String orderByClause) {
    this.orderByClause = orderByClause;
    return this;
  }

  /**
   * If true, Create operations will result in a table with partition column `file_name`.
   */
  CompactionQueryBuilder setPartitioned(boolean partitioned) {
    isPartitioned = partitioned;
    return this;
  }

  /**
   * If true, Create operations for CRUD minor compaction will result in a bucketed table.
   */
  CompactionQueryBuilder setBucketed(boolean bucketed) {
    if(CompactionType.REBALANCE.equals(compactionType) && bucketed) {
      throw new IllegalArgumentException("Rebalance compaction is supported only on implicitly-bucketed tables!");
    }
    isBucketed = bucketed;
    return this;
  }

  /**
   * If true, during CRUD minor compaction, Alter operations will result in the temp table's
   * partitions pointing to delete delta directories as opposed to insert deltas' directories (see
   * MinorQueryCompactor for details).
   */
  CompactionQueryBuilder setIsDeleteDelta(boolean deleteDelta) {
    isDeleteDelta = deleteDelta;
    return this;
  }

  /**
   * Construct a CompactionQueryBuilder with required params.
   *
   * @param compactionType major or minor; crud or insert-only, e.g. CompactionType.MAJOR_CRUD.
   *                       Cannot be null.
   * @param operation query's Operation e.g. Operation.CREATE.
   * @param insertOnly Indicates if the table is not full ACID but Insert-only (Micromanaged)
   * @throws IllegalArgumentException if compactionType is null
   */
  CompactionQueryBuilder(CompactionType compactionType, Operation operation, boolean insertOnly,
      String resultTableName) {
    if (compactionType == null) {
      throw new IllegalArgumentException("CompactionQueryBuilder.CompactionType cannot be null");
    }
    this.compactionType = compactionType;
    this.operation = operation;
    this.resultTableName = resultTableName;
    this.insertOnly = insertOnly;

    if (CompactionType.REBALANCE.equals(compactionType) && insertOnly) {
      throw new IllegalArgumentException("Rebalance compaction is supported only on full ACID tables!");
    }
  }

  /**
   * Build the query string based on parameters.
   *
   * @return query String
   */
  String build() {
    StringBuilder query = new StringBuilder(operation.toString());

    if (operation == Operation.CREATE) {
      query.append(" temporary external");
    }
    if (operation == Operation.INSERT) {
      query.append(CompactionType.REBALANCE.equals(compactionType) ? " overwrite" : " into");
    }
    query.append(" table ");

    if (operation == Operation.DROP) {
      query.append("if exists ");
    }

    query.append(resultTableName);

    switch (operation) {
    case CREATE:
      getDdlForCreate(query);
      break;
    case ALTER:
      buildAddClauseForAlter(query);
      break;
    case INSERT:
      query.append(" select ");
      buildSelectClauseForInsert(query);
      query.append(" from ");
      getSourceForInsert(query);
      buildWhereClauseForInsert(query);
      break;
    case DROP:
    default:
    }

    return query.toString();
  }

  private void buildAddClauseForAlter(StringBuilder query) {
    if (validWriteIdList == null || dir == null) {
      query.setLength(0);
      return;  // avoid NPEs, don't throw an exception but return an empty query
    }
    long minWriteID =
        validWriteIdList.getMinOpenWriteId() == null ? 1 : validWriteIdList.getMinOpenWriteId();
    long highWatermark = validWriteIdList.getHighWatermark();
    List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories().stream().filter(
        delta -> delta.isDeleteDelta() == isDeleteDelta && delta.getMaxWriteId() <= highWatermark
            && delta.getMinWriteId() >= minWriteID)
        .collect(Collectors.toList());
    if (deltas.isEmpty()) {
      query.setLength(0); // no alter query needed; clear StringBuilder
      return;
    }
    query.append(" add ");
    deltas.forEach(delta -> query.append("partition (file_name='")
        .append(delta.getPath().getName()).append("')"
            + " location '").append(delta.getPath()).append("' "));
  }


  private void buildSelectClauseForInsert(StringBuilder query) {
    // Need list of columns for major crud, mmmajor partitioned, mmminor
    List<FieldSchema> cols;
    if (CompactionType.REBALANCE.equals(compactionType) ||
        CompactionType.MAJOR.equals(compactionType) && (!insertOnly || sourcePartition != null) ||
        CompactionType.MINOR.equals(compactionType) && insertOnly) {
      if (sourceTab == null) {
        return; // avoid NPEs, don't throw an exception but skip this part of the query
      }
      cols = sourceTab.getSd().getCols();
    } else {
      cols = null;
    }
    switch (compactionType) {
      case REBALANCE: {
        query.append("0, t2.writeId, t2.rowId DIV CEIL(numRows / ")
            .append(numberOfBuckets)
            .append("), t2.rowId, t2.writeId, t2.data from (select ")
            .append("count(ROW__ID.writeId) over() as numRows, ");
        if (StringUtils.isNotBlank(orderByClause)) {
          // in case of reordering the data the writeids cannot be kept.
          query.append("MAX(ROW__ID.writeId) over() as writeId, row_number() OVER (")
            .append(orderByClause);
        } else {
          query.append("ROW__ID.writeId as writeId, row_number() OVER (order by ROW__ID.writeId ASC, ROW__ID.bucketId ASC, ROW__ID.rowId ASC");
        }
        query.append(") - 1 AS rowId, NAMED_STRUCT(");
        for (int i = 0; i < cols.size(); ++i) {
          query.append(i == 0 ? "'" : ", '").append(cols.get(i).getName()).append("', `")
              .append(cols.get(i).getName()).append("`");
        }
        query.append(") as data");
        break;
      }
      case MAJOR: {
        if (insertOnly) {
          if (sourcePartition != null) { //mmmajor and partitioned
            appendColumns(query, cols, false);
          } else { // mmmajor and unpartitioned
            query.append("*");
          }
        } else {
          query.append(
              "validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId), "
                  + "ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, "
                  + "NAMED_STRUCT(");
          appendColumns(query, cols, true);
          query.append(") ");
        }
        break;
      }
      case MINOR: {
        if (insertOnly) {
          appendColumns(query, cols, false);
        } else {
          query.append(
              "`operation`, `originalTransaction`, `bucket`, `rowId`, `currentTransaction`, `row`");
        }
        break;
      }
    }
  }

  private void appendColumns(StringBuilder query, List<FieldSchema> cols, boolean alias) {
    if (cols == null) {
      throw new IllegalStateException("Query could not be created: Source columns are unknown");
    }
    for (int i = 0; i < cols.size(); ++i) {
      if (alias) {
        query.append(i == 0 ? "'" : ", '").append(cols.get(i).getName()).append("', `")
            .append(cols.get(i).getName()).append("`");
      } else {
        query.append(i == 0 ? "`" : ", `").append(cols.get(i).getName()).append("`");
      }
    }
  }

  private void getSourceForInsert(StringBuilder query) {
    if (sourceTabForInsert != null) {
      query.append(sourceTabForInsert);
    } else {
      query.append(sourceTab.getDbName()).append(".").append(sourceTab.getTableName());
    }
    query.append(" ");
    if (CompactionType.REBALANCE.equals(compactionType)) {
      if (StringUtils.isNotBlank(orderByClause)) {
        query.append(orderByClause);
      } else {
        query.append("order by ROW__ID.writeId ASC, ROW__ID.bucketId ASC, ROW__ID.rowId ASC");
      }
      query.append(") t2");
    } else if (CompactionType.MAJOR.equals(compactionType) && insertOnly && StringUtils.isNotBlank(orderByClause)) {
      query.append(orderByClause);
    }
  }

  private void buildWhereClauseForInsert(StringBuilder query) {
    if (CompactionType.MAJOR.equals(compactionType) && sourcePartition != null && sourceTab != null) {
      List<String> vals = sourcePartition.getValues();
      List<FieldSchema> keys = sourceTab.getPartitionKeys();
      if (keys.size() != vals.size()) {
        throw new IllegalStateException("source partition values ("
            + Arrays.toString(vals.toArray()) + ") do not match source table values ("
            + Arrays.toString(keys.toArray()) + "). Failing compaction.");
      }

      query.append(" where ");
      for (int i = 0; i < keys.size(); ++i) {
        FieldSchema keySchema = keys.get(i);
        query.append(i == 0 ? "`" : " and `").append(keySchema.getName()).append("`=");
        if (!keySchema.getType().equalsIgnoreCase(ColumnType.BOOLEAN_TYPE_NAME)) {
          query.append("'").append(vals.get(i)).append("'");
        } else {
          query.append(vals.get(i));
        }
      }
    }

    if (CompactionType.MINOR.equals(compactionType) && !insertOnly && validWriteIdList != null) {
      long[] invalidWriteIds = validWriteIdList.getInvalidWriteIds();
      if (invalidWriteIds.length > 0) {
        query.append(" where `originalTransaction` not in (").append(
            StringUtils.join(ArrayUtils.toObject(invalidWriteIds), ","))
            .append(")");
      }
    }
  }

  private void getDdlForCreate(StringBuilder query) {
    defineColumns(query);

    // PARTITIONED BY. Used for parts of minor compaction.
    if (isPartitioned) {
      query.append(" PARTITIONED BY (`file_name` STRING) ");
    }

    // CLUSTERED BY. (bucketing)
    int bucketingVersion = 0;
    if (!insertOnly && CompactionType.MINOR.equals(compactionType)) {
      bucketingVersion = getMinorCrudBucketing(query, bucketingVersion);
    } else if (insertOnly) {
      getMmBucketing(query);
    }

    // SKEWED BY
    if (insertOnly) {
      getSkewedByClause(query);
    }

    // STORED AS / ROW FORMAT SERDE + INPUTFORMAT + OUTPUTFORMAT
    if (!insertOnly) {
      query.append(" stored as orc");
    } else {
      copySerdeFromSourceTable(query);
    }

    // LOCATION
    if (location != null) {
      query.append(" LOCATION '").append(HiveStringUtils.escapeHiveCommand(location)).append("'");
    }

    // TBLPROPERTIES
    addTblProperties(query, bucketingVersion);
  }

  /**
   * Define columns of the create query.
   */
  private void defineColumns(StringBuilder query) {
    if (sourceTab == null) {
      return; // avoid NPEs, don't throw an exception but skip this part of the query
    }
    query.append("(");
    if (!insertOnly) {
      query.append(
          "`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, "
              + "`currentTransaction` bigint, `row` struct<");
    }
    List<FieldSchema> cols = sourceTab.getSd().getCols();
    List<String> columnDescs = new ArrayList<>();
    for (FieldSchema col : cols) {
      String columnType = DDLPlanUtils.formatType(TypeInfoUtils.getTypeInfoFromTypeString(col.getType()));
      String columnDesc = "`" + col.getName() + "` " + (!insertOnly ? ":" : "") + columnType;
      columnDescs.add(columnDesc);
    }
    query.append(StringUtils.join(columnDescs, ','));
    query.append(!insertOnly ? ">" : "");
    query.append(") ");
  }

  /**
   * Part of Create operation. Copy source table bucketing for insert-only compaction.
   */
  private void getMmBucketing(StringBuilder query) {
    if (sourceTab == null) {
      return; // avoid NPEs, don't throw an exception but skip this part of the query
    }
    boolean isFirst;
    List<String> buckCols = sourceTab.getSd().getBucketCols();
    if (buckCols.size() > 0) {
      query.append("CLUSTERED BY (").append(StringUtils.join(buckCols, ",")).append(") ");
      List<Order> sortCols = sourceTab.getSd().getSortCols();
      if (sortCols.size() > 0) {
        query.append("SORTED BY (");
        isFirst = true;
        for (Order sortCol : sortCols) {
          if (!isFirst) {
            query.append(", ");
          }
          isFirst = false;
          query.append(sortCol.getCol()).append(" ")
              .append(DirectionUtils.codeToText(sortCol.getOrder()));
        }
        query.append(") ");
      }
      query.append("INTO ").append(sourceTab.getSd().getNumBuckets()).append(" BUCKETS");
    }
  }

  /**
   * Part of Create operation. Minor crud compaction uses its own bucketing system.
   */
  private int getMinorCrudBucketing(StringBuilder query, int bucketingVersion) {
    if (isBucketed && sourceTab != null) { // skip if sourceTab is null to avoid NPEs
      int numBuckets = 1;
      try {
        org.apache.hadoop.hive.ql.metadata.Table t =
            Hive.get().getTable(sourceTab.getDbName(), sourceTab.getTableName());
        numBuckets = Math.max(t.getNumBuckets(), numBuckets);
        bucketingVersion = t.getBucketingVersion();
      } catch (HiveException e) {
        LOG.info("Error finding table {}. Minor compaction result will use 0 buckets.",
            sourceTab.getTableName());
      } finally {
        query.append(" clustered by (`bucket`)")
            .append(" sorted by (`originalTransaction`, `bucket`, `rowId`)")
            .append(" into ").append(numBuckets).append(" buckets");
      }
    }
    return bucketingVersion;
  }

  /**
   * Part of Create operation. Insert-only compaction tables copy source tables.
   */
  private void getSkewedByClause(StringBuilder query) {
    if (sourceTab == null) {
      return; // avoid NPEs, don't throw an exception but skip this part of the query
    }
    boolean isFirst; // Stored as directories. We don't care about the skew otherwise.
    if (sourceTab.getSd().isStoredAsSubDirectories()) {
      SkewedInfo skewedInfo = sourceTab.getSd().getSkewedInfo();
      if (skewedInfo != null && !skewedInfo.getSkewedColNames().isEmpty()) {
        query.append(" SKEWED BY (")
            .append(StringUtils.join(skewedInfo.getSkewedColNames(), ", ")).append(") ON ");
        isFirst = true;
        for (List<String> colValues : skewedInfo.getSkewedColValues()) {
          if (!isFirst) {
            query.append(", ");
          }
          isFirst = false;
          query.append("('").append(StringUtils.join(colValues, "','")).append("')");
        }
        query.append(") STORED AS DIRECTORIES");
      }
    }
  }

  /**
   * Part of Create operation. Insert-only compaction tables copy source tables' serde.
   */
  private void copySerdeFromSourceTable(StringBuilder query) {
    if (storageDescriptor == null) {
      return; // avoid NPEs, don't throw an exception but skip this part of the query
    }
    ensureTableToCompactIsNative();
    SerDeInfo serdeInfo = storageDescriptor.getSerdeInfo();
    Map<String, String> serdeParams = serdeInfo.getParameters();
    query.append(" ROW FORMAT SERDE '")
        .append(HiveStringUtils.escapeHiveCommand(serdeInfo.getSerializationLib())).append("'");
    // WITH SERDEPROPERTIES
    if (!serdeParams.isEmpty()) {
      DDLPlanUtils.appendSerdeParams(query, serdeParams);
    }
    query.append("STORED AS INPUTFORMAT '")
        .append(HiveStringUtils.escapeHiveCommand(storageDescriptor.getInputFormat())).append("'")
        .append(" OUTPUTFORMAT '")
        .append(HiveStringUtils.escapeHiveCommand(storageDescriptor.getOutputFormat()))
        .append("'");
  }

  /**
   * Part of Create operation. All tmp tables are not transactional and are marked as
   * compaction tables. Additionally...
   * - Crud compaction temp tables need tblproperty, "compactiontable."
   * - Minor crud compaction temp tables need bucketing version tblproperty, if table is bucketed.
   * - Insert-only compaction tables copy source tables' tblproperties, except metastore/statistics
   *   properties.
   */
  private void addTblProperties(StringBuilder query, int bucketingVersion) {
    Map<String, String> tblProperties = new HashMap<>();
    tblProperties.put("transactional", "false");
    if (!insertOnly) {
      tblProperties.put(AcidUtils.COMPACTOR_TABLE_PROPERTY, compactionType.name());
      if (CompactionType.MINOR.equals(compactionType) && isBucketed) {
        tblProperties.put("bucketing_version", String.valueOf(bucketingVersion));
      }
    }
    if (sourceTab != null) { // to avoid NPEs, skip this part if sourceTab is null
      if (insertOnly) {
        // Exclude all standard table properties.
        Set<String> excludes = getHiveMetastoreConstants();
        excludes.addAll(StatsSetupConst.TABLE_PARAMS_STATS_KEYS);
        for (Map.Entry<String, String> e : sourceTab.getParameters().entrySet()) {
          if (e.getValue() == null) {
            continue;
          }
          if (excludes.contains(e.getKey())) {
            continue;
          }
          tblProperties.put(e.getKey(), HiveStringUtils.escapeHiveCommand(e.getValue()));
        }
      } else {
        for (Map.Entry<String, String> e : sourceTab.getParameters().entrySet()) {
          if (e.getKey().startsWith("orc.")) {
            tblProperties.put(e.getKey(), HiveStringUtils.escapeHiveCommand(e.getValue()));
          }
        }
      }
    }

    // add TBLPROPERTIES clause to query
    boolean isFirst;
    query.append(" TBLPROPERTIES (");
    isFirst = true;
    for (Map.Entry<String, String> property : tblProperties.entrySet()) {
      if (!isFirst) {
        query.append(", ");
      }
      query.append("'").append(property.getKey()).append("'='").append(property.getValue())
          .append("'");
      isFirst = false;
    }
    query.append(")");
  }

  private static Set<String> getHiveMetastoreConstants() {
    Set<String> result = new HashSet<>();
    for (Field f : hive_metastoreConstants.class.getDeclaredFields()) {
      if (!Modifier.isFinal(f.getModifiers())) {
        continue;
      }
      if (!String.class.equals(f.getType())) {
        continue;
      }
      f.setAccessible(true);
      try {
        result.add((String) f.get(null));
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    return result;
  }

  private void ensureTableToCompactIsNative() {
    if (sourceTab == null) {
      return;
    }
    String storageHandler =
        sourceTab.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE);
    if (storageHandler != null) {
      String message = "Table " + sourceTab.getTableName() + "has a storage handler ("
          + storageHandler + "). Failing compaction for this non-native table.";
      LOG.error(message);
      throw new RuntimeException(message);
    }
  }
}
