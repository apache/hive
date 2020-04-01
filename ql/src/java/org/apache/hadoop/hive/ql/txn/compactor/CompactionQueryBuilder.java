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
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.ddl.table.create.show.ShowCreateTableOperation;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.DirectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Builds query strings that help with query-based compaction of CRUD and insert-only tables.
 */
class CompactionQueryBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionQueryBuilder.class.getName());

  // required fields, set in constructor
  private final Operation operation;
  private String resultTableName;

  // required for some types of compaction. Required...
  private Table sourceTab; // for Create and for Insert in CRUD and insert-only major
  private StorageDescriptor storageDescriptor; // for Create in insert-only
  private String location; // for Create
  private ValidWriteIdList validWriteIdList; // for Alter/Insert in minor and CRUD
  private AcidUtils.Directory dir; // for Alter in minor
  private Partition sourcePartition; // for Insert in major and insert-only minor
  private String fromTableName; // for Insert

  // settable booleans
  private boolean isPartitioned; // for Create
  private boolean isBucketed; // for Create in CRUD
  private boolean isDeleteDelta; // for Alter in CRUD minor

  // internal use only, for legibility
  private final boolean major;
  private final boolean minor;
  private final boolean crud;
  private final boolean insertOnly;

  enum CompactionType {
    MAJOR_CRUD, MINOR_CRUD, MAJOR_INSERT_ONLY, MINOR_INSERT_ONLY
  }

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
  CompactionQueryBuilder setDir(AcidUtils.Directory dir) {
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
   * @param fromTableName name of table to select from, not null
   */
  CompactionQueryBuilder setFromTableName(String fromTableName) {
    this.fromTableName = fromTableName;
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
   * @param resultTableName the name of the table we are running the operation on
   * @throws IllegalArgumentException if compactionType is null
   */
  CompactionQueryBuilder(CompactionType compactionType, Operation operation,
      String resultTableName) {
    if (compactionType == null) {
      throw new IllegalArgumentException("CompactionQueryBuilder.CompactionType cannot be null");
    }
    this.operation = operation;
    this.resultTableName = resultTableName;
    major = compactionType == CompactionType.MAJOR_CRUD
        || compactionType == CompactionType.MAJOR_INSERT_ONLY;
    crud =
        compactionType == CompactionType.MAJOR_CRUD || compactionType == CompactionType.MINOR_CRUD;
    minor = !major;
    insertOnly = !crud;
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
      query.append(" into");
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
      query.append(" from ")
          .append(fromTableName);
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
    if (major && crud || major && insertOnly && sourcePartition != null || minor && insertOnly) {
      if (sourceTab == null) {
        return; // avoid NPEs, don't throw an exception but skip this part of the query
      }
      cols = sourceTab.getSd().getCols();
    } else {
      cols = null;
    }

    if (crud) {
      if (major) {
        query.append(
            "validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId), "
                + "ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, "
                + "NAMED_STRUCT(");
        for (int i = 0; i < cols.size(); ++i) {
          query.append(i == 0 ? "'" : ", '").append(cols.get(i).getName()).append("', ")
              .append(cols.get(i).getName());
        }
        query.append(") ");
      } else { //minor
        query.append(
            "`operation`, `originalTransaction`, `bucket`, `rowId`, `currentTransaction`, `row`");
      }

    } else { // mm
      if (major) {
        if (sourcePartition != null) { //mmmajor and partitioned
          for (int i = 0; i < cols.size(); ++i) {
            query.append(i == 0 ? "`" : ", `").append(cols.get(i).getName()).append("`");
          }
        } else { // mmmajor and unpartitioned
          query.append("*");
        }
      } else { // mmminor
        for (int i = 0; i < cols.size(); ++i) {
          query.append(i == 0 ? "`" : ", `").append(cols.get(i).getName()).append("`");
        }
      }
    }
  }

  private void buildWhereClauseForInsert(StringBuilder query) {
    if (major && sourcePartition != null && sourceTab != null) {
      List<String> vals = sourcePartition.getValues();
      List<FieldSchema> keys = sourceTab.getPartitionKeys();
      if (keys.size() != vals.size()) {
        throw new IllegalStateException("source partition values ("
            + Arrays.toString(vals.toArray()) + ") do not match source table values ("
            + Arrays.toString(keys.toArray()) + "). Failing compaction.");
      }

      query.append(" where ");
      for (int i = 0; i < keys.size(); ++i) {
        query.append(i == 0 ? "`" : " and `").append(keys.get(i).getName()).append("`='")
            .append(vals.get(i)).append("'");
      }
    }

    if (minor && crud && validWriteIdList != null) {
      long[] invalidWriteIds = validWriteIdList.getInvalidWriteIds();
      if (invalidWriteIds.length > 0) {
        query.append(" where `originalTransaction` not in (").append(
            org.apache.commons.lang3.StringUtils.join(ArrayUtils.toObject(invalidWriteIds), ","))
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
    if (crud && minor) {
      bucketingVersion = getMinorCrudBucketing(query, bucketingVersion);
    } else if (insertOnly) {
      getMmBucketing(query);
    }

    // SKEWED BY
    if (insertOnly) {
      getSkewedByClause(query);
    }

    // STORED AS / ROW FORMAT SERDE + INPUTFORMAT + OUTPUTFORMAT
    if (crud) {
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
    if (crud) {
      query.append(
          "`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, "
              + "`currentTransaction` bigint, `row` struct<");
    }
    List<FieldSchema> cols = sourceTab.getSd().getCols();
    boolean isFirst = true;
    for (FieldSchema col : cols) {
      if (!isFirst) {
        query.append(", ");
      }
      isFirst = false;
      query.append("`").append(col.getName()).append("` ");
      query.append(crud ? ":" : "");
      query.append(col.getType());
    }
    query.append(crud ? ">" : "");
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
      query.append("CLUSTERED BY (").append(StringUtils.join(",", buckCols)).append(") ");
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
            .append(" sorted by (`bucket`, `originalTransaction`, `rowId`)")
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
            .append(StringUtils.join(", ", skewedInfo.getSkewedColNames())).append(") ON ");
        isFirst = true;
        for (List<String> colValues : skewedInfo.getSkewedColValues()) {
          if (!isFirst) {
            query.append(", ");
          }
          isFirst = false;
          query.append("('").append(StringUtils.join("','", colValues)).append("')");
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
      ShowCreateTableOperation.appendSerdeParams(query, serdeParams);
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
    if (crud) {
      tblProperties.put(AcidUtils.COMPACTOR_TABLE_PROPERTY, "true");
    }
    if (crud && minor && isBucketed) {
      tblProperties.put("bucketing_version", String.valueOf(bucketingVersion));
    }
    if (insertOnly && sourceTab != null) { // to avoid NPEs, skip this part if sourceTab is null
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
