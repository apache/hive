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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.DDLPlanUtils;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hive.common.util.HiveStringUtils;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

abstract class CompactionQueryBuilder {
  // required fields, set in constructor
  protected Operation operation;
  protected String resultTableName;

  // required for some types of compaction. Required...
  protected Table sourceTab; // for Create and for Insert in CRUD and insert-only major
  protected String location; // for Create
  protected ValidWriteIdList validWriteIdList; // for Alter/Insert in minor and CRUD
  protected AcidDirectory dir; // for Alter in minor
  protected Partition sourcePartition; // for Insert in major and insert-only minor
  protected String sourceTabForInsert; // for Insert
  protected String orderByClause;  //for rebalance
  protected StorageDescriptor storageDescriptor; // for Create in insert-only
  protected int numberOfBuckets;

  // settable booleans
  protected boolean isPartitioned; // for Create
  protected boolean isBucketed; // for Create in CRUD
  protected boolean isDeleteDelta; // for Alter in CRUD minor

  // internal use only, for legibility
  protected boolean insertOnly;
  protected CompactionType compactionType;

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
    if (CompactionType.REBALANCE.equals(compactionType) && bucketed) {
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
   * Sets the target number of implicit buckets for a rebalancing compaction
   * @param numberOfBuckets The target number of buckets
   */
  CompactionQueryBuilder setNumberOfBuckets(int numberOfBuckets) {
    this.numberOfBuckets = numberOfBuckets;
    return this;
  }

  CompactionQueryBuilder setOperation(Operation operation) {
    this.operation = operation;
    return this;
  }

  CompactionQueryBuilder setResultTableName(String resultTableName) {
    this.resultTableName = resultTableName;
    return this;
  }

  /**
   * Construct a CompactionQueryBuilder with required params.
   *
   * @param compactionType major or minor; crud or insert-only, e.g. CompactionType.MAJOR_CRUD.
   *                       Cannot be null.
   * @param insertOnly Indicates if the table is not full ACID but Insert-only (Micromanaged)
   * @throws IllegalArgumentException if compactionType is null
   */
  CompactionQueryBuilder(CompactionType compactionType, boolean insertOnly) {
    if (compactionType == null) {
      throw new IllegalArgumentException("CompactionQueryBuilder.CompactionType cannot be null");
    }
    this.compactionType = compactionType;
    this.insertOnly = insertOnly;
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

  protected void getDdlForCreate(StringBuilder query) {
    defineColumns(query);

    // PARTITIONED BY. Used for parts of minor compaction.
    if (isPartitioned) {
      query.append(" PARTITIONED BY (`file_name` STRING) ");
    }

    // STORED AS / ROW FORMAT SERDE + INPUTFORMAT + OUTPUTFORMAT
    query.append(" stored as orc");

    // LOCATION
    if (location != null) {
      query.append(" LOCATION '").append(HiveStringUtils.escapeHiveCommand(location)).append("'");
    }

    addTblProperties(query, false, 0);
  }

  /**
   * Part of Create operation. All tmp tables are not transactional and are marked as
   * compaction tables. Additionally...
   * - Crud compaction temp tables need tblproperty, "compactiontable."
   * - Minor crud compaction temp tables need bucketing version tblproperty, if table is bucketed.
   */
  protected void addTblProperties(StringBuilder query, boolean addBucketingVersion, int bucketingVersion) {
    Map<String, String> tblProperties = new HashMap<>();
    tblProperties.put("transactional", "false");
    tblProperties.put(AcidUtils.COMPACTOR_TABLE_PROPERTY, compactionType.name());
    if (addBucketingVersion) {
      tblProperties.put("bucketing_version", String.valueOf(bucketingVersion));
    }
    if (sourceTab != null) { // to avoid NPEs, skip this part if sourceTab is null
      sourceTab.getParameters().entrySet().stream().filter(e -> e.getKey().startsWith("orc."))
          .forEach(e -> tblProperties.put(e.getKey(), HiveStringUtils.escapeHiveCommand(e.getValue())));
    }
    addTblProperties(query, tblProperties);
  }

  protected void addTblProperties(StringBuilder query, Map<String, String> tblProperties) {
    if (tblProperties != null && !tblProperties.isEmpty()) {
      // add TBLPROPERTIES clause to query
      boolean isFirst;
      query.append(" TBLPROPERTIES (");
      isFirst = true;
      for (Map.Entry<String, String> property : tblProperties.entrySet()) {
        if (!isFirst) {
          query.append(", ");
        }
        query.append("'").append(property.getKey()).append("'='").append(property.getValue()).append("'");
        isFirst = false;
      }
      query.append(")");
    }
  }

  private void buildAddClauseForAlter(StringBuilder query) {
    if (validWriteIdList == null || dir == null) {
      query.setLength(0);
      return;  // avoid NPEs, don't throw an exception but return an empty query
    }
    long minWriteID = validWriteIdList.getMinOpenWriteId() == null ? 1 : validWriteIdList.getMinOpenWriteId();
    long highWatermark = validWriteIdList.getHighWatermark();
    List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories().stream().filter(
            delta -> delta.isDeleteDelta() == isDeleteDelta && delta.getMaxWriteId() <= highWatermark && delta.getMinWriteId() >= minWriteID)
        .collect(Collectors.toList());
    if (deltas.isEmpty()) {
      query.setLength(0); // no alter query needed; clear StringBuilder
      return;
    }
    query.append(" add ");
    deltas.forEach(
        delta -> query.append("partition (file_name='").append(delta.getPath().getName()).append("') location '")
            .append(delta.getPath()).append("' "));
  }

  protected abstract void buildSelectClauseForInsert(StringBuilder query);

  protected void getSourceForInsert(StringBuilder query) {
    if (sourceTabForInsert != null) {
      query.append(sourceTabForInsert);
    } else {
      query.append(sourceTab.getDbName()).append(".").append(sourceTab.getTableName());
    }
    query.append(" ");
  }

  protected void buildWhereClauseForInsert(StringBuilder query) {
    if (sourcePartition != null && sourceTab != null) {
      List<String> vals = sourcePartition.getValues();
      List<FieldSchema> keys = sourceTab.getPartitionKeys();
      if (keys.size() != vals.size()) {
        throw new IllegalStateException("source partition values (" + Arrays.toString(
            vals.toArray()) + ") do not match source table values (" + Arrays.toString(
            keys.toArray()) + "). Failing compaction.");
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
  }

  protected void appendColumns(StringBuilder query, List<FieldSchema> cols, boolean alias) {
    if (cols == null) {
      throw new IllegalStateException("Query could not be created: Source columns are unknown");
    }
    for (int i = 0; i < cols.size(); ++i) {
      if (alias) {
        query.append(i == 0 ? "'" : ", '").append(cols.get(i).getName()).append("', `").append(cols.get(i).getName())
            .append("`");
      } else {
        query.append(i == 0 ? "`" : ", `").append(cols.get(i).getName()).append("`");
      }
    }
  }

  /**
   * Define columns of the create query.
   */
  protected void defineColumns(StringBuilder query) {
    if (sourceTab != null) {
      query.append("(")
          .append("`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint, ")
          .append("`row` struct<")
          .append(StringUtils.join(getColumnDescs(), ','))
          .append(">) ");
    }
  }

  protected List<String> getColumnDescs() {
    List<FieldSchema> cols = sourceTab.getSd().getCols();
    List<String> columnDescs = new ArrayList<>();
    for (FieldSchema col : cols) {
      String columnType = DDLPlanUtils.formatType(TypeInfoUtils.getTypeInfoFromTypeString(col.getType()));
      String columnDesc = "`" + col.getName() + "` " + (!insertOnly ? ":" : "") + columnType;
      columnDescs.add(columnDesc);
    }
    return columnDescs;
  }

}
