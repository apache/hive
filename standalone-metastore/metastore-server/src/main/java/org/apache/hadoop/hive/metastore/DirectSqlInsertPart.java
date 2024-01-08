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

package org.apache.hadoop.hive.metastore;

import static org.apache.commons.lang3.StringUtils.repeat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jdo.PersistenceManager;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.model.MColumnDescriptor;
import org.apache.hadoop.hive.metastore.model.MFieldSchema;
import org.apache.hadoop.hive.metastore.model.MOrder;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionPrivilege;
import org.apache.hadoop.hive.metastore.model.MSerDeInfo;
import org.apache.hadoop.hive.metastore.model.MStorageDescriptor;
import org.apache.hadoop.hive.metastore.model.MStringList;
import org.datanucleus.ExecutionContext;
import org.datanucleus.api.jdo.JDOPersistenceManager;
import org.datanucleus.identity.DatastoreId;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;

/**
 * This class contains the methods to insert into tables on the underlying database using direct SQL
 */
class DirectSqlInsertPart {
  private final PersistenceManager pm;
  private final DatabaseProduct dbType;
  private final int batchSize;

  public DirectSqlInsertPart(PersistenceManager pm, DatabaseProduct dbType, int batchSize) {
    this.pm = pm;
    this.dbType = dbType;
    this.batchSize = batchSize;
  }

  /**
   * Interface to execute multiple row insert query in batch for direct SQL
   */
  interface BatchExecutionContext {
    void execute(String batchQueryText, int batchRowCount) throws MetaException;
  }

  private Long getDataStoreId(Class<?> modelClass) throws MetaException {
    ExecutionContext ec = ((JDOPersistenceManager) pm).getExecutionContext();
    AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(modelClass, ec.getClassLoaderResolver());
    if (cmd.getIdentityType() == IdentityType.DATASTORE) {
      return (Long) ec.getStoreManager().getValueGenerationStrategyValue(ec, cmd, -1);
    } else {
      throw new MetaException("Identity type is not datastore.");
    }
  }

  private void insertInBatch(String tableName, String columns, int columnCount, int rowCount,
      BatchExecutionContext batchExecutionContext) throws MetaException {
    if (rowCount == 0 || columnCount == 0) {
      return;
    }
    int maxRowsInBatch = batchSize > 0 ? batchSize : rowCount;
    maxRowsInBatch = dbType.getMaxRows(maxRowsInBatch, columnCount);
    int maxBatches = rowCount / maxRowsInBatch;
    int last = rowCount % maxRowsInBatch;
    String rowFormat = "(" + repeat(",?", columnCount).substring(1) + ")";
    String query = "";
    if (maxBatches > 0) {
      query = dbType.getBatchInsertQuery(tableName, columns, rowFormat, maxRowsInBatch);
    }
    for (int batch = 0; batch < maxBatches; batch++) {
      batchExecutionContext.execute(query, maxRowsInBatch);
    }
    if (last != 0) {
      query = dbType.getBatchInsertQuery(tableName, columns, rowFormat, last);
      batchExecutionContext.execute(query, last);
    }
  }

  private void executeQuery(String queryText, Object[] params) throws MetaException {
    try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      MetastoreDirectSqlUtils.executeWithArray(query.getInnerQuery(), params, queryText);
    }
  }

  private void insertSerdeInBatch(Map<Long, MSerDeInfo> serdeIdToSerDeInfo) throws MetaException {
    int rowCount = serdeIdToSerDeInfo.size();
    String columns = "(\"SERDE_ID\",\"DESCRIPTION\",\"DESERIALIZER_CLASS\",\"NAME\",\"SERDE_TYPE\",\"SLIB\","
        + "\"SERIALIZER_CLASS\")";
    int columnCount = 7;
    BatchExecutionContext batchExecutionContext = new BatchExecutionContext() {
      final Iterator<Map.Entry<Long, MSerDeInfo>> it = serdeIdToSerDeInfo.entrySet().iterator();
      @Override
      public void execute(String batchQueryText, int batchRowCount) throws MetaException {
        Object[] params = new Object[batchRowCount * columnCount];
        int paramIndex = 0;
        for (int index = 0; index < batchRowCount; index++) {
          Map.Entry<Long, MSerDeInfo> entry = it.next();
          MSerDeInfo serdeInfo = entry.getValue();
          params[paramIndex++] = entry.getKey();
          params[paramIndex++] = serdeInfo.getDescription();
          params[paramIndex++] = serdeInfo.getDeserializerClass();
          params[paramIndex++] = serdeInfo.getName();
          params[paramIndex++] = serdeInfo.getSerdeType();
          params[paramIndex++] = serdeInfo.getSerializationLib();
          params[paramIndex++] = serdeInfo.getSerializerClass();
        }
        executeQuery(batchQueryText, params);
      }
    };
    insertInBatch("\"SERDES\"", columns, columnCount, rowCount, batchExecutionContext);
  }

  private void insertStorageDescriptorInBatch(Map<Long, MStorageDescriptor> sdIdToStorageDescriptor,
      Map<Long, Long> sdIdToSerdeId, Map<Long, Long> sdIdToCdId) throws MetaException {
    int rowCount = sdIdToStorageDescriptor.size();
    String columns = "(\"SD_ID\",\"CD_ID\",\"INPUT_FORMAT\",\"IS_COMPRESSED\",\"IS_STOREDASSUBDIRECTORIES\","
        + "\"LOCATION\",\"NUM_BUCKETS\",\"OUTPUT_FORMAT\",\"SERDE_ID\")";
    int columnCount = 9;
    BatchExecutionContext batchExecutionContext = new BatchExecutionContext() {
      final Iterator<Map.Entry<Long, MStorageDescriptor>> it = sdIdToStorageDescriptor.entrySet().iterator();
      @Override
      public void execute(String batchQueryText, int batchRowCount) throws MetaException {
        Object[] params = new Object[batchRowCount * columnCount];
        int paramIndex = 0;
        for (int index = 0; index < batchRowCount; index++) {
          Map.Entry<Long, MStorageDescriptor> entry = it.next();
          MStorageDescriptor sd = entry.getValue();
          params[paramIndex++] = entry.getKey();
          params[paramIndex++] = sdIdToCdId.get(entry.getKey());
          params[paramIndex++] = sd.getInputFormat();
          params[paramIndex++] = dbType.getBoolean(sd.isCompressed());
          params[paramIndex++] = dbType.getBoolean(sd.isStoredAsSubDirectories());
          params[paramIndex++] = sd.getLocation();
          params[paramIndex++] = sd.getNumBuckets();
          params[paramIndex++] = sd.getOutputFormat();
          params[paramIndex++] = sdIdToSerdeId.get(entry.getKey());
        }
        executeQuery(batchQueryText, params);
      }
    };
    insertInBatch("\"SDS\"", columns, columnCount, rowCount, batchExecutionContext);
  }

  private void insertPartitionInBatch(Map<Long, MPartition> partIdToPartition, Map<Long, Long> partIdToSdId)
      throws MetaException {
    int rowCount = partIdToPartition.size();
    String columns = "(\"PART_ID\",\"CREATE_TIME\",\"LAST_ACCESS_TIME\",\"PART_NAME\",\"SD_ID\",\"TBL_ID\","
        + "\"WRITE_ID\")";
    int columnCount = 7;
    BatchExecutionContext batchExecutionContext = new BatchExecutionContext() {
      final Iterator<Map.Entry<Long, MPartition>> it = partIdToPartition.entrySet().iterator();
      @Override
      public void execute(String batchQueryText, int batchRowCount) throws MetaException {
        Object[] params = new Object[batchRowCount * columnCount];
        int paramIndex = 0;
        for (int index = 0; index < batchRowCount; index++) {
          Map.Entry<Long, MPartition> entry = it.next();
          MPartition partition = entry.getValue();
          params[paramIndex++] = entry.getKey();
          params[paramIndex++] = partition.getCreateTime();
          params[paramIndex++] = partition.getLastAccessTime();
          params[paramIndex++] = partition.getPartitionName();
          params[paramIndex++] = partIdToSdId.get(entry.getKey());
          params[paramIndex++] = partition.getTable().getId();
          params[paramIndex++] = partition.getWriteId();
        }
        executeQuery(batchQueryText, params);
      }
    };
    insertInBatch("\"PARTITIONS\"", columns, columnCount, rowCount, batchExecutionContext);
  }

  private void insertSerdeParamInBatch(Map<Long, MSerDeInfo> serdeIdToSerDeInfo) throws MetaException {
    int rowCount = 0;
    for (MSerDeInfo serDeInfo : serdeIdToSerDeInfo.values()) {
      rowCount += serDeInfo.getParameters() != null ? serDeInfo.getParameters().size() : 0;
    }
    if (rowCount == 0) {
      return;
    }
    String columns = "(\"SERDE_ID\",\"PARAM_KEY\",\"PARAM_VALUE\")";
    int columnCount = 3;
    BatchExecutionContext batchExecutionContext = new BatchExecutionContext() {
      final Iterator<Map.Entry<Long, MSerDeInfo>> serdeIt = serdeIdToSerDeInfo.entrySet().iterator();
      Map.Entry<Long, MSerDeInfo> serdeEntry = serdeIt.next();
      Iterator<Map.Entry<String, String>> it = serdeEntry.getValue().getParameters() != null  ?
          serdeEntry.getValue().getParameters().entrySet().iterator() : Collections.emptyIterator();
      @Override
      public void execute(String batchQueryText, int batchRowCount) throws MetaException {
        Object[] params = new Object[batchRowCount * columnCount];
        int index = 0;
        int paramIndex = 0;
        do {
          while (index < batchRowCount && it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            params[paramIndex++] = serdeEntry.getKey();
            params[paramIndex++] = entry.getKey();
            params[paramIndex++] = entry.getValue();
            index++;
          }
          if (index < batchRowCount) {
            serdeEntry = serdeIt.next(); // serdeIt.next() cannot be null since it is within the row count
            it = serdeEntry.getValue().getParameters() != null  ?
                serdeEntry.getValue().getParameters().entrySet().iterator() : Collections.emptyIterator();
          }
        } while (index < batchRowCount);
        executeQuery(batchQueryText, params);
      }
    };
    insertInBatch("\"SERDE_PARAMS\"", columns, columnCount, rowCount, batchExecutionContext);
  }

  private void insertStorageDescriptorParamInBatch(Map<Long, MStorageDescriptor> sdIdToStorageDescriptor)
      throws MetaException {
    int rowCount = 0;
    for (MStorageDescriptor sd : sdIdToStorageDescriptor.values()) {
      rowCount += sd.getParameters() != null ? sd.getParameters().size() : 0;
    }
    if (rowCount == 0) {
      return;
    }
    String columns = "(\"SD_ID\",\"PARAM_KEY\",\"PARAM_VALUE\")";
    int columnCount = 3;
    BatchExecutionContext batchExecutionContext = new BatchExecutionContext() {
      final Iterator<Map.Entry<Long, MStorageDescriptor>> sdIt = sdIdToStorageDescriptor.entrySet().iterator();
      Map.Entry<Long, MStorageDescriptor> sdEntry = sdIt.next();
      Iterator<Map.Entry<String, String>> it = sdEntry.getValue().getParameters() != null ?
          sdEntry.getValue().getParameters().entrySet().iterator() : Collections.emptyIterator();

      @Override
      public void execute(String batchQueryText, int batchRowCount) throws MetaException {
        Object[] params = new Object[batchRowCount * columnCount];
        int index = 0;
        int paramIndex = 0;
        do {
          while (index < batchRowCount && it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            params[paramIndex++] = sdEntry.getKey();
            params[paramIndex++] = entry.getKey();
            params[paramIndex++] = entry.getValue();
            index++;
          }
          if (index < batchRowCount) {
            sdEntry = sdIt.next(); // sdIt.next() cannot be null since it is within the row count
            it = sdEntry.getValue().getParameters() != null  ?
                sdEntry.getValue().getParameters().entrySet().iterator() : Collections.emptyIterator();
          }
        } while (index < batchRowCount);
        executeQuery(batchQueryText, params);
      }
    };
    insertInBatch("\"SD_PARAMS\"", columns, columnCount, rowCount, batchExecutionContext);
  }

  private void insertPartitionParamInBatch(Map<Long, MPartition> partIdToPartition) throws MetaException {
    int rowCount = 0;
    for (MPartition part : partIdToPartition.values()) {
      rowCount += part.getParameters() != null ? part.getParameters().size() : 0;
    }
    if (rowCount == 0) {
      return;
    }
    String columns = "(\"PART_ID\",\"PARAM_KEY\",\"PARAM_VALUE\")";
    int columnCount = 3;
    BatchExecutionContext batchExecutionContext = new BatchExecutionContext() {
      final Iterator<Map.Entry<Long, MPartition>> partIt = partIdToPartition.entrySet().iterator();
      Map.Entry<Long, MPartition> partEntry = partIt.next();
      Iterator<Map.Entry<String, String>> it = partEntry.getValue().getParameters() != null ?
          partEntry.getValue().getParameters().entrySet().iterator() : Collections.emptyIterator();
      @Override
      public void execute(String batchQueryText, int batchRowCount) throws MetaException {
        Object[] params = new Object[batchRowCount * columnCount];
        int index = 0;
        int paramIndex = 0;
        do {
          while (index < batchRowCount && it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            params[paramIndex++] = partEntry.getKey();
            params[paramIndex++] = entry.getKey();
            params[paramIndex++] = entry.getValue();
            index++;
          }
          if (index < batchRowCount) {
            partEntry = partIt.next(); // partIt.next() cannot be null since it is within the row count
            it = partEntry.getValue().getParameters() != null ?
                partEntry.getValue().getParameters().entrySet().iterator() : Collections.emptyIterator();
          }
        } while (index < batchRowCount);
        executeQuery(batchQueryText, params);
      }
    };
    insertInBatch("\"PARTITION_PARAMS\"", columns, columnCount, rowCount, batchExecutionContext);
  }

  private void insertPartitionKeyValInBatch(Map<Long, MPartition> partIdToPartition) throws MetaException {
    int rowCount = 0;
    for (MPartition part : partIdToPartition.values()) {
      rowCount += part.getValues().size();
    }
    if (rowCount == 0) {
      return;
    }
    String columns = "(\"PART_ID\",\"PART_KEY_VAL\",\"INTEGER_IDX\")";
    int columnCount = 3;
    BatchExecutionContext batchExecutionContext = new BatchExecutionContext() {
      int colIndex = 0;
      final Iterator<Map.Entry<Long, MPartition>> partIt = partIdToPartition.entrySet().iterator();
      Map.Entry<Long, MPartition> partEntry = partIt.next();
      Iterator<String> it = partEntry.getValue().getValues().iterator();
      @Override
      public void execute(String batchQueryText, int batchRowCount) throws MetaException {
        Object[] params = new Object[batchRowCount * columnCount];
        int index = 0;
        int paramIndex = 0;
        do {
          while (index < batchRowCount && it.hasNext()) {
            params[paramIndex++] = partEntry.getKey();
            params[paramIndex++] = it.next();
            params[paramIndex++] = colIndex++;
            index++;
          }
          if (index < batchRowCount) {
            colIndex = 0;
            partEntry = partIt.next(); // partIt.next() cannot be null since it is within the row count
            it = partEntry.getValue().getValues().iterator();
          }
        } while (index < batchRowCount);
        executeQuery(batchQueryText, params);
      }
    };
    insertInBatch("\"PARTITION_KEY_VALS\"", columns, columnCount, rowCount, batchExecutionContext);
  }

  private void insertColumnDescriptorInBatch(Map<Long, MColumnDescriptor> cdIdToColumnDescriptor) throws MetaException {
    int rowCount = cdIdToColumnDescriptor.size();
    String columns = "(\"CD_ID\")";
    int columnCount = 1;
    BatchExecutionContext batchExecutionContext = new BatchExecutionContext() {
      final Iterator<Long> it = cdIdToColumnDescriptor.keySet().iterator();
      @Override
      public void execute(String batchQueryText, int batchRowCount) throws MetaException {
        Object[] params = new Object[batchRowCount * columnCount];
        int paramIndex = 0;
        for (int index = 0; index < batchRowCount; index++) {
          params[paramIndex++] = it.next();
        }
        executeQuery(batchQueryText, params);
      }
    };
    insertInBatch("\"CDS\"", columns, columnCount, rowCount, batchExecutionContext);
  }

  private void insertColumnV2InBatch(Map<Long, MColumnDescriptor> cdIdToColumnDescriptor) throws MetaException {
    int rowCount = 0;
    for (MColumnDescriptor cd : cdIdToColumnDescriptor.values()) {
      rowCount += cd.getCols().size();
    }
    if (rowCount == 0) {
      return;
    }
    String columns = "(\"CD_ID\",\"COMMENT\",\"COLUMN_NAME\",\"TYPE_NAME\",\"INTEGER_IDX\")";
    int columnCount = 5;
    BatchExecutionContext batchExecutionContext = new BatchExecutionContext() {
      int colIndex = 0;
      final Iterator<Map.Entry<Long, MColumnDescriptor>> cdIt = cdIdToColumnDescriptor.entrySet().iterator();
      Map.Entry<Long, MColumnDescriptor> cdEntry = cdIt.next();
      Iterator<MFieldSchema> it = cdEntry.getValue().getCols().iterator();
      @Override
      public void execute(String batchQueryText, int batchRowCount) throws MetaException {
        Object[] params = new Object[batchRowCount * columnCount];
        int index = 0;
        int paramIndex = 0;
        do {
          while (index < batchRowCount && it.hasNext()) {
            MFieldSchema fieldSchema = it.next();
            params[paramIndex++] = cdEntry.getKey();
            params[paramIndex++] = fieldSchema.getComment();
            params[paramIndex++] = fieldSchema.getName();
            params[paramIndex++] = fieldSchema.getType();
            params[paramIndex++] = colIndex++;
            index++;
          }
          if (index < batchRowCount) {
            colIndex = 0;
            cdEntry = cdIt.next(); // cdIt.next() cannot be null since it is within the row count
            it = cdEntry.getValue().getCols().iterator();
          }
        } while (index < batchRowCount);
        executeQuery(batchQueryText, params);
      }
    };
    insertInBatch("\"COLUMNS_V2\"", columns, columnCount, rowCount, batchExecutionContext);
  }

  private void insertBucketColInBatch(Map<Long, MStorageDescriptor> sdIdToStorageDescriptor) throws MetaException {
    int rowCount = 0;
    for (MStorageDescriptor sd : sdIdToStorageDescriptor.values()) {
      rowCount += sd.getBucketCols() != null ? sd.getBucketCols().size() : 0;
    }
    if (rowCount == 0) {
      return;
    }
    String columns = "(\"SD_ID\",\"BUCKET_COL_NAME\",\"INTEGER_IDX\")";
    int columnCount = 3;
    BatchExecutionContext batchExecutionContext = new BatchExecutionContext() {
      int colIndex = 0;
      final Iterator<Map.Entry<Long, MStorageDescriptor>> sdIt = sdIdToStorageDescriptor.entrySet().iterator();
      Map.Entry<Long, MStorageDescriptor> sdEntry = sdIt.next();
      Iterator<String> it = sdEntry.getValue().getBucketCols() != null ?
          sdEntry.getValue().getBucketCols().iterator() : Collections.emptyIterator();
      @Override
      public void execute(String batchQueryText, int batchRowCount) throws MetaException {
        Object[] params = new Object[batchRowCount * columnCount];
        int index = 0;
        int paramIndex = 0;
        do {
          while (index < batchRowCount && it.hasNext()) {
            params[paramIndex++] = sdEntry.getKey();
            params[paramIndex++] = it.next();
            params[paramIndex++] = colIndex++;
            index++;
          }
          if (index < batchRowCount) {
            colIndex = 0;
            sdEntry = sdIt.next(); // sdIt.next() cannot be null since it is within the row count
            it = sdEntry.getValue().getBucketCols() != null ?
                sdEntry.getValue().getBucketCols().iterator() : Collections.emptyIterator();
          }
        } while (index < batchRowCount);
        executeQuery(batchQueryText, params);
      }
    };
    insertInBatch("\"BUCKETING_COLS\"", columns, columnCount, rowCount, batchExecutionContext);
  }

  private void insertSortColInBatch(Map<Long, MStorageDescriptor> sdIdToStorageDescriptor) throws MetaException {
    int rowCount = 0;
    for (MStorageDescriptor sd : sdIdToStorageDescriptor.values()) {
      rowCount += sd.getSortCols() != null ? sd.getSortCols().size() : 0;
    }
    if (rowCount == 0) {
      return;
    }
    String columns = "(\"SD_ID\",\"COLUMN_NAME\",\"ORDER\",\"INTEGER_IDX\")";
    int columnCount = 4;
    BatchExecutionContext batchExecutionContext = new BatchExecutionContext() {
      int colIndex = 0;
      final Iterator<Map.Entry<Long, MStorageDescriptor>> sdIt = sdIdToStorageDescriptor.entrySet().iterator();
      Map.Entry<Long, MStorageDescriptor> sdEntry = sdIt.next();
      Iterator<MOrder> it = sdEntry.getValue().getSortCols() != null ?
          sdEntry.getValue().getSortCols().iterator() : Collections.emptyIterator();
      @Override
      public void execute(String batchQueryText, int batchRowCount) throws MetaException {
        Object[] params = new Object[batchRowCount * columnCount];
        int index = 0;
        int paramIndex = 0;
        do {
          while (index < batchRowCount && it.hasNext()) {
            MOrder order = it.next();
            params[paramIndex++] = sdEntry.getKey();
            params[paramIndex++] = order.getCol();
            params[paramIndex++] = order.getOrder();
            params[paramIndex++] = colIndex++;
            index++;
          }
          if (index < batchRowCount) {
            colIndex = 0;
            sdEntry = sdIt.next(); // sdIt.next() cannot be null since it is within the row count
            it = sdEntry.getValue().getSortCols() != null ?
                sdEntry.getValue().getSortCols().iterator() : Collections.emptyIterator();
          }
        } while (index < batchRowCount);
        executeQuery(batchQueryText, params);
      }
    };
    insertInBatch("\"SORT_COLS\"", columns, columnCount, rowCount, batchExecutionContext);
  }

  private void insertSkewedStringListInBatch(List<Long> stringListIds) throws MetaException {
    int rowCount = stringListIds.size();
    String columns = "(\"STRING_LIST_ID\")";
    int columnCount = 1;
    BatchExecutionContext batchExecutionContext = new BatchExecutionContext() {
      final Iterator<Long> it = stringListIds.iterator();
      @Override
      public void execute(String batchQueryText, int batchRowCount) throws MetaException {
        Object[] params = new Object[batchRowCount * columnCount];
        int paramIndex = 0;
        for (int index = 0; index < batchRowCount; index++) {
          params[paramIndex++] = it.next();
        }
        executeQuery(batchQueryText, params);
      }
    };
    insertInBatch("\"SKEWED_STRING_LIST\"", columns, columnCount, rowCount, batchExecutionContext);
  }

  private void insertSkewedStringListValInBatch(Map<Long, List<String>> stringListIdToStringList) throws MetaException {
    int rowCount = 0;
    for (List<String> stringList : stringListIdToStringList.values()) {
      rowCount += stringList.size();
    }
    if (rowCount == 0) {
      return;
    }
    String columns = "(\"STRING_LIST_ID\",\"STRING_LIST_VALUE\",\"INTEGER_IDX\")";
    int columnCount = 3;
    BatchExecutionContext batchExecutionContext = new BatchExecutionContext() {
      int colIndex = 0;
      final Iterator<Map.Entry<Long, List<String>>> stringListIt = stringListIdToStringList.entrySet().iterator();
      Map.Entry<Long, List<String>> stringListEntry = stringListIt.next();
      Iterator<String> it = stringListEntry.getValue().iterator();
      @Override
      public void execute(String batchQueryText, int batchRowCount) throws MetaException {
        Object[] params = new Object[batchRowCount * columnCount];
        int index = 0;
        int paramIndex = 0;
        do {
          while (index < batchRowCount && it.hasNext()) {
            params[paramIndex++] = stringListEntry.getKey();
            params[paramIndex++] = it.next();
            params[paramIndex++] = colIndex++;
            index++;
          }
          if (index < batchRowCount) {
            colIndex = 0;
            stringListEntry = stringListIt.next(); // stringListIt.next() cannot be null since it is within row count
            it = stringListEntry.getValue().iterator();
          }
        } while (index < batchRowCount);
        executeQuery(batchQueryText, params);
      }
    };
    insertInBatch("\"SKEWED_STRING_LIST_VALUES\"", columns, columnCount, rowCount, batchExecutionContext);
  }

  private void insertSkewedColInBatch(Map<Long, MStorageDescriptor> sdIdToStorageDescriptor) throws MetaException {
    int rowCount = 0;
    for (MStorageDescriptor sd : sdIdToStorageDescriptor.values()) {
      rowCount += sd.getSkewedColNames() != null ? sd.getSkewedColNames().size() : 0;
    }
    if (rowCount == 0) {
      return;
    }
    String columns = "(\"SD_ID\",\"SKEWED_COL_NAME\",\"INTEGER_IDX\")";
    int columnCount = 3;
    BatchExecutionContext batchExecutionContext = new BatchExecutionContext() {
      int colIndex = 0;
      final Iterator<Map.Entry<Long, MStorageDescriptor>> sdIt = sdIdToStorageDescriptor.entrySet().iterator();
      Map.Entry<Long, MStorageDescriptor> sdEntry = sdIt.next();
      Iterator<String> it = sdEntry.getValue().getSkewedColNames() != null ?
          sdEntry.getValue().getSkewedColNames().iterator() : Collections.emptyIterator();
      @Override
      public void execute(String batchQueryText, int batchRowCount) throws MetaException {
        Object[] params = new Object[batchRowCount * columnCount];
        int index = 0;
        int paramIndex = 0;
        do {
          while (index < batchRowCount && it.hasNext()) {
            params[paramIndex++] = sdEntry.getKey();
            params[paramIndex++] = it.next();
            params[paramIndex++] = colIndex++;
            index++;
          }
          if (index < batchRowCount) {
            colIndex = 0;
            sdEntry = sdIt.next(); // sdIt.next() cannot be null since it is within the row count
            it = sdEntry.getValue().getSkewedColNames() != null ?
                sdEntry.getValue().getSkewedColNames().iterator() : Collections.emptyIterator();
          }
        } while (index < batchRowCount);
        executeQuery(batchQueryText, params);
      }
    };
    insertInBatch("\"SKEWED_COL_NAMES\"", columns, columnCount, rowCount, batchExecutionContext);
  }

  private void insertSkewedValInBatch(List<Long> stringListIds, Map<Long, Long> stringListIdToSdId)
      throws MetaException {
    int rowCount = stringListIds.size();
    String columns = "(\"SD_ID_OID\",\"STRING_LIST_ID_EID\",\"INTEGER_IDX\")";
    int columnCount = 3;
    BatchExecutionContext batchExecutionContext = new BatchExecutionContext() {
      int colIndex = 0;
      long prevSdId = -1;
      final Iterator<Long> it = stringListIds.iterator();
      @Override
      public void execute(String batchQueryText, int batchRowCount) throws MetaException {
        Object[] params = new Object[batchRowCount * columnCount];
        int paramIndex = 0;
        for (int index = 0; index < batchRowCount; index++) {
          Long stringListId = it.next();
          Long sdId = stringListIdToSdId.get(stringListId);
          params[paramIndex++] = sdId;
          params[paramIndex++] = stringListId;
          if (prevSdId != sdId) {
            colIndex = 0;
          }
          params[paramIndex++] = colIndex++;
          prevSdId = sdId;
        }
        executeQuery(batchQueryText, params);
      }
    };
    insertInBatch("\"SKEWED_VALUES\"", columns, columnCount, rowCount, batchExecutionContext);
  }

  private void insertSkewedLocationInBatch(Map<Long, String> stringListIdToLocation, Map<Long, Long> stringListIdToSdId)
      throws MetaException {
    int rowCount = stringListIdToLocation.size();
    String columns = "(\"SD_ID\",\"STRING_LIST_ID_KID\",\"LOCATION\")";
    int columnCount = 3;
    BatchExecutionContext batchExecutionContext = new BatchExecutionContext() {
      final Iterator<Map.Entry<Long, String>> it = stringListIdToLocation.entrySet().iterator();
      @Override
      public void execute(String batchQueryText, int batchRowCount) throws MetaException {
        Object[] params = new Object[batchRowCount * columnCount];
        int paramIndex = 0;
        for (int index = 0; index < batchRowCount; index++) {
          Map.Entry<Long, String> entry = it.next();
          params[paramIndex++] = stringListIdToSdId.get(entry.getKey());
          params[paramIndex++] = entry.getKey();
          params[paramIndex++] = entry.getValue();
        }
        executeQuery(batchQueryText, params);
      }
    };
    insertInBatch("\"SKEWED_COL_VALUE_LOC_MAP\"", columns, columnCount, rowCount, batchExecutionContext);
  }

  private void insertPartitionPrivilegeInBatch(Map<Long, MPartitionPrivilege> partGrantIdToPrivilege,
      Map<Long, Long> partGrantIdToPartId) throws MetaException {
    int rowCount = partGrantIdToPrivilege.size();
    String columns = "(\"PART_GRANT_ID\",\"AUTHORIZER\",\"CREATE_TIME\",\"GRANT_OPTION\",\"GRANTOR\",\"GRANTOR_TYPE\","
        + "\"PART_ID\",\"PRINCIPAL_NAME\",\"PRINCIPAL_TYPE\",\"PART_PRIV\")";
    int columnCount = 10;
    BatchExecutionContext batchExecutionContext = new BatchExecutionContext() {
      final Iterator<Map.Entry<Long, MPartitionPrivilege>> it = partGrantIdToPrivilege.entrySet().iterator();
      @Override
      public void execute(String batchQueryText, int batchRowCount) throws MetaException {
        Object[] params = new Object[batchRowCount * columnCount];
        int paramIndex = 0;
        for (int index = 0; index < batchRowCount; index++) {
          Map.Entry<Long, MPartitionPrivilege> entry = it.next();
          MPartitionPrivilege partPrivilege = entry.getValue();
          params[paramIndex++] = entry.getKey();
          params[paramIndex++] = partPrivilege.getAuthorizer();
          params[paramIndex++] = partPrivilege.getCreateTime();
          params[paramIndex++] = partPrivilege.getGrantOption() ? 1 : 0;
          params[paramIndex++] = partPrivilege.getGrantor();
          params[paramIndex++] = partPrivilege.getGrantorType();
          params[paramIndex++] = partGrantIdToPartId.get(entry.getKey());
          params[paramIndex++] = partPrivilege.getPrincipalName();
          params[paramIndex++] = partPrivilege.getPrincipalType();
          params[paramIndex++] = partPrivilege.getPrivilege();
        }
        executeQuery(batchQueryText, params);
      }
    };
    insertInBatch("\"PART_PRIVS\"", columns, columnCount, rowCount, batchExecutionContext);
  }

  private void insertPartitionColPrivilegeInBatch(Map<Long, MPartitionColumnPrivilege> partColumnGrantIdToPrivilege,
      Map<Long, Long> partColumnGrantIdToPartId) throws MetaException {
    int rowCount = partColumnGrantIdToPrivilege.size();
    String columns = "(\"PART_COLUMN_GRANT_ID\",\"AUTHORIZER\",\"COLUMN_NAME\",\"CREATE_TIME\",\"GRANT_OPTION\","
        + "\"GRANTOR\",\"GRANTOR_TYPE\",\"PART_ID\",\"PRINCIPAL_NAME\",\"PRINCIPAL_TYPE\",\"PART_COL_PRIV\")";
    int columnCount = 11;
    BatchExecutionContext batchExecutionContext = new BatchExecutionContext() {
      final Iterator<Map.Entry<Long, MPartitionColumnPrivilege>> it
          = partColumnGrantIdToPrivilege.entrySet().iterator();
      @Override
      public void execute(String batchQueryText, int batchRowCount) throws MetaException {
        Object[] params = new Object[batchRowCount * columnCount];
        int paramIndex = 0;
        for (int index = 0; index < batchRowCount; index++) {
          Map.Entry<Long, MPartitionColumnPrivilege> entry = it.next();
          MPartitionColumnPrivilege partColumnPrivilege = entry.getValue();
          params[paramIndex++] = entry.getKey();
          params[paramIndex++] = partColumnPrivilege.getAuthorizer();
          params[paramIndex++] = partColumnPrivilege.getColumnName();
          params[paramIndex++] = partColumnPrivilege.getCreateTime();
          params[paramIndex++] = partColumnPrivilege.getGrantOption() ? 1 : 0;
          params[paramIndex++] = partColumnPrivilege.getGrantor();
          params[paramIndex++] = partColumnPrivilege.getGrantorType();
          params[paramIndex++] = partColumnGrantIdToPartId.get(entry.getKey());
          params[paramIndex++] = partColumnPrivilege.getPrincipalName();
          params[paramIndex++] = partColumnPrivilege.getPrincipalType();
          params[paramIndex++] = partColumnPrivilege.getPrivilege();
        }
        executeQuery(batchQueryText, params);
      }
    };
    insertInBatch("\"PART_COL_PRIVS\"", columns, columnCount, rowCount, batchExecutionContext);
  }

  /**
   * Add partitions in batch using direct SQL
   * @param parts list of partitions
   * @param partPrivilegesList list of partition privileges
   * @param partColPrivilegesList list of partition column privileges
   * @throws MetaException
   */
  public void addPartitions(List<MPartition> parts, List<List<MPartitionPrivilege>> partPrivilegesList,
      List<List<MPartitionColumnPrivilege>> partColPrivilegesList) throws MetaException {
    Map<Long, MSerDeInfo> serdeIdToSerDeInfo = new HashMap<>();
    Map<Long, MColumnDescriptor> cdIdToColumnDescriptor = new HashMap<>();
    Map<Long, MStorageDescriptor> sdIdToStorageDescriptor = new HashMap<>();
    Map<Long, MPartition> partIdToPartition = new HashMap<>();
    Map<Long, MPartitionPrivilege> partGrantIdToPrivilege = new HashMap<>();
    Map<Long, MPartitionColumnPrivilege> partColumnGrantIdToPrivilege = new HashMap<>();
    Map<Long, Long> sdIdToSerdeId = new HashMap<>();
    Map<Long, Long> sdIdToCdId = new HashMap<>();
    Map<Long, Long> partIdToSdId = new HashMap<>();
    Map<Long, List<String>> stringListIdToStringList = new HashMap<>();
    Map<Long, Long> stringListIdToSdId = new HashMap<>();
    Map<Long, String> stringListIdToLocation = new HashMap<>();
    Map<Long, Long> partGrantIdToPartId = new HashMap<>();
    Map<Long, Long> partColumnGrantIdToPartId = new HashMap<>();
    List<Long> stringListIds = new ArrayList<>();
    int partitionsCount = parts.size();
    for (int index = 0; index < partitionsCount; index++) {
      MPartition part = parts.get(index);
      MStorageDescriptor sd = part.getSd();
      if (part.getValues() == null || sd == null || sd.getSerDeInfo() == null || sd.getCD() == null
          || sd.getCD().getCols() == null) {
        throw new MetaException("Invalid partition");
      }
      Long serDeId = getDataStoreId(MSerDeInfo.class);
      serdeIdToSerDeInfo.put(serDeId, sd.getSerDeInfo());

      Long cdId;
      DatastoreId storeId = (DatastoreId) pm.getObjectId(sd.getCD());
      if (storeId == null) {
        cdId = getDataStoreId(MColumnDescriptor.class);
        cdIdToColumnDescriptor.put(cdId, sd.getCD());
      } else {
        cdId = (Long) storeId.getKeyAsObject();
      }

      Long sdId = getDataStoreId(MStorageDescriptor.class);
      sdIdToStorageDescriptor.put(sdId, sd);
      sdIdToSerdeId.put(sdId, serDeId);
      sdIdToCdId.put(sdId, cdId);

      Long partId = getDataStoreId(MPartition.class);
      partIdToPartition.put(partId, part);
      partIdToSdId.put(partId, sdId);

      Map<List<String>, String> stringListToLocation = new HashMap<>();
      if (sd.getSkewedColValueLocationMaps() != null) {
        for (Map.Entry<MStringList, String> entry : sd.getSkewedColValueLocationMaps().entrySet()) {
          stringListToLocation.put(entry.getKey().getInternalList(), entry.getValue());
        }
      }
      if (CollectionUtils.isNotEmpty(sd.getSkewedColValues())) {
        int skewedValCount = sd.getSkewedColValues().size();
        for (int i = 0; i < skewedValCount; i++) {
          Long stringListId = getDataStoreId(MStringList.class);
          stringListIds.add(stringListId);
          stringListIdToSdId.put(stringListId, sdId);
          List<String> stringList = sd.getSkewedColValues().get(i).getInternalList();
          stringListIdToStringList.put(stringListId, stringList);
          String location = stringListToLocation.get(stringList);
          if (location != null) {
            stringListIdToLocation.put(stringListId, location);
          }
        }
      }

      List<MPartitionPrivilege> partPrivileges = partPrivilegesList.get(index);
      for (MPartitionPrivilege partPrivilege : partPrivileges) {
        Long partGrantId = getDataStoreId(MPartitionPrivilege.class);
        partGrantIdToPrivilege.put(partGrantId, partPrivilege);
        partGrantIdToPartId.put(partGrantId, partId);
      }
      List<MPartitionColumnPrivilege> partColPrivileges = partColPrivilegesList.get(index);
      for (MPartitionColumnPrivilege partColPrivilege : partColPrivileges) {
        Long partColumnGrantId = getDataStoreId(MPartitionColumnPrivilege.class);
        partColumnGrantIdToPrivilege.put(partColumnGrantId, partColPrivilege);
        partColumnGrantIdToPartId.put(partColumnGrantId, partId);
      }
    }
    insertSerdeInBatch(serdeIdToSerDeInfo);
    insertSerdeParamInBatch(serdeIdToSerDeInfo);
    insertColumnDescriptorInBatch(cdIdToColumnDescriptor);
    insertColumnV2InBatch(cdIdToColumnDescriptor);
    insertStorageDescriptorInBatch(sdIdToStorageDescriptor, sdIdToSerdeId, sdIdToCdId);
    insertStorageDescriptorParamInBatch(sdIdToStorageDescriptor);
    insertBucketColInBatch(sdIdToStorageDescriptor);
    insertSortColInBatch(sdIdToStorageDescriptor);
    insertSkewedColInBatch(sdIdToStorageDescriptor);
    insertSkewedStringListInBatch(stringListIds);
    insertSkewedStringListValInBatch(stringListIdToStringList);
    insertSkewedValInBatch(stringListIds, stringListIdToSdId);
    insertSkewedLocationInBatch(stringListIdToLocation, stringListIdToSdId);
    insertPartitionInBatch(partIdToPartition, partIdToSdId);
    insertPartitionParamInBatch(partIdToPartition);
    insertPartitionKeyValInBatch(partIdToPartition);
    insertPartitionPrivilegeInBatch(partGrantIdToPrivilege, partGrantIdToPartId);
    insertPartitionColPrivilegeInBatch(partColumnGrantIdToPrivilege, partColumnGrantIdToPartId);
  }
}
