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

package org.apache.hadoop.hive.ql.hooks;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.io.Serializable;

/**
 * This class encapsulates an object that is being written to by the query. This
 * object may be a table, partition, dfs directory or a local directory.
 */
public class WriteEntity extends Entity implements Serializable {
  private static final long serialVersionUID = 1L;

  private boolean isTempURI = false;
  private transient boolean isDynamicPartitionWrite = false;
  private transient boolean isTxnAnalyze = false;

  public static enum WriteType {
    DDL_EXCLUSIVE, // for use in DDL statements that require an exclusive lock,
                   // such as dropping a table or partition
    DDL_EXCL_WRITE, // for use in DDL operations that can allow concurrent reads, like truncate in acid
    DDL_SHARED, // for use in DDL operations that only need a shared lock, such as creating a table
    DDL_NO_LOCK, // for use in DDL statements that do not require a lock
    INSERT,
    INSERT_OVERWRITE,
    UPDATE,
    DELETE,
    CTAS,
    PATH_WRITE, // Write to a URI, no locking done for this
  };

  private WriteType writeType;

  /**
   * Only used by serialization.
   */
  public WriteEntity() {
    super();
  }

  public WriteEntity(Database database, WriteType type) {
    super(database, true);
    setWriteTypeInternal(type);
  }

  public WriteEntity(DataConnector connector, WriteType type) {
    super(connector, true);
    setWriteTypeInternal(type);
  }

  /**
   * Constructor for a table.
   *
   * @param t
   *          Table that is written to.
   */
  public WriteEntity(Table t, WriteType type) {
    super(t, true);
    setWriteTypeInternal(type);
  }

  public WriteEntity(Table t, WriteType type, boolean complete) {
    super(t, complete);
    setWriteTypeInternal(type);
  }

  public WriteEntity(Function function, WriteType type) {
    super(function, true);
    setWriteTypeInternal(type);
  }

  /**
   * Constructor for objects represented as String.
   * Currently applicable only for function names.
   * @param db
   * @param objName
   * @param className
   * @param type
   * @param writeType
   */
  public WriteEntity(Database db, String objName, String className, Type type, WriteType writeType) {
    super(db, objName, className, type);
    this.writeType = writeType;
  }

  /**
   * Constructor for a partition.
   *
   * @param p
   *          Partition that is written to.
   */
  public WriteEntity(Partition p, WriteType type) {
    super(p, true);
    setWriteTypeInternal(type);
  }

  public WriteEntity(DummyPartition p, WriteType type, boolean complete) {
    super(p, complete);
    setWriteTypeInternal(type);
  }

  /**
   * Constructor for a file.
   *
   * @param d
   *          The name of the directory that is being written to.
   * @param isLocal
   *          Flag to decide whether this directory is local or in dfs.
   */
  public WriteEntity(Path d, boolean isLocal) {
    this(d, isLocal, false);
  }

  /**
   * Constructor for a file.
   *
   * @param d
   *          The name of the directory that is being written to.
   * @param isLocal
   *          Flag to decide whether this directory is local or in dfs.
   * @param isTemp
   *          True if this is a temporary location such as scratch dir
   */
  public WriteEntity(Path d, boolean isLocal, boolean isTemp) {
    super(d, isLocal, true);
    this.isTempURI = isTemp;
    this.writeType = WriteType.PATH_WRITE;
  }

  public WriteEntity(String name, Type t) {
    super(name, t);
    this.writeType = WriteType.DDL_NO_LOCK;
  }

  /**
   * Determine which type of write this is.  This is needed by the lock
   * manager so it can understand what kind of lock to acquire.
   * @return write type
   */
  public WriteType getWriteType() {
    return writeType;
  }

  private void setWriteTypeInternal(WriteType type) {
    writeType = type;
  }

  /**
   * Equals function.
   */
  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }

    if (o instanceof WriteEntity) {
      WriteEntity ore = (WriteEntity) o;
      return (getName().equalsIgnoreCase(ore.getName())) && this.writeType == ore.writeType;
    } else {
      return false;
    }
  }

  public String toStringDetail() {
    return "WriteEntity(" + toString() + ") Type=" + getType() + " WriteType=" + getWriteType();
  }

  public boolean isTempURI() {
    return isTempURI;
  }

  /**
   * Determine the type of lock to request for a given alter table type.
   * @param op Operation type from the alter table description
   * @return the write type this should use.
   */
  public static WriteType determineAlterTableWriteType(AlterTableType op, Table table, HiveConf conf) {
    switch (op) {
      case ARCHIVE:
      case UNARCHIVE:
        // Archiving methods are currently disabled
      case ALTERLOCATION:
        // alter table {table_name} [partition ({partition_spec})] set location "{new_location}"
      case DROPPARTITION:
        // Not used, @see org.apache.hadoop.hive.ql.ddl.table.partition.drop.AlterTableDropPartitionAnalyzer
        // alter table {table_name} drop [if exists] partition ({partition_spec}) [, partition ({partition_spec}), ...] [purge]
      case RENAMEPARTITION:
        // Not used, @see org.apache.hadoop.hive.ql.ddl.table.partition.rename.AlterTableRenamePartitionAnalyzer
        // alter table {table_name} partition {partition_spec} rename to partition {partition_spec}
      case SKEWED_BY:
        // Not used, @see org.apache.hadoop.hive.ql.ddl.table.storage.skewed.AlterTableSkewedByAnalyzer
        // alter table {table_name} skewed by (col_name1, col_name2, ...)
        //   on ([(col_name1_value, col_name2_value, ...) [, (col_name1_value, col_name2_value), ...] [stored as directories]
      case SET_SKEWED_LOCATION:
        // alter table {table_name} set skewed location (col_name1="location1" [, col_name2="location2", ...] )
      case INTO_BUCKETS:
        // Not used, @see org.apache.hadoop.hive.ql.ddl.table.storage.cluster.AlterTableIntoBucketsAnalyzer
        // alter table {table_name} [partition ({partition_spec})] into {bucket_number} buckets
      case ALTERPARTITION:
        // Not used: @see org.apache.hadoop.hive.ql.ddl.table.partition.alter.AlterTableAlterPartitionAnalyzer
        // alter table {table_name} partition column ({column_name} {column_type})
      case TRUNCATE:
        // truncate table {table_name} [partition ({partition_spec})] columns ({column_spec})
        // Also @see org.apache.hadoop.hive.ql.ddl.table.misc.truncate.TruncateTableAnalyzer
      case MERGEFILES:
        // alter table {table_name} [partition (partition_key = 'partition_value' [, ...])] concatenate
        // Also @see org.apache.hadoop.hive.ql.ddl.table.storage.concatenate.AlterTableConcatenateAnalyzer
        if (AcidUtils.isLocklessReadsEnabled(table, conf)) {
          throw new UnsupportedOperationException(op.name());
        } else {
          return WriteType.DDL_EXCLUSIVE;
        }

      case CLUSTERED_BY:
        // alter table {table_name} clustered by (col_name, col_name, ...) [sorted by (col_name, ...)]
        //    into {num_buckets} buckets;
      case NOT_SORTED:
      case NOT_CLUSTERED:
      case SET_FILE_FORMAT:
        // alter table {table_name} [partition ({partition_spec})] set fileformat {file_format}
      case SET_SERDE:
        // alter table {table_name} [PARTITION ({partition_spec})] set serde '{serde_class_name}'
      case ADDCOLS:
      case REPLACE_COLUMNS:
        // alter table {table_name} [partition ({partition_spec})] add/replace columns ({col_name} {data_type})
      case RENAME_COLUMN:
        // alter table {table_name} [partition ({partition_spec})] change column {column_name} {column_name} {data_type}
      case ADD_CONSTRAINT:
      case DROP_CONSTRAINT:
      case OWNER:
      case RENAME:
        // alter table {table_name} rename to {new_table_name}
      case DROPPROPS:
        return AcidUtils.isLocklessReadsEnabled(table, conf) ?
            WriteType.DDL_EXCL_WRITE : WriteType.DDL_EXCLUSIVE;

      case ADDPARTITION:
        // Not used: @see org.apache.hadoop.hive.ql.ddl.table.partition.add.AbstractAddPartitionAnalyzer
        // alter table {table_name} add [if not exists] partition ({partition_spec}) [location '{location}']
        //   [, partition ({partition_spec}) [location '{location}'], ...];
      case SET_SERDE_PROPS:
      case ADDPROPS:
      case UPDATESTATS:
        return WriteType.DDL_SHARED;

      case COMPACT:
        // alter table {table_name} [partition (partition_key = 'partition_value' [, ...])]
        //    compact 'compaction_type'[and wait] [with overwrite tblproperties ("property"="value" [, ...])];
      case TOUCH:
        // alter table {table_name} touch [partition ({partition_spec})]
        return WriteType.DDL_NO_LOCK;

      default:
        throw new RuntimeException("Unknown operation " + op.toString());
    }
  }

  public boolean isDynamicPartitionWrite() {
    return isDynamicPartitionWrite;
  }
  public void setDynamicPartitionWrite(boolean t) {
    isDynamicPartitionWrite = t;
  }
  public String toDetailedString() {
    return toString() + " Type=" + getTyp() + " WriteType=" + getWriteType() + " isDP=" + isDynamicPartitionWrite();
  }

  public boolean isTxnAnalyze() {
    return isTxnAnalyze;
  }

  public void setTxnAnalyze(boolean isTxnAnalyze) {
    this.isTxnAnalyze = isTxnAnalyze;
  }
}
