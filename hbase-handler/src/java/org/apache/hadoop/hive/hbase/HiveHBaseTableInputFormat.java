/**
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

package org.apache.hadoop.hive.hbase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.mapred.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * HiveHBaseTableInputFormat implements InputFormat for HBase storage handler
 * tables, decorating an underlying HBase TableInputFormat with extra Hive logic
 * such as column pruning.
 */
public class HiveHBaseTableInputFormat<K extends ImmutableBytesWritable, V extends RowResult>
    implements InputFormat<K, V>, JobConfigurable {
  
  static final Log LOG = LogFactory.getLog(HiveHBaseTableInputFormat.class);
  
  private HBaseExposedTableInputFormat hbaseInputFormat;

  public HiveHBaseTableInputFormat() {
    hbaseInputFormat = new HBaseExposedTableInputFormat();
  }

  @Override
  public RecordReader<K, V> getRecordReader(
    InputSplit split, JobConf job,
    Reporter reporter) throws IOException {

    HBaseSplit hbaseSplit = (HBaseSplit) split;

    byte [] tableNameBytes;
    String hbaseTableName = job.get(HBaseSerDe.HBASE_TABLE_NAME);
    hbaseInputFormat.setHBaseTable(
      new HTable(
        new HBaseConfiguration(job),
        Bytes.toBytes(hbaseTableName)));
    
    // because the hbase key is mapped to the first column in its hive table,
    // we add the "_key" before the columnMapping that we can use the
    // hive column id to find the exact hbase column one-for-one.
    String columnMapping = "_key," + hbaseSplit.getColumnsMapping();
    String[] columns = columnMapping.split(",");   
    List<Integer> readColIDs =
      ColumnProjectionUtils.getReadColumnIDs(job);
 
    if (columns.length < readColIDs.size()) {
      throw new IOException(
        "Cannot read more columns than the given table contains.");
    }
    
    byte [][] scanColumns;
    if (readColIDs.size() == 0) {
      scanColumns = new byte[columns.length - 1][];
      for (int i=0; i < columns.length - 1; i++) {
        scanColumns[i] = Bytes.toBytes(columns[i + 1]);
      }
    } else {
      Collections.sort(readColIDs);
      
      if (readColIDs.get(0) == 0) {
        // sql like "select key from hbasetable;"
        // As HBase can not scan a hbase table while just getting its keys,
        // so we will scan out the second column of the hive table
        // but ignore it.
        if (readColIDs.size() == 1) {
          scanColumns = new byte[1][];
          scanColumns[0] = Bytes.toBytes(columns[1]);
        } else {
          scanColumns = new byte[readColIDs.size() - 1][];
          for (int i=0; i<scanColumns.length; i++) {
            scanColumns[i] = Bytes.toBytes(columns[readColIDs.get(i + 1)]);
          }
        }
      } else {
        scanColumns = new byte[readColIDs.size()][];
        for (int i=0; i<scanColumns.length; i++) {
          scanColumns[i] = Bytes.toBytes(columns[readColIDs.get(i)]);
        }
      }
    }
    
    hbaseInputFormat.setScanColumns(scanColumns);
    
    return (RecordReader<K, V>)
      hbaseInputFormat.getRecordReader(hbaseSplit.getSplit(), job, reporter);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    Path [] tableNames = FileInputFormat.getInputPaths(job);
    String hbaseTableName = job.get(HBaseSerDe.HBASE_TABLE_NAME);
    hbaseInputFormat.setHBaseTable(
      new HTable(new HBaseConfiguration(job), hbaseTableName));
    
    String hbaseSchemaMapping = job.get(HBaseSerDe.HBASE_COL_MAPPING);
    if (hbaseSchemaMapping == null) {
      throw new IOException("hbase.columns.mapping required for HBase Table.");
    }
    
    String [] columns = hbaseSchemaMapping.split(",");
    byte [][] inputColumns = new byte[columns.length][];
    for (int i=0; i < columns.length; i++) {
      inputColumns[i] = Bytes.toBytes(columns[i]);
    }
    
    hbaseInputFormat.setScanColumns(inputColumns);
    
    InputSplit[] splits = hbaseInputFormat.getSplits(
      job, numSplits <= 0 ? 1 : numSplits);
    InputSplit[] results = new InputSplit[splits.length];
    for (int i = 0; i < splits.length; i++) {
      results[i] = new HBaseSplit(
        (TableSplit) splits[i], hbaseSchemaMapping, tableNames[0]);
    }
    return results;
  }
 
  @Override
  public void configure(JobConf job) {
    hbaseInputFormat.configure(job);
  }

  /**
   * HBaseExposedTableInputFormat exposes some protected methods
   * from the HBase TableInputFormatBase.
   */
  static class HBaseExposedTableInputFormat
    extends org.apache.hadoop.hbase.mapred.TableInputFormatBase
    implements JobConfigurable {

    @Override
    public void configure(JobConf job) {
      // not needed for now
    }
    
    public void setScanColumns(byte[][] scanColumns) {
      setInputColumns(scanColumns);
    }
    
    public void setHBaseTable(HTable table) {
      setHTable(table);
    }
  }
}
