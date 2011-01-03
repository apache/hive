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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hive.ql.exec.ExprNodeConstantEvaluator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * HiveHBaseTableInputFormat implements InputFormat for HBase storage handler
 * tables, decorating an underlying HBase TableInputFormat with extra Hive logic
 * such as column pruning and filter pushdown.
 */
public class HiveHBaseTableInputFormat extends TableInputFormatBase
    implements InputFormat<ImmutableBytesWritable, Result> {

  static final Log LOG = LogFactory.getLog(HiveHBaseTableInputFormat.class);

  @Override
  public RecordReader<ImmutableBytesWritable, Result> getRecordReader(
    InputSplit split,
    JobConf jobConf,
    final Reporter reporter) throws IOException {

    HBaseSplit hbaseSplit = (HBaseSplit) split;
    TableSplit tableSplit = hbaseSplit.getSplit();
    String hbaseTableName = jobConf.get(HBaseSerDe.HBASE_TABLE_NAME);
    setHTable(new HTable(new HBaseConfiguration(jobConf), Bytes.toBytes(hbaseTableName)));
    String hbaseColumnsMapping = jobConf.get(HBaseSerDe.HBASE_COLUMNS_MAPPING);
    List<String> hbaseColumnFamilies = new ArrayList<String>();
    List<String> hbaseColumnQualifiers = new ArrayList<String>();
    List<byte []> hbaseColumnFamiliesBytes = new ArrayList<byte []>();
    List<byte []> hbaseColumnQualifiersBytes = new ArrayList<byte []>();

    int iKey;
    try {
      iKey = HBaseSerDe.parseColumnMapping(hbaseColumnsMapping, hbaseColumnFamilies,
          hbaseColumnFamiliesBytes, hbaseColumnQualifiers, hbaseColumnQualifiersBytes);
    } catch (SerDeException se) {
      throw new IOException(se);
    }
    List<Integer> readColIDs = ColumnProjectionUtils.getReadColumnIDs(jobConf);

    if (hbaseColumnFamilies.size() < readColIDs.size()) {
      throw new IOException("Cannot read more columns than the given table contains.");
    }

    boolean addAll = (readColIDs.size() == 0);
    Scan scan = new Scan();
    boolean empty = true;

    if (!addAll) {
      for (int i : readColIDs) {
        if (i == iKey) {
          continue;
        }

        if (hbaseColumnQualifiers.get(i) == null) {
          scan.addFamily(hbaseColumnFamiliesBytes.get(i));
        } else {
          scan.addColumn(hbaseColumnFamiliesBytes.get(i), hbaseColumnQualifiersBytes.get(i));
        }

        empty = false;
      }
    }

    // The HBase table's row key maps to a Hive table column. In the corner case when only the
    // row key column is selected in Hive, the HBase Scan will be empty i.e. no column family/
    // column qualifier will have been added to the scan. We arbitrarily add at least one column
    // to the HBase scan so that we can retrieve all of the row keys and return them as the Hive
    // tables column projection.
    if (empty) {
      for (int i = 0; i < hbaseColumnFamilies.size(); i++) {
        if (i == iKey) {
          continue;
        }

        if (hbaseColumnQualifiers.get(i) == null) {
          scan.addFamily(hbaseColumnFamiliesBytes.get(i));
        } else {
          scan.addColumn(hbaseColumnFamiliesBytes.get(i), hbaseColumnQualifiersBytes.get(i));
        }

        if (!addAll) {
          break;
        }
      }
    }

    // If Hive's optimizer gave us a filter to process, convert it to the
    // HBase scan form now.
    tableSplit = convertFilter(jobConf, scan, tableSplit, iKey);

    setScan(scan);

    Job job = new Job(jobConf);
    TaskAttemptContext tac =
      new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID()) {

        @Override
        public void progress() {
          reporter.progress();
        }
      };

    final org.apache.hadoop.mapreduce.RecordReader<ImmutableBytesWritable, Result>
    recordReader = createRecordReader(tableSplit, tac);

    return new RecordReader<ImmutableBytesWritable, Result>() {

      @Override
      public void close() throws IOException {
        recordReader.close();
      }

      @Override
      public ImmutableBytesWritable createKey() {
        return new ImmutableBytesWritable();
      }

      @Override
      public Result createValue() {
        return new Result();
      }

      @Override
      public long getPos() throws IOException {
        return 0;
      }

      @Override
      public float getProgress() throws IOException {
        float progress = 0.0F;

        try {
          progress = recordReader.getProgress();
        } catch (InterruptedException e) {
          throw new IOException(e);
        }

        return progress;
      }

      @Override
      public boolean next(ImmutableBytesWritable rowKey, Result value) throws IOException {

        boolean next = false;

        try {
          next = recordReader.nextKeyValue();

          if (next) {
            rowKey.set(recordReader.getCurrentValue().getRow());
            Writables.copyWritable(recordReader.getCurrentValue(), value);
          }
        } catch (InterruptedException e) {
          throw new IOException(e);
        }

        return next;
      }
    };
  }

  /**
   * Converts a filter (which has been pushed down from Hive's optimizer)
   * into corresponding restrictions on the HBase scan.  The
   * filter should already be in a form which can be fully converted.
   *
   * @param jobConf configuration for the scan
   *
   * @param scan the HBase scan object to restrict
   *
   * @param tableSplit the HBase table split to restrict, or null
   * if calculating splits
   *
   * @param iKey 0-based offset of key column within Hive table
   *
   * @return converted table split if any
   */
  private TableSplit convertFilter(
    JobConf jobConf,
    Scan scan,
    TableSplit tableSplit,
    int iKey)
    throws IOException {

    String filterExprSerialized =
      jobConf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (filterExprSerialized == null) {
      return tableSplit;
    }
    ExprNodeDesc filterExpr =
      Utilities.deserializeExpression(filterExprSerialized, jobConf);

    String columnNameProperty = jobConf.get(Constants.LIST_COLUMNS);
    List<String> columnNames =
      Arrays.asList(columnNameProperty.split(","));

    IndexPredicateAnalyzer analyzer =
      newIndexPredicateAnalyzer(columnNames.get(iKey));

    List<IndexSearchCondition> searchConditions =
      new ArrayList<IndexSearchCondition>();
    ExprNodeDesc residualPredicate =
      analyzer.analyzePredicate(filterExpr, searchConditions);

    // There should be no residual since we already negotiated
    // that earlier in HBaseStorageHandler.decomposePredicate.
    if (residualPredicate != null) {
      throw new RuntimeException(
        "Unexpected residual predicate " + residualPredicate.getExprString());
    }

    // There should be exactly one predicate since we already
    // negotiated that also.
    if (searchConditions.size() != 1) {
      throw new RuntimeException(
        "Exactly one search condition expected in push down");
    }

    // Convert the search condition into a restriction on the HBase scan
    IndexSearchCondition sc = searchConditions.get(0);
    ExprNodeConstantEvaluator eval =
      new ExprNodeConstantEvaluator(sc.getConstantDesc());
    byte [] startRow;
    try {
      ObjectInspector objInspector = eval.initialize(null);
      Object writable = eval.evaluate(null);
      ByteStream.Output serializeStream = new ByteStream.Output();
      LazyUtils.writePrimitiveUTF8(
        serializeStream,
        writable,
        (PrimitiveObjectInspector) objInspector,
        false,
        (byte) 0,
        null);
      startRow = new byte[serializeStream.getCount()];
      System.arraycopy(
        serializeStream.getData(), 0,
        startRow, 0, serializeStream.getCount());
    } catch (HiveException ex) {
      throw new IOException(ex);
    }

    // stopRow is exclusive, so pad it with a trailing 0 byte to
    // make it compare as the very next value after startRow
    byte [] stopRow = new byte[startRow.length + 1];
    System.arraycopy(startRow, 0, stopRow, 0, startRow.length);

    if (tableSplit != null) {
      tableSplit = new TableSplit(
        tableSplit.getTableName(),
        startRow,
        stopRow,
        tableSplit.getRegionLocation());
    }
    scan.setStartRow(startRow);
    scan.setStopRow(stopRow);

    // Add a WhileMatchFilter to make the scan terminate as soon
    // as we see a non-matching key.  This is probably redundant
    // since the stopRow above should already take care of it for us.
    scan.setFilter(
      new WhileMatchFilter(
        new RowFilter(
          CompareFilter.CompareOp.EQUAL,
          new BinaryComparator(startRow))));
    return tableSplit;
  }

  /**
   * Instantiates a new predicate analyzer suitable for
   * determining how to push a filter down into the HBase scan,
   * based on the rules for what kinds of pushdown we currently support.
   *
   * @param keyColumnName name of the Hive column mapped to the HBase row key
   *
   * @return preconfigured predicate analyzer
   */
  static IndexPredicateAnalyzer newIndexPredicateAnalyzer(
    String keyColumnName) {

    IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();

    // for now, we only support equality comparisons
    analyzer.addComparisonOp(
      "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual");

    // and only on the key column
    analyzer.clearAllowedColumnNames();
    analyzer.allowColumnName(keyColumnName);

    return analyzer;
  }
  
  @Override
  public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {

    String hbaseTableName = jobConf.get(HBaseSerDe.HBASE_TABLE_NAME);
    setHTable(new HTable(new HBaseConfiguration(jobConf), Bytes.toBytes(hbaseTableName)));
    String hbaseColumnsMapping = jobConf.get(HBaseSerDe.HBASE_COLUMNS_MAPPING);

    if (hbaseColumnsMapping == null) {
      throw new IOException("hbase.columns.mapping required for HBase Table.");
    }

    List<String> hbaseColumnFamilies = new ArrayList<String>();
    List<String> hbaseColumnQualifiers = new ArrayList<String>();
    List<byte []> hbaseColumnFamiliesBytes = new ArrayList<byte []>();
    List<byte []> hbaseColumnQualifiersBytes = new ArrayList<byte []>();

    int iKey;
    try {
      iKey = HBaseSerDe.parseColumnMapping(hbaseColumnsMapping, hbaseColumnFamilies,
          hbaseColumnFamiliesBytes, hbaseColumnQualifiers, hbaseColumnQualifiersBytes);
    } catch (SerDeException se) {
      throw new IOException(se);
    }

    Scan scan = new Scan();

    // Take filter pushdown into account while calculating splits; this
    // allows us to prune off regions immediately.  Note that although
    // the Javadoc for the superclass getSplits says that it returns one
    // split per region, the implementation actually takes the scan
    // definition into account and excludes regions which don't satisfy
    // the start/stop row conditions (HBASE-1829).
    convertFilter(jobConf, scan, null, iKey);

    // REVIEW:  are we supposed to be applying the getReadColumnIDs
    // same as in getRecordReader?
    for (int i = 0; i < hbaseColumnFamilies.size(); i++) {
      if (i == iKey) {
        continue;
      }

      if (hbaseColumnQualifiers.get(i) == null) {
        scan.addFamily(hbaseColumnFamiliesBytes.get(i));
      } else {
        scan.addColumn(hbaseColumnFamiliesBytes.get(i), hbaseColumnQualifiersBytes.get(i));
      }
    }

    setScan(scan);
    Job job = new Job(jobConf);
    JobContext jobContext = new JobContext(job.getConfiguration(), job.getJobID());
    Path [] tablePaths = FileInputFormat.getInputPaths(jobContext);

    List<org.apache.hadoop.mapreduce.InputSplit> splits =
      super.getSplits(jobContext);
    InputSplit [] results = new InputSplit[splits.size()];

    for (int i = 0; i < splits.size(); i++) {
      results[i] = new HBaseSplit((TableSplit) splits.get(i), tablePaths[0]);
    }

    return results;
  }
}
