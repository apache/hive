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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.ql.exec.ExprNodeConstantEvaluator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * HiveHBaseTableInputFormat implements InputFormat for HBase storage handler
 * tables, decorating an underlying HBase TableInputFormat with extra Hive logic
 * such as column pruning and filter pushdown.
 */
public class HiveHBaseTableInputFormat extends TableInputFormatBase
    implements InputFormat<ImmutableBytesWritable, ResultWritable> {

  static final Log LOG = LogFactory.getLog(HiveHBaseTableInputFormat.class);
  private static final Object hbaseTableMonitor = new Object();

  @Override
  public RecordReader<ImmutableBytesWritable, ResultWritable> getRecordReader(
    InputSplit split,
    JobConf jobConf,
    final Reporter reporter) throws IOException {

    HBaseSplit hbaseSplit = (HBaseSplit) split;
    TableSplit tableSplit = hbaseSplit.getTableSplit();

    setHTable(HiveHBaseInputFormatUtil.getTable(jobConf));
    setScan(HiveHBaseInputFormatUtil.getScan(jobConf));

    Job job = new Job(jobConf);
    TaskAttemptContext tac = ShimLoader.getHadoopShims().newTaskAttemptContext(
        job.getConfiguration(), reporter);

    final org.apache.hadoop.mapreduce.RecordReader<ImmutableBytesWritable, Result>
    recordReader = createRecordReader(tableSplit, tac);
    try {
      recordReader.initialize(tableSplit, tac);
    } catch (InterruptedException e) {
      throw new IOException("Failed to initialize RecordReader", e);
    }

    return new RecordReader<ImmutableBytesWritable, ResultWritable>() {

      @Override
      public void close() throws IOException {
        recordReader.close();
      }

      @Override
      public ImmutableBytesWritable createKey() {
        return new ImmutableBytesWritable();
      }

      @Override
      public ResultWritable createValue() {
        return new ResultWritable(new Result());
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
      public boolean next(ImmutableBytesWritable rowKey, ResultWritable value) throws IOException {

        boolean next = false;

        try {
          next = recordReader.nextKeyValue();

          if (next) {
            rowKey.set(recordReader.getCurrentValue().getRow());
            value.setResult(recordReader.getCurrentValue());
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
   * @param iKey 0-based offset of key column within Hive table
   *
   * @return converted table split if any
   */
  private Scan createFilterScan(JobConf jobConf, int iKey, int iTimestamp, boolean isKeyBinary)
      throws IOException {

    // TODO: assert iKey is HBaseSerDe#HBASE_KEY_COL

    Scan scan = new Scan();
    String filterObjectSerialized = jobConf.get(TableScanDesc.FILTER_OBJECT_CONF_STR);
    if (filterObjectSerialized != null) {
      HBaseScanRange range = Utilities.deserializeObject(filterObjectSerialized,
          HBaseScanRange.class);
      try {
        range.setup(scan, jobConf);
      } catch (Exception e) {
        throw new IOException(e);
      }
      return scan;
    }

    String filterExprSerialized = jobConf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (filterExprSerialized == null) {
      return scan;
    }

    ExprNodeGenericFuncDesc filterExpr =
      Utilities.deserializeExpression(filterExprSerialized);

    String keyColName = jobConf.get(serdeConstants.LIST_COLUMNS).split(",")[iKey];
    String colType = jobConf.get(serdeConstants.LIST_COLUMN_TYPES).split(",")[iKey];
    boolean isKeyComparable = isKeyBinary || colType.equalsIgnoreCase("string");

    String tsColName = null;
    if (iTimestamp >= 0) {
      tsColName = jobConf.get(serdeConstants.LIST_COLUMNS).split(",")[iTimestamp];
    }

    IndexPredicateAnalyzer analyzer =
        newIndexPredicateAnalyzer(keyColName, isKeyComparable, tsColName);

    List<IndexSearchCondition> conditions = new ArrayList<IndexSearchCondition>();
    ExprNodeDesc residualPredicate = analyzer.analyzePredicate(filterExpr, conditions);

    // There should be no residual since we already negotiated that earlier in
    // HBaseStorageHandler.decomposePredicate. However, with hive.optimize.index.filter
    // OpProcFactory#pushFilterToStorageHandler pushes the original filter back down again.
    // Since pushed-down filters are not omitted at the higher levels (and thus the
    // contract of negotiation is ignored anyway), just ignore the residuals.
    // Re-assess this when negotiation is honored and the duplicate evaluation is removed.
    // THIS IGNORES RESIDUAL PARSING FROM HBaseStorageHandler#decomposePredicate
    if (residualPredicate != null) {
      LOG.debug("Ignoring residual predicate " + residualPredicate.getExprString());
    }

    Map<String, List<IndexSearchCondition>> split = HiveHBaseInputFormatUtil.decompose(conditions);
    List<IndexSearchCondition> keyConditions = split.get(keyColName);
    if (keyConditions != null && !keyConditions.isEmpty()) {
      setupKeyRange(scan, keyConditions, isKeyBinary);
    }
    List<IndexSearchCondition> tsConditions = split.get(tsColName);
    if (tsConditions != null && !tsConditions.isEmpty()) {
      setupTimeRange(scan, tsConditions);
    }
    return scan;
  }

  private void setupKeyRange(Scan scan, List<IndexSearchCondition> conditions, boolean isBinary)
      throws IOException {
    // Convert the search condition into a restriction on the HBase scan
    byte [] startRow = HConstants.EMPTY_START_ROW, stopRow = HConstants.EMPTY_END_ROW;
    for (IndexSearchCondition sc : conditions) {

      ExprNodeConstantEvaluator eval = new ExprNodeConstantEvaluator(sc.getConstantDesc());
      PrimitiveObjectInspector objInspector;
      Object writable;

      try {
        objInspector = (PrimitiveObjectInspector)eval.initialize(null);
        writable = eval.evaluate(null);
      } catch (ClassCastException cce) {
        throw new IOException("Currently only primitve types are supported. Found: " +
            sc.getConstantDesc().getTypeString());
      } catch (HiveException e) {
        throw new IOException(e);
      }

      byte[] constantVal = getConstantVal(writable, objInspector, isBinary);
      String comparisonOp = sc.getComparisonOp();

      if("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual".equals(comparisonOp)){
        startRow = constantVal;
        stopRow = getNextBA(constantVal);
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan".equals(comparisonOp)){
        stopRow = constantVal;
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan"
          .equals(comparisonOp)) {
        startRow = constantVal;
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan"
          .equals(comparisonOp)){
        startRow = getNextBA(constantVal);
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan"
          .equals(comparisonOp)){
        stopRow = getNextBA(constantVal);
      } else {
        throw new IOException(comparisonOp + " is not a supported comparison operator");
      }
    }
    scan.setStartRow(startRow);
    scan.setStopRow(stopRow);

    if (LOG.isDebugEnabled()) {
      LOG.debug(Bytes.toStringBinary(startRow) + " ~ " + Bytes.toStringBinary(stopRow));
    }
  }

  private void setupTimeRange(Scan scan, List<IndexSearchCondition> conditions)
      throws IOException {
    long start = 0;
    long end = Long.MAX_VALUE;
    for (IndexSearchCondition sc : conditions) {
      long timestamp = getTimestampVal(sc);
      String comparisonOp = sc.getComparisonOp();
      if("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual".equals(comparisonOp)){
        start = timestamp;
        end = timestamp + 1;
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan".equals(comparisonOp)){
        end = timestamp;
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan"
          .equals(comparisonOp)) {
        start = timestamp;
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan"
          .equals(comparisonOp)){
        start = timestamp + 1;
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan"
          .equals(comparisonOp)){
        end = timestamp + 1;
      } else {
        throw new IOException(comparisonOp + " is not a supported comparison operator");
      }
    }
    scan.setTimeRange(start, end);
  }

  private long getTimestampVal(IndexSearchCondition sc) throws IOException {
    long timestamp;
    try {
      ExprNodeConstantEvaluator eval = new ExprNodeConstantEvaluator(sc.getConstantDesc());
      ObjectInspector inspector = eval.initialize(null);
      Object value = eval.evaluate(null);
      if (inspector instanceof LongObjectInspector) {
        timestamp = ((LongObjectInspector)inspector).get(value);
      } else {
        PrimitiveObjectInspector primitive = (PrimitiveObjectInspector) inspector;
        timestamp = PrimitiveObjectInspectorUtils.getTimestamp(value, primitive).getTime();
      }
    } catch (HiveException e) {
      throw new IOException(e);
    }
    return timestamp;
  }

    private byte[] getConstantVal(Object writable, PrimitiveObjectInspector poi,
        boolean isKeyBinary) throws IOException{

        if (!isKeyBinary){
          // Key is stored in text format. Get bytes representation of constant also of
          // text format.
          byte[] startRow;
          ByteStream.Output serializeStream = new ByteStream.Output();
          LazyUtils.writePrimitiveUTF8(serializeStream, writable, poi, false, (byte) 0, null);
          startRow = new byte[serializeStream.getLength()];
          System.arraycopy(serializeStream.getData(), 0, startRow, 0, serializeStream.getLength());
          return startRow;
        }

        PrimitiveCategory pc = poi.getPrimitiveCategory();
        switch (poi.getPrimitiveCategory()) {
        case INT:
            return Bytes.toBytes(((IntWritable)writable).get());
        case BOOLEAN:
            return Bytes.toBytes(((BooleanWritable)writable).get());
        case LONG:
            return Bytes.toBytes(((LongWritable)writable).get());
        case FLOAT:
            return Bytes.toBytes(((FloatWritable)writable).get());
        case DOUBLE:
            return Bytes.toBytes(((DoubleWritable)writable).get());
        case SHORT:
            return Bytes.toBytes(((ShortWritable)writable).get());
        case STRING:
            return Bytes.toBytes(((Text)writable).toString());
        case BYTE:
            return Bytes.toBytes(((ByteWritable)writable).get());

        default:
          throw new IOException("Type not supported " + pc);
        }
      }


  private byte[] getNextBA(byte[] current){
    // startRow is inclusive while stopRow is exclusive,
    // this util method returns very next bytearray which will occur after the current one
    // by padding current one with a trailing 0 byte.
    byte[] next = new byte[current.length + 1];
    System.arraycopy(current, 0, next, 0, current.length);
    return next;
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
      String keyColumnName, boolean isKeyComparable, String timestampColumn) {

    IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();

    // We can always do equality predicate. Just need to make sure we get appropriate
    // BA representation of constant of filter condition.
    // We can do other comparisons only if storage format in hbase is either binary
    // or we are dealing with string types since there lexicographic ordering will suffice.
    if (isKeyComparable) {
      analyzer.addComparisonOp(keyColumnName,
          "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual",
          "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan",
          "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan",
          "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan",
          "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan");
    } else {
      analyzer.addComparisonOp(keyColumnName,
          "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual");
    }

    if (timestampColumn != null) {
      analyzer.addComparisonOp(timestampColumn,
          "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual",
          "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan",
          "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan",
          "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan",
          "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan");
    }

    return analyzer;
  }

  @Override
  public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    synchronized (hbaseTableMonitor) {
      return getSplitsInternal(jobConf, numSplits);
    }
  }

  private InputSplit[] getSplitsInternal(JobConf jobConf, int numSplits) throws IOException {

    //obtain delegation tokens for the job
    if (UserGroupInformation.getCurrentUser().hasKerberosCredentials()) {
      TableMapReduceUtil.initCredentials(jobConf);
    }

    String hbaseTableName = jobConf.get(HBaseSerDe.HBASE_TABLE_NAME);
    setHTable(new HTable(HBaseConfiguration.create(jobConf), Bytes.toBytes(hbaseTableName)));
    String hbaseColumnsMapping = jobConf.get(HBaseSerDe.HBASE_COLUMNS_MAPPING);
    boolean doColumnRegexMatching = jobConf.getBoolean(HBaseSerDe.HBASE_COLUMNS_REGEX_MATCHING, true);

    if (hbaseColumnsMapping == null) {
      throw new IOException(HBaseSerDe.HBASE_COLUMNS_MAPPING + " required for HBase Table.");
    }

    ColumnMappings columnMappings = null;
    try {
      columnMappings = HBaseSerDe.parseColumnsMapping(hbaseColumnsMapping, doColumnRegexMatching);
    } catch (SerDeException e) {
      throw new IOException(e);
    }

    int iKey = columnMappings.getKeyIndex();
    int iTimestamp = columnMappings.getTimestampIndex();
    ColumnMapping keyMapping = columnMappings.getKeyMapping();

    // Take filter pushdown into account while calculating splits; this
    // allows us to prune off regions immediately.  Note that although
    // the Javadoc for the superclass getSplits says that it returns one
    // split per region, the implementation actually takes the scan
    // definition into account and excludes regions which don't satisfy
    // the start/stop row conditions (HBASE-1829).
    Scan scan = createFilterScan(jobConf, iKey, iTimestamp,
        HiveHBaseInputFormatUtil.getStorageFormatOfKey(keyMapping.mappingSpec,
            jobConf.get(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE, "string")));

    // The list of families that have been added to the scan
    List<String> addedFamilies = new ArrayList<String>();

    // REVIEW:  are we supposed to be applying the getReadColumnIDs
    // same as in getRecordReader?
    for (ColumnMapping colMap : columnMappings) {
      if (colMap.hbaseRowKey || colMap.hbaseTimestamp) {
        continue;
      }

      if (colMap.qualifierName == null) {
        scan.addFamily(colMap.familyNameBytes);
        addedFamilies.add(colMap.familyName);
      } else {
        if(!addedFamilies.contains(colMap.familyName)){
          // add the column only if the family has not already been added
          scan.addColumn(colMap.familyNameBytes, colMap.qualifierNameBytes);
        }
      }
    }
    setScan(scan);

    Job job = new Job(jobConf);
    JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job);
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
