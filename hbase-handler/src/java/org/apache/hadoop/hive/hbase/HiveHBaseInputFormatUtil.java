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

package org.apache.hadoop.hive.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.ql.exec.ExprNodeConstantEvaluator;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
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
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util code common between HiveHBaseTableInputFormat and HiveHBaseTableSnapshotInputFormat.
 */
class HiveHBaseInputFormatUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HiveHBaseInputFormatUtil.class);

  /**
   * Parse {@code jobConf} to create a {@link Scan} instance.
   */
  public static Scan getScan(JobConf jobConf) throws IOException {
    String hbaseColumnsMapping = jobConf.get(HBaseSerDe.HBASE_COLUMNS_MAPPING);
    boolean doColumnRegexMatching = jobConf.getBoolean(HBaseSerDe.HBASE_COLUMNS_REGEX_MATCHING, true);
    List<Integer> readColIDs = ColumnProjectionUtils.getReadColumnIDs(jobConf);
    ColumnMappings columnMappings;

    try {
      columnMappings = HBaseSerDe.parseColumnsMapping(hbaseColumnsMapping, doColumnRegexMatching);
    } catch (SerDeException e) {
      throw new IOException(e);
    }

    if (columnMappings.size() < readColIDs.size()) {
      throw new IOException("Cannot read more columns than the given table contains.");
    }

    boolean readAllColumns = ColumnProjectionUtils.isReadAllColumns(jobConf);
    Scan scan = new Scan();
    boolean empty = true;

    // The list of families that have been added to the scan
    List<String> addedFamilies = new ArrayList<String>();

    if (!readAllColumns) {
      ColumnMapping[] columnsMapping = columnMappings.getColumnsMapping();
      for (int i : readColIDs) {
        ColumnMapping colMap = columnsMapping[i];
        if (colMap.hbaseRowKey || colMap.hbaseTimestamp) {
          continue;
        }

        if (colMap.qualifierName == null) {
          scan.addFamily(colMap.familyNameBytes);
          addedFamilies.add(colMap.familyName);
        } else {
          if(!addedFamilies.contains(colMap.familyName)){
            // add only if the corresponding family has not already been added
            scan.addColumn(colMap.familyNameBytes, colMap.qualifierNameBytes);
          }
        }

        empty = false;
      }
    }

    // If we have cases where we are running a query like count(key) or count(*),
    // in such cases, the readColIDs is either empty(for count(*)) or has just the
    // key column in it. In either case, nothing gets added to the scan. So if readAllColumns is
    // true, we are going to add all columns. Else we are just going to add a key filter to run a
    // count only on the keys
    if (empty) {
      if (readAllColumns) {
        for (ColumnMapping colMap: columnMappings) {
          if (colMap.hbaseRowKey || colMap.hbaseTimestamp) {
            continue;
          }

          if (colMap.qualifierName == null) {
            scan.addFamily(colMap.familyNameBytes);
          } else {
            scan.addColumn(colMap.familyNameBytes, colMap.qualifierNameBytes);
          }
        }
      } else {
        // Add a filter to just do a scan on the keys so that we pick up everything
        scan.setFilter(new FilterList(new FirstKeyOnlyFilter(), new KeyOnlyFilter()));
      }
    }

    String scanCache = jobConf.get(HBaseSerDe.HBASE_SCAN_CACHE);
    if (scanCache != null) {
      scan.setCaching(Integer.parseInt(scanCache));
    }

    boolean scanCacheBlocks =
        jobConf.getBoolean(HBaseSerDe.HBASE_SCAN_CACHEBLOCKS, false);
    scan.setCacheBlocks(scanCacheBlocks);

    String scanBatch = jobConf.get(HBaseSerDe.HBASE_SCAN_BATCH);
    if (scanBatch != null) {
      scan.setBatch(Integer.parseInt(scanBatch));
    }

    String filterObjectSerialized = jobConf.get(TableScanDesc.FILTER_OBJECT_CONF_STR);

    if (filterObjectSerialized != null) {
      setupScanRange(scan, filterObjectSerialized, jobConf, true);
    }

    return scan;
  }

  public static boolean getStorageFormatOfKey(String spec, String defaultFormat) throws IOException{

    String[] mapInfo = spec.split("#");
    boolean tblLevelDefault = "binary".equalsIgnoreCase(defaultFormat);

    switch (mapInfo.length) {
      case 1:
        return tblLevelDefault;

      case 2:
        String storageType = mapInfo[1];
        if(storageType.equals("-")) {
          return tblLevelDefault;
        } else if ("string".startsWith(storageType)){
          return false;
        } else if ("binary".startsWith(storageType)){
          return true;
        }

      default:
        throw new IOException("Malformed string: " + spec);
    }
  }

  public static Map<String, List<IndexSearchCondition>> decompose(
      List<IndexSearchCondition> searchConditions) {
    Map<String, List<IndexSearchCondition>> result =
        new HashMap<String, List<IndexSearchCondition>>();
    for (IndexSearchCondition condition : searchConditions) {
      List<IndexSearchCondition> conditions = result.get(condition.getColumnDesc().getColumn());
      if (conditions == null) {
        conditions = new ArrayList<IndexSearchCondition>();
        result.put(condition.getColumnDesc().getColumn(), conditions);
      }
      conditions.add(condition);
    }
    return result;
  }

  static void setupScanRange(Scan scan, String filterObjectSerialized, JobConf jobConf,
      boolean filterOnly) throws IOException {
    HBaseScanRange range =
            SerializationUtilities.deserializeObject(filterObjectSerialized,
                    HBaseScanRange.class);
    try {
      range.setup(scan, jobConf, filterOnly);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  static void setupKeyRange(Scan scan, List<IndexSearchCondition> conditions, boolean isBinary)
      throws IOException {
    // Convert the search condition into a restriction on the HBase scan
    byte[] startRow = HConstants.EMPTY_START_ROW, stopRow = HConstants.EMPTY_END_ROW;
    for (IndexSearchCondition sc : conditions) {

      ExprNodeConstantEvaluator eval = new ExprNodeConstantEvaluator(sc.getConstantDesc());
      PrimitiveObjectInspector objInspector;
      Object writable;

      try {
        objInspector = (PrimitiveObjectInspector) eval.initialize(null);
        writable = eval.evaluate(null);
      } catch (ClassCastException cce) {
        throw new IOException("Currently only primitve types are supported. Found: "
            + sc.getConstantDesc().getTypeString());
      } catch (HiveException e) {
        throw new IOException(e);
      }

      byte[] constantVal = getConstantVal(writable, objInspector, isBinary);
      String comparisonOp = sc.getComparisonOp();

      if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual".equals(comparisonOp)) {
        startRow = constantVal;
        stopRow = getNextBA(constantVal);
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan".equals(comparisonOp)) {
        stopRow = constantVal;
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan"
          .equals(comparisonOp)) {
        startRow = constantVal;
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan"
          .equals(comparisonOp)) {
        startRow = getNextBA(constantVal);
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan"
          .equals(comparisonOp)) {
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

  static void setupTimeRange(Scan scan, List<IndexSearchCondition> conditions) throws IOException {
    long start = 0;
    long end = Long.MAX_VALUE;
    for (IndexSearchCondition sc : conditions) {
      long timestamp = getTimestampVal(sc);
      String comparisonOp = sc.getComparisonOp();
      if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual".equals(comparisonOp)) {
        start = timestamp;
        end = timestamp + 1;
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan".equals(comparisonOp)) {
        end = timestamp;
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan"
          .equals(comparisonOp)) {
        start = timestamp;
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan"
          .equals(comparisonOp)) {
        start = timestamp + 1;
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan"
          .equals(comparisonOp)) {
        end = timestamp + 1;
      } else {
        throw new IOException(comparisonOp + " is not a supported comparison operator");
      }
    }
    scan.setTimeRange(start, end);
  }

  static long getTimestampVal(IndexSearchCondition sc) throws IOException {
    long timestamp;
    try {
      ExprNodeConstantEvaluator eval = new ExprNodeConstantEvaluator(sc.getConstantDesc());
      ObjectInspector inspector = eval.initialize(null);
      Object value = eval.evaluate(null);
      if (inspector instanceof LongObjectInspector) {
        timestamp = ((LongObjectInspector) inspector).get(value);
      } else {
        PrimitiveObjectInspector primitive = (PrimitiveObjectInspector) inspector;
        timestamp = PrimitiveObjectInspectorUtils.getTimestamp(value, primitive).toEpochMilli();
      }
    } catch (HiveException e) {
      throw new IOException(e);
    }
    return timestamp;
  }

  static byte[] getConstantVal(Object writable, PrimitiveObjectInspector poi, boolean isKeyBinary)
      throws IOException {

    if (!isKeyBinary) {
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
      return Bytes.toBytes(((IntWritable) writable).get());
    case BOOLEAN:
      return Bytes.toBytes(((BooleanWritable) writable).get());
    case LONG:
      return Bytes.toBytes(((LongWritable) writable).get());
    case FLOAT:
      return Bytes.toBytes(((FloatWritable) writable).get());
    case DOUBLE:
      return Bytes.toBytes(((DoubleWritable) writable).get());
    case SHORT:
      return Bytes.toBytes(((ShortWritable) writable).get());
    case STRING:
      return Bytes.toBytes(((Text) writable).toString());
    case BYTE:
      return Bytes.toBytes(((ByteWritable) writable).get());

    default:
      throw new IOException("Type not supported " + pc);
    }
  }

  static byte[] getNextBA(byte[] current) {
    // startRow is inclusive while stopRow is exclusive,
    // this util method returns very next bytearray which will occur after the current one
    // by padding current one with a trailing 0 byte.
    byte[] next = new byte[current.length + 1];
    System.arraycopy(current, 0, next, 0, current.length);
    return next;
  }
}
