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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.io.parquet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestParquetRowGroupFilter extends AbstractTestParquetDirect {

  JobConf conf;
  String columnNames;
  String columnTypes;
  private Path bloomTestPath;

  @Before
  public void initConf() throws Exception {
    conf = new JobConf();

  }

  @Test
  public void testRowGroupFilterTakeEffect() throws Exception {
    // define schema
    columnNames = "intCol";
    columnTypes = "int";
    StructObjectInspector inspector = getObjectInspector(columnNames, columnTypes);
    MessageType fileSchema = MessageTypeParser.parseMessageType(
        "message hive_schema {\n"
            + "  optional int32 intCol;\n"
            + "}\n"
    );

    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "intCol");
    conf.set("columns", "intCol");
    conf.set("columns.types", "int");

    // create Parquet file with specific data
    Path testPath = writeDirect("RowGroupFilterTakeEffect", fileSchema,
        new DirectWriter() {
          @Override
          public void write(RecordConsumer consumer) {
            for(int i = 0; i < 100; i++) {
              consumer.startMessage();
              consumer.startField("int", 0);
              consumer.addInteger(i);
              consumer.endField("int", 0);
              consumer.endMessage();
            }
          }
        });

    // > 50
    GenericUDF udf = new GenericUDFOPGreaterThan();
    List<ExprNodeDesc> children = Lists.newArrayList();
    ExprNodeColumnDesc columnDesc = new ExprNodeColumnDesc(Integer.class, "intCol", "T", false);
    ExprNodeConstantDesc constantDesc = new ExprNodeConstantDesc(50);
    children.add(columnDesc);
    children.add(constantDesc);
    ExprNodeGenericFuncDesc genericFuncDesc = new ExprNodeGenericFuncDesc(inspector, udf, children);
    String searchArgumentStr = SerializationUtilities.serializeExpression(genericFuncDesc);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, searchArgumentStr);

    ParquetRecordReaderWrapper recordReader = (ParquetRecordReaderWrapper)
        new MapredParquetInputFormat().getRecordReader(
        new FileSplit(testPath, 0, fileLength(testPath), (String[]) null), conf, null);

    Assert.assertEquals("row group is not filtered correctly", 1, recordReader.getFilteredBlocks().size());

    // > 100
    constantDesc = new ExprNodeConstantDesc(100);
    children.set(1, constantDesc);
    genericFuncDesc = new ExprNodeGenericFuncDesc(inspector, udf, children);
    searchArgumentStr = SerializationUtilities.serializeExpression(genericFuncDesc);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, searchArgumentStr);

    recordReader = (ParquetRecordReaderWrapper)
        new MapredParquetInputFormat().getRecordReader(
            new FileSplit(testPath, 0, fileLength(testPath), (String[]) null), conf, null);

    Assert.assertEquals("row group is not filtered correctly", 0, recordReader.getFilteredBlocks().size());
  }

  /**
   * A row group whose bloom filter proves the value is absent must be dropped even when the value sits
   * inside the column's min/max range, where statistics alone cannot prune anything.
   */
  @Test
  public void testBloomFilterRowGroupFilterTakeEffect() throws Exception {
    StructObjectInspector inspector = writeBloomFilterFile();

    // present value, and one that statistics cannot rule out because it lies inside [0, 198]
    Assert.assertEquals("row group with a value present in the bloom filter must be read",
        1, filteredBlocksForEquals(inspector, 50).size());
    Assert.assertEquals("row group must be dropped by the bloom filter",
        0, filteredBlocksForEquals(inspector, 51).size());

    // the same predicate keeps the row group once bloom filtering is switched off
    conf.setBoolean(ParquetInputFormat.BLOOM_FILTERING_ENABLED, false);
    Assert.assertEquals("row group must survive on statistics alone",
        1, filteredBlocksForEquals(inspector, 51).size());
  }

  /**
   * A bloom filter on a column the predicate never mentions cannot prune anything, so the row group must
   * survive on statistics and the data file must not be consulted for it.
   */
  @Test
  public void testBloomFilterOnUnrelatedColumnDoesNotPrune() throws Exception {
    columnNames = "intCol,otherCol";
    columnTypes = "int,int";
    StructObjectInspector inspector = getObjectInspector(columnNames, columnTypes);
    MessageType fileSchema = MessageTypeParser.parseMessageType(
        "message hive_schema {\n"
            + "  optional int32 intCol;\n"
            + "  optional int32 otherCol;\n"
            + "}\n"
    );

    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "intCol,otherCol");
    conf.set("columns", "intCol,otherCol");
    conf.set("columns.types", "int,int");

    // bloom filter only on intCol; otherCol holds the same even values
    bloomTestPath = writeDirect("BloomFilterOnUnrelatedColumn", fileSchema,
        consumer -> {
          for (int i = 0; i < 100; i++) {
            consumer.startMessage();
            consumer.startField("intCol", 0);
            consumer.addInteger(i * 2);
            consumer.endField("intCol", 0);
            consumer.startField("otherCol", 1);
            consumer.addInteger(i * 2);
            consumer.endField("otherCol", 1);
            consumer.endMessage();
          }
        },
        builder -> builder.withBloomFilterEnabled("intCol", true));

    List<ExprNodeDesc> children = Lists.newArrayList(
        new ExprNodeColumnDesc(Integer.class, "otherCol", "T", false), new ExprNodeConstantDesc(51));
    ExprNodeGenericFuncDesc predicate =
        new ExprNodeGenericFuncDesc(inspector, new GenericUDFOPEqual(), children);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, SerializationUtilities.serializeExpression(predicate));

    Assert.assertEquals("a bloom filter on another column must not prune this row group",
        1, filteredBlocksVectorized(inspector).size());
  }

  /** Writes a single-row-group file holding only even values 0..198, with a bloom filter on intCol. */
  private StructObjectInspector writeBloomFilterFile() throws Exception {
    columnNames = "intCol";
    columnTypes = "int";
    StructObjectInspector inspector = getObjectInspector(columnNames, columnTypes);
    MessageType fileSchema = MessageTypeParser.parseMessageType(
        "message hive_schema {\n"
            + "  optional int32 intCol;\n"
            + "}\n"
    );

    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "intCol");
    conf.set("columns", "intCol");
    conf.set("columns.types", "int");

    bloomTestPath = writeDirect("BloomFilterRowGroupFilterTakeEffect", fileSchema,
        consumer -> {
          for (int i = 0; i < 100; i++) {
            consumer.startMessage();
            consumer.startField("intCol", 0);
            consumer.addInteger(i * 2);
            consumer.endField("intCol", 0);
            consumer.endMessage();
          }
        },
        builder -> builder.withBloomFilterEnabled("intCol", true));
    return inspector;
  }

  /**
   * The JIRA is about the vectorized reader, which is the only one parquet did not already apply the bloom
   * filter level for, so assert the pruning on that reader and not just on the mapred one.
   */
  @Test
  public void testBloomFilterRowGroupFilterVectorized() throws Exception {
    StructObjectInspector inspector = writeBloomFilterFile();

    setEqualsPredicate(inspector, 50);
    Assert.assertEquals("row group with a value present in the bloom filter must be read",
        1, filteredBlocksVectorized(inspector).size());

    setEqualsPredicate(inspector, 51);
    Assert.assertEquals("vectorized reader must drop the row group on the bloom filter",
        0, filteredBlocksVectorized(inspector).size());
  }

  /**
   * A conjunction is dropped when either side drops it, so a range term alongside an equality term must not
   * stop the bloom filter from being consulted.
   */
  @Test
  public void testBloomFilterUsedForConjunctionContainingEquality() throws Exception {
    StructObjectInspector inspector = writeBloomFilterFile();

    List<ExprNodeDesc> equals = Lists.newArrayList(
        new ExprNodeColumnDesc(Integer.class, "intCol", "T", false), new ExprNodeConstantDesc(51));
    List<ExprNodeDesc> greater = Lists.newArrayList(
        new ExprNodeColumnDesc(Integer.class, "intCol", "T", false), new ExprNodeConstantDesc(0));
    ExprNodeGenericFuncDesc conjunction = new ExprNodeGenericFuncDesc(inspector, new GenericUDFOPAnd(),
        Lists.newArrayList(
            new ExprNodeGenericFuncDesc(inspector, new GenericUDFOPEqual(), equals),
            new ExprNodeGenericFuncDesc(inspector, new GenericUDFOPGreaterThan(), greater)));
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, SerializationUtilities.serializeExpression(conjunction));

    Assert.assertEquals("bloom filter must still prune when the equality sits inside a conjunction",
        0, filteredBlocksVectorized(inspector).size());
  }

  /**
   * IN reaches Parquet as a chain of ORed equalities, and a disjunction only drops a row group when every
   * branch drops it. This is the case bloom filters are most useful for, so pin it.
   */
  @Test
  public void testBloomFilterUsedForInList() throws Exception {
    StructObjectInspector inspector = writeBloomFilterFile();

    Assert.assertEquals("row group must be dropped when no value of the IN list is in the bloom filter",
        0, filteredBlocksForIn(inspector, 51, 53).size());
    Assert.assertEquals("row group must be read when one value of the IN list is present",
        1, filteredBlocksForIn(inspector, 50, 51).size());
  }

  private List<BlockMetaData> filteredBlocksForIn(StructObjectInspector inspector, int... values)
      throws Exception {
    List<ExprNodeDesc> disjuncts = Lists.newArrayList();
    for (int value : values) {
      disjuncts.add(new ExprNodeGenericFuncDesc(inspector, new GenericUDFOPEqual(), Lists.newArrayList(
          new ExprNodeColumnDesc(Integer.class, "intCol", "T", false), new ExprNodeConstantDesc(value))));
    }
    ExprNodeDesc predicate = disjuncts.get(0);
    for (int i = 1; i < disjuncts.size(); i++) {
      predicate = new ExprNodeGenericFuncDesc(inspector, new GenericUDFOPOr(),
          Lists.newArrayList(predicate, disjuncts.get(i)));
    }
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR,
        SerializationUtilities.serializeExpression((ExprNodeGenericFuncDesc) predicate));

    return filteredBlocksVectorized(inspector);
  }

  /**
   * A range-only predicate cannot be served by a bloom filter; the row group must survive on statistics.
   */
  @Test
  public void testRangeOnlyPredicateKeepsRowGroup() throws Exception {
    StructObjectInspector inspector = writeBloomFilterFile();

    List<ExprNodeDesc> children = Lists.newArrayList(
        new ExprNodeColumnDesc(Integer.class, "intCol", "T", false), new ExprNodeConstantDesc(50));
    ExprNodeGenericFuncDesc predicate =
        new ExprNodeGenericFuncDesc(inspector, new GenericUDFOPGreaterThan(), children);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, SerializationUtilities.serializeExpression(predicate));

    Assert.assertEquals("range predicate must be answered by statistics alone",
        1, filteredBlocksVectorized(inspector).size());
  }

  /**
   * Bloom filter pruning only runs on the vectorized reader: Parquet applies every filter level itself for
   * the mapred reader, whose context carries the pushed down predicate. So the bloom assertions have to go
   * through VectorizedParquetRecordReader.
   */
  private List<BlockMetaData> filteredBlocksVectorized(StructObjectInspector inspector) throws Exception {
    MapWork mapWork = new MapWork();
    VectorizedRowBatchCtx rbCtx = new VectorizedRowBatchCtx();
    rbCtx.init(inspector, new String[0]);
    mapWork.setVectorMode(true);
    mapWork.setVectorizedRowBatchCtx(rbCtx);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    HiveConf.setVar(conf, HiveConf.ConfVars.PLAN, "//tmp");
    Utilities.setMapWork(conf, mapWork);

    try (VectorizedParquetRecordReader reader = new VectorizedParquetRecordReader(
        new FileSplit(bloomTestPath, 0, fileLength(bloomTestPath), (String[]) null), new JobConf(conf))) {
      return reader.getFilteredBlocks();
    }
  }

  private void setEqualsPredicate(StructObjectInspector inspector, int value) {
    List<ExprNodeDesc> children = Lists.newArrayList(
        new ExprNodeColumnDesc(Integer.class, "intCol", "T", false), new ExprNodeConstantDesc(value));
    ExprNodeGenericFuncDesc predicate =
        new ExprNodeGenericFuncDesc(inspector, new GenericUDFOPEqual(), children);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, SerializationUtilities.serializeExpression(predicate));
  }

  private List<BlockMetaData> filteredBlocksForEquals(StructObjectInspector inspector, int value)
      throws Exception {
    List<ExprNodeDesc> children = Lists.newArrayList(
        new ExprNodeColumnDesc(Integer.class, "intCol", "T", false),
        new ExprNodeConstantDesc(value));
    ExprNodeGenericFuncDesc predicate =
        new ExprNodeGenericFuncDesc(inspector, new GenericUDFOPEqual(), children);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, SerializationUtilities.serializeExpression(predicate));

    return filteredBlocksVectorized(inspector);
  }

  private ArrayWritableObjectInspector getObjectInspector(final String columnNames, final String columnTypes) {
    List<TypeInfo> columnTypeList = createHiveTypeInfoFrom(columnTypes);
    List<String> columnNameList = createHiveColumnsFrom(columnNames);
    StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNameList, columnTypeList);

    return new ArrayWritableObjectInspector(rowTypeInfo);
  }

  private List<String> createHiveColumnsFrom(final String columnNamesStr) {
    List<String> columnNames;
    if (columnNamesStr.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNamesStr.split(","));
    }

    return columnNames;
  }

  private List<TypeInfo> createHiveTypeInfoFrom(final String columnsTypeStr) {
    List<TypeInfo> columnTypes;

    if (columnsTypeStr.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnsTypeStr);
    }

    return columnTypes;
  }
}
