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
package org.apache.hadoop.hive.llap.io.api.impl;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.parquet.AbstractTestParquetDirect;
import org.apache.hadoop.hive.ql.io.parquet.VectorizedParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * That the bloom filters a vectorized Parquet read prunes by are served from the LLAP metadata cache.
 * The query results are the same whether they come from the cache or the file, so what shows the cache
 * was used is a read that is not allowed to touch the file: it can only answer from what was cached.
 */
public class TestLlapParquetBloomFilterCache extends AbstractTestParquetDirect {

  private static final String COLUMN_NAMES = "intCol";
  private static final String COLUMN_TYPES = "int";

  private JobConf conf;

  @BeforeClass
  public static void startLlapIo() throws Exception {
    HiveConf daemonConf = new HiveConf();
    // a real cache, sized for a test rather than for a daemon
    HiveConf.setVar(daemonConf, HiveConf.ConfVars.LLAP_IO_MEMORY_MAX_SIZE, "64Mb");
    HiveConf.setBoolVar(daemonConf, HiveConf.ConfVars.LLAP_ALLOCATOR_DIRECT, false);
    HiveConf.setBoolVar(daemonConf, HiveConf.ConfVars.LLAP_ALLOCATOR_PREALLOCATE, false);
    HiveConf.setIntVar(daemonConf, HiveConf.ConfVars.LLAP_ALLOCATOR_ARENA_COUNT, 1);
    HiveConf.setBoolVar(daemonConf, HiveConf.ConfVars.LLAP_TRACK_CACHE_USAGE, false);

    LlapProxy.setDaemon(true);
    LlapProxy.initializeLlapIo(daemonConf);
    Assert.assertTrue("these filters are held by the low level cache",
        LlapProxy.getIo().usingLowLevelCache());
  }

  @AfterClass
  public static void stopLlapIo() {
    LlapProxy.close();
  }

  @Before
  public void initConf() {
    conf = new JobConf();
  }

  @Test
  public void testTheFiltersPrunedByComeFromTheCache() throws Exception {
    StructObjectInspector inspector = objectInspector();
    Path first = writeBloomFilterFile("LlapBloomFirst");
    Path second = writeBloomFilterFile("LlapBloomSecond");

    // this read is here to fill the cache; the pruning itself is pinned by TestParquetRowGroupFilter
    Assert.assertEquals("the row group holds no odd value, so the bloom filter drops it",
        0, filteredBlocks(first, inspector, 51, new GenericUDFOPEqual()).size());

    // the second file is read by a predicate no bloom filter answers, so its footer is cached and
    // its filters are not: what the cache holds of the two files differs only in the filters
    Assert.assertEquals("a greater-than keeps the row group",
        1, filteredBlocks(second, inspector, 5, new GenericUDFOPGreaterThan()).size());

    // from here neither file may be read, so anything answered comes from the cache
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.LLAP_IO_CACHE_ONLY, true);

    Assert.assertEquals("the filter cached by the first read still drops the row group",
        0, filteredBlocks(first, inspector, 51, new GenericUDFOPEqual()).size());

    Assert.assertEquals("a value the row group holds keeps it, so the cached filter is the right one",
        1, filteredBlocks(first, inspector, 50, new GenericUDFOPEqual()).size());

    // proves the second file's footer is in the cache: a predicate needing no filter reads fine under
    // cache only, so the refusal below can only be about the filters
    Assert.assertEquals("the second file's footer is cached, so a greater-than answers from the cache",
        1, filteredBlocks(second, inspector, 5, new GenericUDFOPGreaterThan()).size());

    // the second file's footer is cached but its filters are not, so the filters cannot be had
    try {
      filteredBlocks(second, inspector, 51, new GenericUDFOPEqual());
      Assert.fail("a filter that was never cached must not be read from the file under cache only");
    } catch (RuntimeException e) {
      Assert.assertTrue("expected the cache only refusal, got: " + rootCause(e),
          rootCause(e).contains(HiveConf.ConfVars.LLAP_IO_CACHE_ONLY.varname));
    }
  }

  private static String rootCause(Throwable t) {
    Throwable cause = t;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    return String.valueOf(cause.getMessage());
  }

  /** One row group of even values, with a bloom filter over the column. */
  private Path writeBloomFilterFile(String name) throws Exception {
    MessageType fileSchema = MessageTypeParser.parseMessageType(
        "message hive_schema {\n  optional int32 intCol;\n}\n");
    return writeDirect(name, fileSchema,
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
  }

  private List<BlockMetaData> filteredBlocks(Path file, StructObjectInspector inspector, int value,
      GenericUDF comparison) throws Exception {
    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, COLUMN_NAMES);
    conf.set("columns", COLUMN_NAMES);
    conf.set("columns.types", COLUMN_TYPES);

    List<ExprNodeDesc> children = Lists.newArrayList(
        new ExprNodeColumnDesc(Integer.class, "intCol", "T", false), new ExprNodeConstantDesc(value));
    ExprNodeGenericFuncDesc predicate =
        new ExprNodeGenericFuncDesc(inspector, comparison, children);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, SerializationUtilities.serializeExpression(predicate));

    MapWork mapWork = new MapWork();
    VectorizedRowBatchCtx rbCtx = new VectorizedRowBatchCtx();
    rbCtx.init(inspector, new String[0]);
    mapWork.setVectorMode(true);
    mapWork.setVectorizedRowBatchCtx(rbCtx);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    HiveConf.setVar(conf, HiveConf.ConfVars.PLAN, "//tmp");
    Utilities.setMapWork(conf, mapWork);

    // the caches reach the reader the way they do in a daemon, through the input format
    VectorizedParquetInputFormat inputFormat = new VectorizedParquetInputFormat();
    LlapProxy.getIo().initCacheOnlyInputFormat(inputFormat);
    FileSplit split = new FileSplit(file, 0, fileLength(file), (String[]) null);
    try (VectorizedParquetRecordReader reader =
             (VectorizedParquetRecordReader) inputFormat.getRecordReader(split, conf, null)) {
      return reader.getFilteredBlocks();
    }
  }

  private ArrayWritableObjectInspector objectInspector() {
    StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(
        Arrays.asList(COLUMN_NAMES.split(",")),
        TypeInfoUtils.getTypeInfosFromTypeString(COLUMN_TYPES));
    return new ArrayWritableObjectInspector(rowTypeInfo);
  }

  /**
   * A file of two row groups, where a value held by one is inside the other's statistics: only the filters
   * tell them apart, so each row group has to be served the filter that belongs to it. The first read leaves
   * one of the two filters cached, so the second is served from a cache that holds part of what it asks for.
   */
  @Test
  public void testEachRowGroupIsServedItsOwnFilter() throws Exception {
    StructObjectInspector inspector = objectInspector();
    Path file = writeTwoRowGroups("LlapBloomTwoRowGroups");

    // a predicate no filter answers, so this reads the row groups the statistics keep: both of them
    List<BlockMetaData> both = filteredBlocks(file, inspector, -1, new GenericUDFOPGreaterThan());
    Assert.assertEquals("the file should hold two row groups for the rest of this to mean anything",
        2, both.size());
    long firstRowGroup = both.get(0).getStartingPos();
    long secondRowGroup = both.get(1).getStartingPos();

    // 0 is below everything the second row group holds, so the statistics leave one row group to ask
    // about and only that one's filter is read and cached
    Assert.assertEquals("the first row group holds 0",
        1, filteredBlocks(file, inspector, 0, new GenericUDFOPEqual()).size());

    // both row groups' statistics span 50, so both filters are wanted and only one of them is cached
    List<BlockMetaData> holdingFifty = filteredBlocks(file, inspector, 50, new GenericUDFOPEqual());
    Assert.assertEquals("50 is held by one row group", 1, holdingFifty.size());
    Assert.assertEquals("and it is the first, so it was served its own filter",
        firstRowGroup, holdingFifty.get(0).getStartingPos());

    List<BlockMetaData> holdingFiftyOne = filteredBlocks(file, inspector, 51, new GenericUDFOPEqual());
    Assert.assertEquals("51 is held by one row group", 1, holdingFiftyOne.size());
    Assert.assertEquals("and it is the second, so the two filters were not swapped",
        secondRowGroup, holdingFiftyOne.get(0).getStartingPos());

    // both filters are cached by now, so the same answers must come from the cache alone
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.LLAP_IO_CACHE_ONLY, true);
    Assert.assertEquals("the cached filter of the first row group still holds 50",
        firstRowGroup, filteredBlocks(file, inspector, 50, new GenericUDFOPEqual()).get(0).getStartingPos());
    Assert.assertEquals("the cached filter of the second row group still holds 51",
        secondRowGroup, filteredBlocks(file, inspector, 51, new GenericUDFOPEqual()).get(0).getStartingPos());
  }

  /**
   * Evens then odds, a row group each, so a value of either sits inside the other's statistics. Written a
   * record at a time because Parquet decides where a row group ends by counting the records it is handed.
   */
  private Path writeTwoRowGroups(String name) throws Exception {
    java.io.File temp = tempDir.newFile(name + ".parquet");
    temp.delete();
    Path path = new Path(temp.getPath());
    MessageType schema = MessageTypeParser.parseMessageType(
        "message hive_schema {\n  optional int32 intCol;\n}\n");
    try (ParquetWriter<Integer> writer = new IntBuilder(path, schema)
        .withBloomFilterEnabled("intCol", true)
        .withRowGroupSize(128L)
        .build()) {
      for (int i = 0; i < 200; i++) {
        writer.write(i < 100 ? i * 2 : (i - 100) * 2 + 1);
      }
    }
    return path;
  }

  private static final class IntBuilder extends ParquetWriter.Builder<Integer, IntBuilder> {
    private final MessageType schema;

    private IntBuilder(Path file, MessageType schema) {
      super(file);
      this.schema = schema;
    }

    @Override
    protected IntBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<Integer> getWriteSupport(Configuration conf) {
      return new WriteSupport<Integer>() {
        private RecordConsumer consumer;

        @Override
        public WriteContext init(Configuration configuration) {
          return new WriteContext(schema, new java.util.HashMap<>());
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
          this.consumer = recordConsumer;
        }

        @Override
        public void write(Integer value) {
          consumer.startMessage();
          consumer.startField("intCol", 0);
          consumer.addInteger(value);
          consumer.endField("intCol", 0);
          consumer.endMessage();
        }
      };
    }
  }
}
