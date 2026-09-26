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
package org.apache.hadoop.hive.llap.cache;

import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LlapHiveUtils;
import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;
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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * That a read lets go of the bloom filter buffers it locks in the metadata cache. A buffer left locked is
 * never evicted, so a leak fills a daemon's cache with entries nothing can reclaim - and because a locked
 * buffer is still readable, every other test of this feature would keep passing.
 *
 * <p>This lives beside the cache rather than beside the reader because the reference count it asserts on is
 * visible only within this package.
 */
public class TestParquetBloomFilterBufferRelease extends AbstractTestParquetDirect {

  @BeforeClass
  public static void startLlapIo() throws Exception {
    HiveConf daemonConf = new HiveConf();
    HiveConf.setVar(daemonConf, HiveConf.ConfVars.LLAP_IO_MEMORY_MAX_SIZE, "64Mb");
    HiveConf.setBoolVar(daemonConf, HiveConf.ConfVars.LLAP_ALLOCATOR_DIRECT, false);
    HiveConf.setBoolVar(daemonConf, HiveConf.ConfVars.LLAP_ALLOCATOR_PREALLOCATE, false);
    HiveConf.setIntVar(daemonConf, HiveConf.ConfVars.LLAP_ALLOCATOR_ARENA_COUNT, 1);
    HiveConf.setBoolVar(daemonConf, HiveConf.ConfVars.LLAP_TRACK_CACHE_USAGE, false);
    LlapProxy.setDaemon(true);
    LlapProxy.initializeLlapIo(daemonConf);
    Assert.assertTrue(LlapProxy.getIo().usingLowLevelCache());
  }

  @AfterClass
  public static void stopLlapIo() {
    LlapProxy.close();
  }

  @Test
  public void testAReadLetsGoOfTheFiltersItLocks() throws Exception {
    JobConf conf = new JobConf();
    Path file = writeBloomFilterFile();

    // reading with an equality predicate caches the filter it prunes by, and should leave it unlocked
    Assert.assertEquals("the row group holds no odd value, so the bloom filter drops it",
        0, filteredBlocks(file, conf, 51).size());

    long offset = bloomOffsetOf(file, conf);
    SortedMap<Long, Integer> ranges = new TreeMap<>();
    ranges.put(offset, bloomLengthOf(file, conf));
    Map<Long, MemoryBufferOrBuffers> served = LlapProxy.getIo()
        .getParquetBloomFilterBuffersFromCache(file, conf, fileKeyOf(file, conf), ranges);
    Assert.assertNotNull("the read should have cached the filter it used", served);

    // this fetch is the only lock standing: a read that kept its own would make it two
    LlapAllocatorBuffer buffer = (LlapAllocatorBuffer) served.get(offset).getSingleBuffer();
    Assert.assertEquals("the read let go of the filter it locked", 1, buffer.getRefCount());
    buffer.decRef();
  }

  private static Object fileKeyOf(Path file, JobConf conf) throws Exception {
    return LlapHiveUtils.createFileIdUsingFS(file.getFileSystem(conf), file, conf);
  }

  private static long bloomOffsetOf(Path file, JobConf conf) throws Exception {
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(file, conf))) {
      return reader.getFooter().getBlocks().get(0).getColumns().get(0).getBloomFilterOffset();
    }
  }

  private static int bloomLengthOf(Path file, JobConf conf) throws Exception {
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(file, conf))) {
      return reader.getFooter().getBlocks().get(0).getColumns().get(0).getBloomFilterLength();
    }
  }

  private Path writeBloomFilterFile() throws Exception {
    MessageType fileSchema = MessageTypeParser.parseMessageType(
        "message hive_schema {\n  optional int32 intCol;\n}\n");
    return writeDirect("BloomFilterBufferRelease", fileSchema,
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

  private List<org.apache.parquet.hadoop.metadata.BlockMetaData> filteredBlocks(
      Path file, JobConf conf, int value) throws Exception {
    StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(
        Arrays.asList("intCol"), TypeInfoUtils.getTypeInfosFromTypeString("int"));
    StructObjectInspector inspector = new ArrayWritableObjectInspector(rowTypeInfo);

    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "intCol");
    conf.set("columns", "intCol");
    conf.set("columns.types", "int");

    List<ExprNodeDesc> children = Lists.newArrayList(
        new ExprNodeColumnDesc(Integer.class, "intCol", "T", false), new ExprNodeConstantDesc(value));
    ExprNodeGenericFuncDesc predicate =
        new ExprNodeGenericFuncDesc(inspector, new GenericUDFOPEqual(), children);
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, SerializationUtilities.serializeExpression(predicate));

    MapWork mapWork = new MapWork();
    VectorizedRowBatchCtx rbCtx = new VectorizedRowBatchCtx();
    rbCtx.init(inspector, new String[0]);
    mapWork.setVectorMode(true);
    mapWork.setVectorizedRowBatchCtx(rbCtx);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    HiveConf.setVar(conf, HiveConf.ConfVars.PLAN, "//tmp");
    Utilities.setMapWork(conf, mapWork);

    VectorizedParquetInputFormat inputFormat = new VectorizedParquetInputFormat();
    LlapProxy.getIo().initCacheOnlyInputFormat(inputFormat);
    FileSplit split = new FileSplit(file, 0, fileLength(file), (String[]) null);
    try (VectorizedParquetRecordReader reader =
             (VectorizedParquetRecordReader) inputFormat.getRecordReader(split, conf, null)) {
      return reader.getFilteredBlocks();
    }
  }
}
