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

package org.apache.hadoop.hive.ql.io.orc;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.filemeta.OrcFileMetadataHandler;
import org.apache.hadoop.hive.metastore.MetadataStore;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.orc.ExternalCache.ExternalFooterCachesByConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class TestOrcSplitElimination {

  public static class AllTypesRow {
    Long userid;
    Text string1;
    Double subtype;
    HiveDecimal decimal1;
    Timestamp ts;

    AllTypesRow(Long uid, String s1, Double d1, HiveDecimal decimal, Timestamp ts) {
      this.userid = uid;
      this.string1 = new Text(s1);
      this.subtype = d1;
      this.decimal1 = decimal;
      this.ts = ts;
    }
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  JobConf conf;
  FileSystem fs;
  Path testFilePath, testFilePath2;

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem() throws Exception {
    conf = new JobConf();
    // all columns
    conf.set("columns", "userid,string1,subtype,decimal1,ts");
    conf.set("columns.types", "bigint,string,double,decimal,timestamp");
    // needed columns
    conf.set(ColumnProjectionUtils.READ_ALL_COLUMNS, "false");
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0,2");
    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "userid,subtype");
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile." +
        testCaseName.getMethodName() + ".orc");
    testFilePath2 = new Path(workDir, "TestOrcFile." +
    testCaseName.getMethodName() + ".2.orc");
    fs.delete(testFilePath, false);
    fs.delete(testFilePath2, false);
  }

  @Test
  public void testSplitEliminationSmallMaxSplit() throws Exception {
    ObjectInspector inspector = createIO();
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        100000, CompressionKind.NONE, 10000, 10000);
    writeData(writer);
    writer.close();
    HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMINSPLITSIZE, 1000);
    HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMAXSPLITSIZE, 5000);
    InputFormat<?, ?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());

    GenericUDF udf = new GenericUDFOPEqualOrLessThan();
    List<ExprNodeDesc> childExpr = Lists.newArrayList();
    ExprNodeConstantDesc con;
    ExprNodeGenericFuncDesc en;
    String sargStr;
    createTestSarg(inspector, udf, childExpr);
    InputSplit[] splits = in.getSplits(conf, 1);
    assertEquals(5, splits.length);

    con = new ExprNodeConstantDesc(1);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = SerializationUtilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    assertEquals(0, splits.length);

    con = new ExprNodeConstantDesc(2);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = SerializationUtilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    assertEquals(1, splits.length);

    con = new ExprNodeConstantDesc(5);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = SerializationUtilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    assertEquals(2, splits.length);

    con = new ExprNodeConstantDesc(13);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = SerializationUtilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    assertEquals(3, splits.length);

    con = new ExprNodeConstantDesc(29);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = SerializationUtilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    assertEquals(4, splits.length);

    con = new ExprNodeConstantDesc(70);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = SerializationUtilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    assertEquals(5, splits.length);
  }

  @Test
  public void testSplitEliminationLargeMaxSplit() throws Exception {
    ObjectInspector inspector = createIO();
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        100000, CompressionKind.NONE, 10000, 10000);
    writeData(writer);
    writer.close();
    HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMINSPLITSIZE, 1000);
    HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMAXSPLITSIZE, 150000);
    InputFormat<?, ?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());

    GenericUDF udf = new GenericUDFOPEqualOrLessThan();
    List<ExprNodeDesc> childExpr = Lists.newArrayList();
    ExprNodeConstantDesc con;
    ExprNodeGenericFuncDesc en;
    String sargStr;
    createTestSarg(inspector, udf, childExpr);
    InputSplit[] splits = in.getSplits(conf, 1);
    assertEquals(2, splits.length);

    con = new ExprNodeConstantDesc(0);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = SerializationUtilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // no stripes satisfies the condition
    assertEquals(0, splits.length);

    con = new ExprNodeConstantDesc(2);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = SerializationUtilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // only first stripe will satisfy condition and hence single split
    assertEquals(1, splits.length);

    con = new ExprNodeConstantDesc(5);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = SerializationUtilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // first stripe will satisfy the predicate and will be a single split, last stripe will be a
    // separate split
    assertEquals(2, splits.length);

    con = new ExprNodeConstantDesc(13);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = SerializationUtilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // first 2 stripes will satisfy the predicate and merged to single split, last stripe will be a
    // separate split
    assertEquals(2, splits.length);

    con = new ExprNodeConstantDesc(29);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = SerializationUtilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // first 3 stripes will satisfy the predicate and merged to single split, last stripe will be a
    // separate split
    assertEquals(2, splits.length);

    con = new ExprNodeConstantDesc(70);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
    sargStr = SerializationUtilities.serializeExpression(en);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // first 2 stripes will satisfy the predicate and merged to single split, last two stripe will
    // be a separate split
    assertEquals(2, splits.length);
  }


  @Test
  public void testSplitEliminationComplexExpr() throws Exception {
    ObjectInspector inspector = createIO();
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        100000, CompressionKind.NONE, 10000, 10000);
    writeData(writer);
    writer.close();
    HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMINSPLITSIZE, 1000);
    HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMAXSPLITSIZE, 150000);
    InputFormat<?, ?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());

    // predicate expression: userid <= 100 and subtype <= 1000.0
    GenericUDF udf = new GenericUDFOPEqualOrLessThan();
    List<ExprNodeDesc> childExpr = Lists.newArrayList();
    ExprNodeColumnDesc col = new ExprNodeColumnDesc(Long.class, "userid", "T", false);
    ExprNodeConstantDesc con = new ExprNodeConstantDesc(100);
    childExpr.add(col);
    childExpr.add(con);
    ExprNodeGenericFuncDesc en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);

    GenericUDF udf1 = new GenericUDFOPEqualOrLessThan();
    List<ExprNodeDesc> childExpr1 = Lists.newArrayList();
    ExprNodeColumnDesc col1 = new ExprNodeColumnDesc(Double.class, "subtype", "T", false);
    ExprNodeConstantDesc con1 = new ExprNodeConstantDesc(1000.0);
    childExpr1.add(col1);
    childExpr1.add(con1);
    ExprNodeGenericFuncDesc en1 = new ExprNodeGenericFuncDesc(inspector, udf1, childExpr1);

    GenericUDF udf2 = new GenericUDFOPAnd();
    List<ExprNodeDesc> childExpr2 = Lists.newArrayList();
    childExpr2.add(en);
    childExpr2.add(en1);
    ExprNodeGenericFuncDesc en2 = new ExprNodeGenericFuncDesc(inspector, udf2, childExpr2);

    String sargStr = SerializationUtilities.serializeExpression(en2);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    InputSplit[] splits = in.getSplits(conf, 1);
    assertEquals(2, splits.length);

    con = new ExprNodeConstantDesc(2);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);

    con1 = new ExprNodeConstantDesc(0.0);
    childExpr1.set(1, con1);
    en1 = new ExprNodeGenericFuncDesc(inspector, udf1, childExpr1);

    childExpr2.set(0, en);
    childExpr2.set(1, en1);
    en2 = new ExprNodeGenericFuncDesc(inspector, udf2, childExpr2);

    sargStr = SerializationUtilities.serializeExpression(en2);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // no stripe will satisfy the predicate
    assertEquals(0, splits.length);

    con = new ExprNodeConstantDesc(2);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);

    con1 = new ExprNodeConstantDesc(1.0);
    childExpr1.set(1, con1);
    en1 = new ExprNodeGenericFuncDesc(inspector, udf1, childExpr1);

    childExpr2.set(0, en);
    childExpr2.set(1, en1);
    en2 = new ExprNodeGenericFuncDesc(inspector, udf2, childExpr2);

    sargStr = SerializationUtilities.serializeExpression(en2);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // only first stripe will satisfy condition and hence single split
    assertEquals(1, splits.length);

    udf = new GenericUDFOPEqual();
    con = new ExprNodeConstantDesc(13);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);

    con1 = new ExprNodeConstantDesc(80.0);
    childExpr1.set(1, con1);
    en1 = new ExprNodeGenericFuncDesc(inspector, udf1, childExpr1);

    childExpr2.set(0, en);
    childExpr2.set(1, en1);
    en2 = new ExprNodeGenericFuncDesc(inspector, udf2, childExpr2);

    sargStr = SerializationUtilities.serializeExpression(en2);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // first two stripes will satisfy condition and hence single split
    assertEquals(2, splits.length);

    udf = new GenericUDFOPEqual();
    con = new ExprNodeConstantDesc(13);
    childExpr.set(1, con);
    en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);

    udf1 = new GenericUDFOPEqual();
    con1 = new ExprNodeConstantDesc(80.0);
    childExpr1.set(1, con1);
    en1 = new ExprNodeGenericFuncDesc(inspector, udf1, childExpr1);

    childExpr2.set(0, en);
    childExpr2.set(1, en1);
    en2 = new ExprNodeGenericFuncDesc(inspector, udf2, childExpr2);

    sargStr = SerializationUtilities.serializeExpression(en2);
    conf.set("hive.io.filter.expr.serialized", sargStr);
    splits = in.getSplits(conf, 1);
    // only second stripes will satisfy condition and hence single split
    assertEquals(1, splits.length);
  }

  private static class OrcInputFormatForTest extends OrcInputFormat {
    public static void clearLocalCache() {
      OrcInputFormat.Context.clearLocalCache();
    }
    static MockExternalCaches caches = new MockExternalCaches();
    @Override
    protected ExternalFooterCachesByConf createExternalCaches() {
      return caches;
    }
  }

  private static class MockExternalCaches
    implements ExternalFooterCachesByConf, ExternalFooterCachesByConf.Cache, MetadataStore {
    private static class MockItem {
      ByteBuffer data;
      ByteBuffer[] extraCols;
      ByteBuffer[] extraData;

      @Override
      public String toString() {
        return (data == null ? 0 : data.remaining()) + " bytes"
            + (extraCols == null ? "" : ("; " + extraCols.length + " extras"));
      }
    }
    private final Map<Long, MockItem> cache = new ConcurrentHashMap<>();
    private final OrcFileMetadataHandler handler = new OrcFileMetadataHandler();
    private final AtomicInteger putCount = new AtomicInteger(0),
        getCount = new AtomicInteger(0), getHitCount = new AtomicInteger(0),
        getByExprCount = new AtomicInteger(0), getHitByExprCount = new AtomicInteger();

    public void resetCounts() {
      getByExprCount.set(0);
      getCount.set(0);
      putCount.set(0);
      getHitCount.set(0);
      getHitByExprCount.set(0);
    }

    @Override
    public Cache getCache(HiveConf conf) throws IOException {
      handler.configure(conf, new PartitionExpressionForMetastore(), this);
      return this;
    }

    @Override
    public Iterator<Entry<Long, MetadataPpdResult>> getFileMetadataByExpr(
        List<Long> fileIds, ByteBuffer sarg, boolean doGetFooters) throws HiveException {
      getByExprCount.incrementAndGet();
      ByteBuffer[] metadatas = new ByteBuffer[fileIds.size()];
      ByteBuffer[] ppdResults = new ByteBuffer[fileIds.size()];
      boolean[] eliminated = new boolean[fileIds.size()];
      try {
        byte[] bb = new byte[sarg.remaining()];
        System.arraycopy(sarg.array(), sarg.arrayOffset(), bb, 0, sarg.remaining());
        handler.getFileMetadataByExpr(fileIds, bb, metadatas, ppdResults, eliminated);
      } catch (IOException e) {
        throw new HiveException(e);
      }
      Map<Long, MetadataPpdResult> result = new HashMap<>();
      for (int i = 0; i < metadatas.length; ++i) {
        long fileId = fileIds.get(i);
        ByteBuffer metadata = metadatas[i];
        if (metadata == null) continue;
        getHitByExprCount.incrementAndGet();
        metadata = eliminated[i] ? null : metadata;
        MetadataPpdResult mpr = new MetadataPpdResult();
        ByteBuffer bitset = eliminated[i] ? null : ppdResults[i];
        mpr.setMetadata(doGetFooters ? metadata : null);
        mpr.setIncludeBitset(bitset);
        result.put(fileId, mpr);
      }
      return result.entrySet().iterator();
    }

    @Override
    public void clearFileMetadata(List<Long> fileIds) throws HiveException {
      for (Long id : fileIds) {
        cache.remove(id);
      }
    }

    @Override
    public Iterator<Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds)
        throws HiveException {
      getCount.incrementAndGet();
      HashMap<Long, ByteBuffer> result = new HashMap<>();
      for (Long id : fileIds) {
        MockItem mi = cache.get(id);
        if (mi == null) continue;
        getHitCount.incrementAndGet();
        result.put(id, mi.data);
      }
      return result.entrySet().iterator();
    }

    @Override
    public void putFileMetadata(ArrayList<Long> fileIds,
        ArrayList<ByteBuffer> values) throws HiveException {
      putCount.incrementAndGet();
      ByteBuffer[] addedCols = handler.createAddedCols();
      ByteBuffer[][] addedVals = null;
      if (addedCols != null) {
        addedVals = handler.createAddedColVals(values);
      }
      try {
        storeFileMetadata(fileIds, values, addedCols, addedVals);
      } catch (IOException | InterruptedException e) {
        throw new HiveException(e);
      }
    }

    // MetadataStore
    @Override
    public void getFileMetadata(List<Long> fileIds, ByteBuffer[] result) throws IOException {
      for (int i = 0; i < fileIds.size(); ++i) {
        MockItem mi = cache.get(fileIds.get(i));
        result[i] = (mi == null ? null : mi.data);
      }
    }

    @Override
    public void storeFileMetadata(List<Long> fileIds, List<ByteBuffer> metadataBuffers,
        ByteBuffer[] addedCols, ByteBuffer[][] addedVals)
            throws IOException, InterruptedException {
      for (int i = 0; i < fileIds.size(); ++i) {
        ByteBuffer value = (metadataBuffers != null) ? metadataBuffers.get(i) : null;
        ByteBuffer[] av = addedVals == null ? null : addedVals[i];
        storeFileMetadata(fileIds.get(i), value, addedCols, av);
      }
    }

    @Override
    public void storeFileMetadata(long fileId, ByteBuffer metadata,
        ByteBuffer[] addedCols, ByteBuffer[] addedVals) throws IOException, InterruptedException {
      if (metadata == null) {
        cache.remove(metadata);
        return;
      }
      MockItem mi = new MockItem();
      mi.data = metadata;
      if (addedVals != null) {
        mi.extraCols = addedCols;
        mi.extraData = addedVals;
      }
      cache.put(fileId, mi);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(TestOrcSplitElimination.class);

  @Ignore("External cache has been turned off for now")
  @Test
  public void testExternalFooterCache() throws Exception {
    testFooterExternalCacheImpl(false);
  }

  @Ignore("External cache has been turned off for now")
  @Test
  public void testExternalFooterCachePpd() throws Exception {
    testFooterExternalCacheImpl(true);
  }

  private final static class FsWithHash {
    private FileSplit fs;
    public FsWithHash(FileSplit fs) {
      this.fs = fs;
    }
    @Override
    public int hashCode() {
      if (fs == null) return 0;
      final int prime = 31;
      int result = prime * 1 + fs.getPath().hashCode();
      result = prime * result + Long.valueOf(fs.getStart()).hashCode();
      return prime * result + Long.valueOf(fs.getLength()).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof FsWithHash)) return false;
      FsWithHash other = (FsWithHash)obj;
      if ((fs == null) != (other.fs == null)) return false;
      if (fs == null && other.fs == null) return true;
      return fs.getStart() == other.fs.getStart() && fs.getLength() == other.fs.getLength()
          && fs.getPath().equals(other.fs.getPath());
    }
  }

  private void testFooterExternalCacheImpl(boolean isPpd) throws IOException {
    ObjectInspector inspector = createIO();
    writeFile(inspector, testFilePath);
    writeFile(inspector, testFilePath2);

    GenericUDF udf = new GenericUDFOPEqualOrLessThan();
    List<ExprNodeDesc> childExpr = Lists.newArrayList();
    createTestSarg(inspector, udf, childExpr);
    setupExternalCacheConfig(isPpd, testFilePath + "," + testFilePath2);
    // Get the base values w/o cache.
    conf.setBoolean(ConfVars.HIVE_ORC_MS_FOOTER_CACHE_ENABLED.varname, false);
    OrcInputFormatForTest.clearLocalCache();
    OrcInputFormat in0 = new OrcInputFormat();
    InputSplit[] originals = in0.getSplits(conf, -1);
    assertEquals(10, originals.length);
    HashSet<FsWithHash> originalHs = new HashSet<>();
    for (InputSplit original : originals) {
      originalHs.add(new FsWithHash((FileSplit)original));
    }

    // Populate the cache.
    conf.setBoolean(ConfVars.HIVE_ORC_MS_FOOTER_CACHE_ENABLED.varname, true);
    OrcInputFormatForTest in = new OrcInputFormatForTest();
    OrcInputFormatForTest.clearLocalCache();
    OrcInputFormatForTest.caches.resetCounts();
    OrcInputFormatForTest.caches.cache.clear();
    InputSplit[] splits = in.getSplits(conf, -1);
    // Puts, gets, hits, unused, unused.
    @SuppressWarnings("static-access")
    AtomicInteger[] counts = { in.caches.putCount,
        isPpd ? in.caches.getByExprCount : in.caches.getCount,
        isPpd ? in.caches.getHitByExprCount : in.caches.getHitCount,
        isPpd ? in.caches.getCount : in.caches.getByExprCount,
        isPpd ? in.caches.getHitCount : in.caches.getHitByExprCount };

    verifySplits(originalHs, splits);
    verifyCallCounts(counts, 2, 2, 0);
    assertEquals(2, OrcInputFormatForTest.caches.cache.size());

    // Verify we can get from cache.
    OrcInputFormatForTest.clearLocalCache();
    OrcInputFormatForTest.caches.resetCounts();
    splits = in.getSplits(conf, -1);
    verifySplits(originalHs, splits);
    verifyCallCounts(counts, 0, 2, 2);

    // Verify ORC SARG still works.
    OrcInputFormatForTest.clearLocalCache();
    OrcInputFormatForTest.caches.resetCounts();
    childExpr.set(1, new ExprNodeConstantDesc(5));
    conf.set("hive.io.filter.expr.serialized", SerializationUtilities.serializeExpression(
        new ExprNodeGenericFuncDesc(inspector, udf, childExpr)));
    splits = in.getSplits(conf, -1);
    InputSplit[] filtered = { originals[0], originals[4], originals[5], originals[9] };
    originalHs = new HashSet<>();
    for (InputSplit original : filtered) {
      originalHs.add(new FsWithHash((FileSplit)original));
    }
    verifySplits(originalHs, splits);
    verifyCallCounts(counts, 0, 2, 2);

    // Verify corrupted cache value gets replaced.
    OrcInputFormatForTest.clearLocalCache();
    OrcInputFormatForTest.caches.resetCounts();
    Map.Entry<Long, MockExternalCaches.MockItem> e =
        OrcInputFormatForTest.caches.cache.entrySet().iterator().next();
    Long key = e.getKey();
    byte[] someData = new byte[8];
    ByteBuffer toCorrupt = e.getValue().data;
    System.arraycopy(toCorrupt.array(), toCorrupt.arrayOffset(), someData, 0, someData.length);
    toCorrupt.putLong(0, 0L);
    splits = in.getSplits(conf, -1);
    verifySplits(originalHs, splits);
    if (!isPpd) { // Recovery is not implemented yet for PPD path.
      ByteBuffer restored = OrcInputFormatForTest.caches.cache.get(key).data;
      byte[] newData = new byte[someData.length];
      System.arraycopy(restored.array(), restored.arrayOffset(), newData, 0, newData.length);
      assertArrayEquals(someData, newData);
    }
  }

  private void verifyCallCounts(AtomicInteger[] counts, int puts, int gets, int hits) {
    assertEquals("puts", puts, counts[0].get());
    assertEquals("gets", gets, counts[1].get());
    assertEquals("hits", hits, counts[2].get());
    assertEquals("unused1", 0, counts[3].get());
    assertEquals("unused2", 0, counts[4].get());
  }

  private void verifySplits(HashSet<FsWithHash> originalHs, InputSplit[] splits) {
    if (originalHs.size() != splits.length) {
      String s = "Expected [";
      for (FsWithHash fwh : originalHs) {
        s += toString(fwh.fs) + ", ";
      }
      s += "], actual [";
      for (InputSplit fs : splits) {
        s += toString((FileSplit)fs) + ", ";
      }
      fail(s + "]");
    }
    for (int i = 0; i < splits.length; ++i) {
      FileSplit fs = (FileSplit)splits[i];
      if (!originalHs.contains(new FsWithHash((FileSplit)splits[i]))) {
        String s = " in [";
        for (FsWithHash fwh : originalHs) {
          s += toString(fwh.fs) + ", ";
        }
        fail("Cannot find " + toString(fs) + s);
      }
    }

  }

  private static String toString(FileSplit fs) {
    return "{" + fs.getPath() + ", " + fs.getStart() + ", "  + fs.getLength() + "}";
  }

  private void setupExternalCacheConfig(boolean isPpd, String paths) {
    FileInputFormat.setInputPaths(conf, paths);
    conf.set(ConfVars.HIVE_ORC_SPLIT_STRATEGY.varname, "ETL");
    conf.setLong(HiveConf.ConfVars.MAPREDMINSPLITSIZE.varname, 1000);
    conf.setLong(HiveConf.ConfVars.MAPREDMAXSPLITSIZE.varname, 5000);
    conf.setBoolean(ConfVars.HIVE_ORC_MS_FOOTER_CACHE_PPD.varname, isPpd);
    conf.setBoolean(ConfVars.HIVEOPTINDEXFILTER.varname, isPpd);
  }

  private ObjectInspector createIO() {
    synchronized (TestOrcFile.class) {
      return ObjectInspectorFactory
          .getReflectionObjectInspector(AllTypesRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
  }

  private void writeFile(ObjectInspector inspector, Path filePath) throws IOException {
    Writer writer = OrcFile.createWriter(
        fs, filePath, conf, inspector, 100000, CompressionKind.NONE, 10000, 10000);
    writeData(writer);
    writer.close();
  }

  private void createTestSarg(
      ObjectInspector inspector, GenericUDF udf, List<ExprNodeDesc> childExpr) {
    childExpr.add(new ExprNodeColumnDesc(Long.class, "userid", "T", false));
    childExpr.add(new ExprNodeConstantDesc(100));
    conf.set("hive.io.filter.expr.serialized", SerializationUtilities.serializeExpression(
        new ExprNodeGenericFuncDesc(inspector, udf, childExpr)));
  }

  private void writeData(Writer writer) throws IOException {
    for (int i = 0; i < 25000; i++) {
      if (i == 0) {
        writer.addRow(new AllTypesRow(2L, "foo", 0.8, HiveDecimal.create("1.2"), new Timestamp(0)));
      } else if (i == 5000) {
        writer.addRow(new AllTypesRow(13L, "bar", 80.0, HiveDecimal.create("2.2"), new Timestamp(
            5000)));
      } else if (i == 10000) {
        writer.addRow(new AllTypesRow(29L, "cat", 8.0, HiveDecimal.create("3.3"), new Timestamp(
            10000)));
      } else if (i == 15000) {
        writer.addRow(new AllTypesRow(70L, "dog", 1.8, HiveDecimal.create("4.4"), new Timestamp(
            15000)));
      } else if (i == 20000) {
        writer.addRow(new AllTypesRow(5L, "eat", 0.8, HiveDecimal.create("5.5"), new Timestamp(
            20000)));
      } else {
        writer.addRow(new AllTypesRow(100L, "zebra", 8.0, HiveDecimal.create("0.0"), new Timestamp(
            250000)));
      }
    }
  }
}
