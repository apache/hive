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
package org.apache.hadoop.hive.llap.cache;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashSet;

import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.orc.DataReader;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.llap.IncrementalObjectSizeEstimator;
import org.apache.hadoop.hive.llap.IncrementalObjectSizeEstimator.ObjectEstimator;
import org.apache.hadoop.hive.llap.io.metadata.OrcFileMetadata;
import org.apache.hadoop.hive.llap.io.metadata.OrcStripeMetadata;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.StripeInformation;
import org.apache.hadoop.hive.ql.io.orc.encoded.OrcBatchKey;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.orc.OrcProto;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.protobuf.CodedOutputStream;

public class TestIncrementalObjectSizeEstimator {
  private static final Logger LOG = LoggerFactory.getLogger(TestIncrementalObjectSizeEstimator.class);

  private static class DummyMetadataReader implements DataReader {
    public boolean doStreamStep = false;
    public boolean isEmpty;

    @Override
    public void open() throws IOException {

    }

    @Override
    public OrcIndex readRowIndex(StripeInformation stripe,
                                 TypeDescription fileSchema,
                                 OrcProto.StripeFooter footer,
                                 boolean ignoreNonUtf8BloomFilter,
                                 boolean[] included,
                                 OrcProto.RowIndex[] indexes,
                                 boolean[] sargColumns,
                                 OrcFile.WriterVersion version,
                                 OrcProto.Stream.Kind[] bloomFilterKinds,
                                 OrcProto.BloomFilterIndex[] bloomFilterIndices
                                 ) throws IOException {
      if (isEmpty) {
        return new OrcIndex(new OrcProto.RowIndex[] { },
            bloomFilterKinds,
            new OrcProto.BloomFilterIndex[] { });
      }
      OrcProto.ColumnStatistics cs = OrcProto.ColumnStatistics.newBuilder()
          .setBucketStatistics(OrcProto.BucketStatistics.newBuilder().addCount(0))
          .setStringStatistics(OrcProto.StringStatistics.newBuilder().setMaximum("zzz").setMinimum("aaa"))
          .setBinaryStatistics(OrcProto.BinaryStatistics.newBuilder().setSum(5))
          .setDateStatistics(OrcProto.DateStatistics.newBuilder().setMinimum(4545).setMaximum(6656))
          .setDecimalStatistics(OrcProto.DecimalStatistics.newBuilder().setMaximum("zzz").setMinimum("aaa"))
          .setDoubleStatistics(OrcProto.DoubleStatistics.newBuilder().setMinimum(0.5).setMaximum(1.5))
          .setIntStatistics(OrcProto.IntegerStatistics.newBuilder().setMaximum(10).setMinimum(5))
          .setTimestampStatistics(OrcProto.TimestampStatistics.newBuilder().setMaximum(10)).build();
      OrcProto.RowIndex ri = OrcProto.RowIndex.newBuilder()
          .addEntry(OrcProto.RowIndexEntry.newBuilder().addPositions(1))
          .addEntry(OrcProto.RowIndexEntry.newBuilder().addPositions(0).addPositions(2).setStatistics(cs))
          .build();
      OrcProto.RowIndex ri2 = OrcProto.RowIndex.newBuilder()
          .addEntry(OrcProto.RowIndexEntry.newBuilder().addPositions(3))
          .build();
      OrcProto.BloomFilterIndex bfi = OrcProto.BloomFilterIndex.newBuilder().addBloomFilter(
          OrcProto.BloomFilter.newBuilder().addBitset(0).addBitset(1)).build();
      if (doStreamStep) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream cos = CodedOutputStream.newInstance(baos);
        ri.writeTo(cos);
        cos.flush();
        ri = OrcProto.RowIndex.newBuilder().mergeFrom(baos.toByteArray()).build();
        baos = new ByteArrayOutputStream();
        cos = CodedOutputStream.newInstance(baos);
        ri2.writeTo(cos);
        cos.flush();
        ri2 = OrcProto.RowIndex.newBuilder().mergeFrom(baos.toByteArray()).build();
        baos = new ByteArrayOutputStream();
        cos = CodedOutputStream.newInstance(baos);
        bfi.writeTo(cos);
        cos.flush();
        bfi = OrcProto.BloomFilterIndex.newBuilder().mergeFrom(baos.toByteArray()).build();
      }
      return new OrcIndex(
          new OrcProto.RowIndex[] { ri, ri2 },
          bloomFilterKinds,
          new OrcProto.BloomFilterIndex[] { bfi });
    }

    @Override
    public OrcProto.StripeFooter readStripeFooter(StripeInformation stripe) throws IOException {
      OrcProto.StripeFooter.Builder fb = OrcProto.StripeFooter.newBuilder();
      if (!isEmpty) {
        fb.addStreams(OrcProto.Stream.newBuilder().setColumn(0).setLength(20).setKind(OrcProto.Stream.Kind.LENGTH))
          .addStreams(OrcProto.Stream.newBuilder().setColumn(0).setLength(40).setKind(OrcProto.Stream.Kind.DATA))
          .addColumns(OrcProto.ColumnEncoding.newBuilder().setDictionarySize(10).setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2));
      }
      OrcProto.StripeFooter footer = fb.build();
      if (doStreamStep) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream cos = CodedOutputStream.newInstance(baos);
        footer.writeTo(cos);
        cos.flush();
        footer = OrcProto.StripeFooter.newBuilder().mergeFrom(baos.toByteArray()).build();
      }
      return footer;
    }

    @Override
    public DiskRangeList readFileData(DiskRangeList range, long baseOffset, boolean doForceDirect) throws IOException {
      return null;
    }

    @Override
    public boolean isTrackingDiskRanges() {
      return false;
    }

    @Override
    public void releaseBuffer(ByteBuffer toRelease) {

    }

    @Override
    public DataReader clone() {
      return null;
    }

    @Override
    public void close() throws IOException {
    }
  }

  @Test
  public void testMetadata() throws IOException {
    // Mostly tests that it doesn't crash.
    OrcStripeMetadata osm = OrcStripeMetadata.createDummy(0);
    HashMap<Class<?>, ObjectEstimator> map =
        IncrementalObjectSizeEstimator.createEstimators(osm);
    IncrementalObjectSizeEstimator.addEstimator("com.google.protobuf.LiteralByteString", map);
    ObjectEstimator root = map.get(OrcStripeMetadata.class);

    LOG.info("Estimated " + root.estimate(osm, map) + " for a dummy OSM");

    OrcBatchKey stripeKey = null;
    DummyMetadataReader mr = new DummyMetadataReader();
    mr.doStreamStep = false;
    mr.isEmpty = true;
    StripeInformation si = Mockito.mock(StripeInformation.class);
    Mockito.when(si.getNumberOfRows()).thenReturn(0L);
    osm = new OrcStripeMetadata(stripeKey, mr, si, null, null, null, null);
    LOG.info("Estimated " + root.estimate(osm, map) + " for an empty OSM");
    mr.doStreamStep = true;
    osm = new OrcStripeMetadata(stripeKey, mr, si, null, null, null, null);
    LOG.info("Estimated " + root.estimate(osm, map) + " for an empty OSM after serde");

    mr.isEmpty = false;
    stripeKey = new OrcBatchKey(0, 0, 0);
    osm = new OrcStripeMetadata(stripeKey, mr, si, null, null, null, null);
    LOG.info("Estimated " + root.estimate(osm, map) + " for a test OSM");
    osm.resetRowIndex();
    LOG.info("Estimated " + root.estimate(osm, map) + " for a test OSM w/o row index");
    mr.doStreamStep = true;
    osm = new OrcStripeMetadata(stripeKey, mr, si, null, null, null, null);
    LOG.info("Estimated " + root.estimate(osm, map) + " for a test OSM after serde");
    osm.resetRowIndex();
    LOG.info("Estimated " + root.estimate(osm, map) + " for a test OSM w/o row index after serde");

    OrcFileMetadata ofm = OrcFileMetadata.createDummy(0);
    map = IncrementalObjectSizeEstimator.createEstimators(ofm);
    IncrementalObjectSizeEstimator.addEstimator("com.google.protobuf.LiteralByteString", map);
    root = map.get(OrcFileMetadata.class);

    LOG.info("Estimated " + root.estimate(ofm, map) + " for a dummy OFM");
  }

  private static class Struct {
    Integer i;
    int j = 0;
    LinkedHashSet<Object> list2;
    List<Object> list;
  }
  private static class Struct2 {
    Struct2 next;
    Struct2 prev;
    Struct2 top;
  }

  @Test
  public void testSimpleTypes() {
    JavaDataModel memModel = JavaDataModel.get();
    int intSize = runEstimate(new Integer(0), memModel, null);
    runEstimate(new String(""), memModel, "empty string");
    runEstimate(new String("foobarzzzzzzzzzzzzzz"), memModel, null);
    List<Object> list = new ArrayList<Object>(0);
    runEstimate(list, memModel, "empty ArrayList");
    list.add(new String("zzz"));
    runEstimate(list, memModel, "ArrayList - one string");
    list.add(new Integer(5));
    list.add(new Integer(6));
    int arrayListSize = runEstimate(list, memModel, "ArrayList - 3 elements");
    LinkedHashSet<Object> list2 = new LinkedHashSet<Object>(0);
    runEstimate(list2, memModel, "empty LinkedHashSet");
    list2.add(new String("zzzz"));
    runEstimate(list2, memModel, "LinkedHashSet - one string");
    list2.add(new Integer(7));
    list2.add(new Integer(4));
    int lhsSize = runEstimate(list2, memModel, "LinkedHashSet - 3 elements");

    Struct struct = new Struct();
    int structSize = runEstimate(struct, memModel, "Struct - empty");
    struct.i = 10;
    int structSize2 = runEstimate(struct, memModel, "Struct - one reference");
    assertEquals(intSize + structSize, structSize2);
    struct.list = list;
    int structSize3 = runEstimate(struct, memModel, "Struct - with ArrayList");
    assertEquals(arrayListSize + structSize2, structSize3);
    struct.list2 = list2;
    int structSize4 = runEstimate(struct, memModel, "Struct - with LinkedHashSet");
    assertEquals(lhsSize + structSize3, structSize4);

    Struct2 struct2 = new Struct2();
    int recSize1 = runEstimate(struct2, memModel, "recursive struct - empty");
    struct2.next = new Struct2();
    struct2.top = new Struct2();
    int recSize2 = runEstimate(struct2, memModel, "recursive struct - no ring");
    assertEquals(recSize1 * 3, recSize2);
    struct2.next.prev = struct2;
    int recSize3 = runEstimate(struct2, memModel, "recursive struct - ring added");
    assertEquals(recSize2, recSize3);
  }

  private int runEstimate(Object obj, JavaDataModel memModel, String desc) {
    HashMap<Class<?>, ObjectEstimator> map = IncrementalObjectSizeEstimator.createEstimators(obj);
    ObjectEstimator root = map.get(obj.getClass());
    int estimate = root.estimate(obj, map);
    LOG.info("Estimated " + estimate + " for " + (desc == null ? obj.getClass().getName() : desc));
    return estimate;
  }

}
