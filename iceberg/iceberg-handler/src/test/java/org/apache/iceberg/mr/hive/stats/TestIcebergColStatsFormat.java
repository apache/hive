/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.stats;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

/** The partition blob frame: what a read fetches and decodes is only what it asked for. */
public class TestIcebergColStatsFormat {

  @Test
  public void nothingInTheFrameIsPacked() throws Exception {
    // an entry and a sketch both come out no smaller packed, and what a read would save in bytes
    // it pays back unpacking them, so the frame stores every slice whole
    ColumnStatisticsObj statsObj = columns(1).getFirst();
    byte[] sketch = new byte[4096];
    new Random(11).nextBytes(sketch);
    statsObj.getStatsData().getLongStats().setBitVectors(sketch);

    ByteBuffer blob = IcebergColStatsWriter.encodePartBlob(List.of(statsObj), ids(1));

    Assert.assertTrue("the sketch takes its own size in the blob", blob.remaining() > sketch.length);
    Assert.assertEquals(List.of(statsObj), IcebergColStatsReader.decodePartBlob(blob, null, true));
  }

  @Test
  public void aVectorComesBackOnlyWhenAskedForAndAHistogramAlways() throws Exception {
    // a vector is read to merge distinct counts across partitions, which a read either wants or
    // does not; a histogram is read wherever the planner finds one, so it is given back either way
    ColumnStatisticsObj statsObj = columns(1).getFirst();
    statsObj.getStatsData().getLongStats().setBitVectors(new byte[] {1, 2, 3, 4});
    statsObj.getStatsData().getLongStats().setHistogram(new byte[] {5, 6, 7});
    ByteBuffer blob = IcebergColStatsWriter.encodePartBlob(List.of(statsObj), ids(1));

    ColumnStatisticsObj asked =
        IcebergColStatsReader.decodePartBlob(blob, null, true).getFirst();
    Assert.assertArrayEquals("the vector is put back where it was taken from",
        new byte[] {1, 2, 3, 4}, asked.getStatsData().getLongStats().getBitVectors());
    Assert.assertArrayEquals("and so is the histogram",
        new byte[] {5, 6, 7}, asked.getStatsData().getLongStats().getHistogram());

    ColumnStatisticsObj unasked =
        IcebergColStatsReader.decodePartBlob(blob, null, false).getFirst();
    Assert.assertFalse("a read that did not ask for the vector does not get it",
        unasked.getStatsData().getLongStats().isSetBitVectors());
    Assert.assertArrayEquals("the histogram comes back whatever was asked",
        new byte[] {5, 6, 7}, unasked.getStatsData().getLongStats().getHistogram());
    Assert.assertEquals("what the entry states of itself stands either way",
        statsObj.getStatsData().getLongStats().getNumDVs(),
        unasked.getStatsData().getLongStats().getNumDVs());
  }

  @Test
  public void aFrameFromAVersionThisReaderDoesNotKnowReadsAsAbsent() throws Exception {
    ByteBuffer blob = IcebergColStatsWriter.encodePartBlob(columns(2), ids(2));
    blob.putInt(0, IcebergColStatsCodec.BLOB_VERSION + 1);

    Assert.assertTrue(IcebergColStatsReader.decodePartBlob(blob, null, true).isEmpty());
  }

  @Test
  public void aWideFrameStillYieldsOnlyTheAskedColumns() throws Exception {
    // 3000 columns push the header well past anything a single small read would hold
    ByteBuffer blob = IcebergColStatsWriter.encodePartBlob(columns(3000), ids(3000));

    List<ColumnStatisticsObj> read =
        IcebergColStatsReader.decodePartBlob(blob, Set.of("c0", "c1499", "c2999"), true);

    Assert.assertEquals(List.of("c0", "c1499", "c2999"),
        read.stream().map(ColumnStatisticsObj::getColName).toList());
    for (ColumnStatisticsObj obj : read) {
      long ordinal = Long.parseLong(obj.getColName().substring(1));
      Assert.assertEquals(ordinal, obj.getStatsData().getLongStats().getLowValue());
      Assert.assertEquals(2 * ordinal, obj.getStatsData().getLongStats().getHighValue());
    }
  }

  @Test
  public void aVectorLeftBehindByAMergeIsStillStored() throws Exception {
    // a merge leaves the vector as the estimator it merged, and the field Thrift writes stays
    // empty until something asks for the bytes. Writing the entry without asking stored an entry
    // with no vector, and the next merge had nothing to merge
    org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector data =
        new org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector();
    data.setNumNulls(0);
    data.setNumDVs(5);
    data.setNdvEstimator(org.apache.hadoop.hive.common.ndv.hll.HyperLogLog.builder()
        .setSizeOptimized().build());
    ColumnStatisticsObj statsObj = new ColumnStatisticsObj("c0", "bigint",
        ColumnStatisticsData.longStats(data));

    Assert.assertFalse("Thrift writes the field, which the merge left empty",
        IcebergColStatsCodec.deserialize(IcebergColStatsCodec.serialize(statsObj))
            .getStatsData().getLongStats().isSetBitVectors());
    Assert.assertTrue("what the merge left is what the next one merges",
        IcebergColStatsCodec.deserialize(IcebergColStatsCodec.encodeEntry(statsObj))
            .getStatsData().getLongStats().isSetBitVectors());
    Assert.assertFalse("and a read that wants no vector still gets none",
        IcebergColStatsCodec.decodeEntry(IcebergColStatsCodec.encodeEntry(statsObj), false)
            .getStatsData().getLongStats().isSetBitVectors());
  }

  private static List<ColumnStatisticsObj> columns(int count) {
    return IntStream.range(0, count).mapToObj(i -> {
      LongColumnStatsData longStats = new LongColumnStatsData(0, i + 1);
      longStats.setLowValue(i);
      longStats.setHighValue(2L * i);
      return new ColumnStatisticsObj("c" + i, "bigint", ColumnStatisticsData.longStats(longStats));
    }).toList();
  }


  /** Field ids for entries a test builds, in the order it builds them. */
  private static List<Integer> ids(int count) {
    return IntStream.rangeClosed(1, count).boxed().toList();
  }


  /** A stream over bytes in memory that records what was asked of it. */
  private static final class RecordingStream extends org.apache.iceberg.io.SeekableInputStream {
    private final byte[] bytes;
    private final List<String> reads = Lists.newArrayList();
    private int pos;

    private RecordingStream(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long offset) {
      pos = (int) offset;
    }

    @Override
    public int read() {
      return pos < bytes.length ? bytes[pos++] & 0xff : -1;
    }

    @Override
    public int read(byte[] into, int off, int len) {
      int taken = Math.min(len, bytes.length - pos);
      if (taken <= 0) {
        return -1;
      }
      System.arraycopy(bytes, pos, into, off, taken);
      reads.add(pos + "+" + taken);
      pos += taken;
      return taken;
    }
  }

  /** Lays the given blobs out at the given offsets and reads them back through the run planner. */
  private static RecordingStream layOut(List<byte[]> blobs, List<Long> offsets, List<BlobMetadata> meta) {
    long end = 0;
    for (int i = 0; i < blobs.size(); i++) {
      end = Math.max(end, offsets.get(i) + blobs.get(i).length);
    }
    byte[] file = new byte[(int) end];
    for (int i = 0; i < blobs.size(); i++) {
      System.arraycopy(blobs.get(i), 0, file, offsets.get(i).intValue(), blobs.get(i).length);
      meta.add(new BlobMetadata(IcebergColStatsWriter.HIVE_PART_COL_STATS_BLOB_V1,
          List.of(1), 1L, 1L, offsets.get(i), blobs.get(i).length, null,
          Map.of(IcebergColStatsWriter.PARTITION_FIELD, "p=" + i)));
    }
    return new RecordingStream(file);
  }

  @Test
  public void blobsLyingCloseTogetherAreTakenInOneRead() throws Exception {
    List<byte[]> blobs = Lists.newArrayList();
    for (int i = 0; i < 3; i++) {
      blobs.add(IcebergColStatsCodec.encodeBlob(
          List.of(IcebergColStatsCodec.encodeEntry(longColumn("c" + i, i))), List.of(1)));
    }
    // laid end to end, so no gap is worth a second request
    List<Long> offsets = List.of(0L, (long) blobs.get(0).length,
        (long) blobs.get(0).length + blobs.get(1).length);
    List<BlobMetadata> meta = Lists.newArrayList();
    RecordingStream in = layOut(blobs, offsets, meta);

    Map<String, List<ColumnStatisticsObj>> read = Maps.newLinkedHashMap();
    IcebergColStatsReader.readRanges(in, meta, null, true, read, null);

    Assert.assertEquals("three adjacent blobs are one request", 1, in.reads.size());
    Assert.assertEquals(3, read.size());
    // each partition must hold its own bytes, not a neighbour's
    for (int i = 0; i < 3; i++) {
      Assert.assertEquals("p=" + i + " holds its own entry", "c" + i,
          read.get("p=" + i).getFirst().getColName());
    }
  }

  @Test
  public void aBlobBeyondTheSeekWorthMakingIsTakenOnItsOwn() throws Exception {
    List<byte[]> blobs = Lists.newArrayList();
    for (int i = 0; i < 2; i++) {
      blobs.add(IcebergColStatsCodec.encodeBlob(
          List.of(IcebergColStatsCodec.encodeEntry(longColumn("c" + i, i))), List.of(1)));
    }
    // a gap wider than any seek is worth crossing
    List<Long> offsets = List.of(0L, 8L * 1024 * 1024);
    List<BlobMetadata> meta = Lists.newArrayList();
    RecordingStream in = layOut(blobs, offsets, meta);

    Map<String, List<ColumnStatisticsObj>> read = Maps.newLinkedHashMap();
    IcebergColStatsReader.readRanges(in, meta, null, true, read, null);

    Assert.assertEquals("a wide gap costs a second request", 2, in.reads.size());
    Assert.assertEquals("c0", read.get("p=0").getFirst().getColName());
    Assert.assertEquals("c1", read.get("p=1").getFirst().getColName());
  }

  private static ColumnStatisticsObj longColumn(String name, long value) {
    LongColumnStatsData data = new LongColumnStatsData(0L, 1L);
    data.setLowValue(value);
    data.setHighValue(value);
    return new ColumnStatisticsObj(name, "bigint", ColumnStatisticsData.longStats(data));
  }
}
