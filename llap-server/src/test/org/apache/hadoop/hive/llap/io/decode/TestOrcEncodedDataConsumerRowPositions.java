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
package org.apache.hadoop.hive.llap.io.decode;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer.Includes;
import org.apache.hadoop.hive.llap.io.metadata.ConsumerFileMetadata;
import org.apache.hadoop.hive.llap.io.metadata.ConsumerStripeMetadata;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonIOMetrics;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.RowPositionAwareVectorizedRecordReader;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.encoded.Consumer;
import org.apache.hadoop.hive.ql.io.orc.encoded.IoTrace;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader.OrcEncodedColumnBatch;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcProto.CalendarKind;
import org.apache.orc.OrcProto.ColumnEncoding;
import org.apache.orc.OrcProto.RowIndex;
import org.apache.orc.OrcProto.RowIndexEntry;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.tez.common.counters.TezCounters;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Absolute file row positions reported by {@link OrcEncodedDataConsumer} on every emitted batch.
 * ORC files are read end to end through LLAP IO; the position-less (SerDe-like) metadata case is
 * driven straight into decodeBatch.
 */
public class TestOrcEncodedDataConsumerRowPositions {

  private static final TypeDescription SCHEMA = TypeDescription.fromString("struct<id:bigint,name:string>");
  private static final int STRIDE = 2500;
  // Neither a multiple of the row index stride nor of VectorizedRowBatch.DEFAULT_SIZE.
  private static final int[] STRIPE_ROWS = {8300, 6100, 3700};
  private static final int[] NO_INDEX_STRIPE_ROWS = {5300, 2700};
  private static final List<Integer> ALL_COLUMNS = Arrays.asList(0, 1);

  private static final LlapDaemonIOMetrics IO_METRICS =
      LlapDaemonIOMetrics.create("TestOrcEncodedDataConsumerRowPositions-io", "1", null);

  private static HiveConf daemonConf;
  private static Path indexedFile;
  private static Path noIndexFile;

  @BeforeClass
  public static void setUpClass() throws Exception {
    daemonConf = new HiveConf();
    HiveConf.setVar(daemonConf, ConfVars.LLAP_IO_MEMORY_MODE, "none");

    Path tmpDir = new Path(Files.createTempDirectory("llap-orc-row-positions").toString());
    indexedFile = new Path(tmpDir, "indexed.orc");
    noIndexFile = new Path(tmpDir, "no-index.orc");
    writeFile(indexedFile, STRIDE, STRIPE_ROWS);
    writeFile(noIndexFile, 0, NO_INDEX_STRIPE_ROWS);
    assertLayout(indexedFile, STRIDE, STRIPE_ROWS);
    assertLayout(noIndexFile, 0, NO_INDEX_STRIPE_ROWS);

    LlapProxy.setDaemon(true);
    LlapProxy.initializeLlapIo(daemonConf);
  }

  @AfterClass
  public static void tearDownClass() {
    LlapProxy.close();
  }

  @Test
  public void testRowGroupPositionsAcrossStripes() throws Exception {
    List<long[]> batches = readThroughLlap(indexedFile, ALL_COLUMNS);

    assertEquals(expectedBatches(STRIDE, STRIPE_ROWS), render(batches));
    assertFirstRowIds(batches);
  }

  @Test
  public void testIndexDisabledFileStartsAtStripeBoundary() throws Exception {
    List<long[]> batches = readThroughLlap(noIndexFile, ALL_COLUMNS);

    assertEquals(expectedBatches(Integer.MAX_VALUE, NO_INDEX_STRIPE_ROWS), render(batches));
    assertFirstRowIds(batches);
  }

  /** Without columns, the reader decodes each stripe as one ALL_RGS batch. */
  @Test
  public void testNoColumnReadStartsAtStripeBoundary() throws Exception {
    List<long[]> batches = readThroughLlap(indexedFile, Collections.emptyList());

    assertEquals(expectedBatches(Integer.MAX_VALUE, STRIPE_ROWS), render(batches));
  }

  @Test
  public void testNoStripesReportsNoPosition() throws Exception {
    List<Long> starts = decodeStripe(noStripesMetadata(), 2 * VectorizedRowBatch.DEFAULT_SIZE + 1);

    assertEquals(Collections.nCopies(3, Long.MIN_VALUE), starts);
  }

  // ---- LLAP IO end to end ----

  /** One {startRowInFile, size, id of the first row} per batch, in the order LLAP returned them. */
  private static List<long[]> readThroughLlap(Path path, List<Integer> columnIds) throws Exception {
    RecordReader<NullWritable, VectorizedRowBatch> reader = llapReader(path, columnIds);
    List<long[]> batches = new ArrayList<>();
    try {
      VectorizedRowBatch vrb = reader.createValue();
      while (reader.next(NullWritable.get(), vrb)) {
        long position = ((RowPositionAwareVectorizedRecordReader) reader).getRowNumber();
        batches.add(new long[] {position, vrb.size, ((LongColumnVector) vrb.cols[0]).vector[0]});
      }
    } finally {
      reader.close();
    }
    return batches;
  }

  private static RecordReader<NullWritable, VectorizedRowBatch> llapReader(Path path, List<Integer> columnIds)
      throws IOException {
    JobConf job = jobConf(path);
    long fileLen = path.getFileSystem(job).getFileStatus(path).getLen();
    RecordReader<NullWritable, VectorizedRowBatch> reader = LlapProxy.getIo().orc().vectorizedReaderForPath(
        null, path, null, columnIds, job, 0, fileLen, Reporter.NULL);
    assertNotNull("LLAP should handle this ORC read", reader);
    return reader;
  }

  private static void assertFirstRowIds(List<long[]> batches) {
    for (long[] batch : batches) {
      assertEquals("id of the first row of the batch at " + batch[0], batch[0], batch[2]);
    }
  }

  // ---- decodeBatch with faked metadata ----

  /** Row positions of the batches decodeBatch emits for the first stripe of a zero-column read. */
  private static List<Long> decodeStripe(ConsumerFileMetadata fileMetadata, int stripeRows) throws Exception {
    QueryFragmentCounters counters = new QueryFragmentCounters(daemonConf, new TezCounters());
    List<Long> starts = new ArrayList<>();
    OrcEncodedDataConsumer consumer = new OrcEncodedDataConsumer(null, includes(), counters, IO_METRICS);
    consumer.init(null, null, new IoTrace(0, false));
    consumer.setFileMetadata(fileMetadata);
    consumer.setStripeMetadata(new FakeStripeMetadata(stripeRows));

    OrcEncodedColumnBatch batch = new OrcEncodedColumnBatch();
    batch.init(null, 0, OrcEncodedColumnBatch.ALL_RGS, 0);
    consumer.decodeBatch(batch, new Consumer<ColumnVectorBatch>() {
      @Override public void consumeData(ColumnVectorBatch cvb) {
        starts.add(cvb.startRowInFile);
      }

      @Override public void setDone() {
      }

      @Override public void setError(Throwable t) {
        throw new AssertionError(t);
      }
    });
    return starts;
  }

  private static ConsumerFileMetadata noStripesMetadata() {
    return new ConsumerFileMetadata() {
      @Override public int getStripeCount() {
        return 0;
      }

      @Override public CompressionKind getCompressionKind() {
        return CompressionKind.NONE;
      }

      @Override public List<OrcProto.Type> getTypes() {
        return Collections.emptyList();
      }

      @Override public TypeDescription getSchema() {
        return SCHEMA;
      }

      @Override public OrcFile.Version getFileVersion() {
        return OrcFile.Version.V_0_12;
      }

      @Override public CalendarKind getCalendar() {
        return CalendarKind.PROLEPTIC_GREGORIAN;
      }

      @Override public List<StripeInformation> getStripes() {
        return Collections.emptyList();
      }

      @Override public int getRowIndexStride() {
        return 0;
      }
    };
  }

  /** Reports a row count only. */
  private static final class FakeStripeMetadata implements ConsumerStripeMetadata {
    private final long rowCount;

    FakeStripeMetadata(long rowCount) {
      this.rowCount = rowCount;
    }

    @Override public int getStripeIx() {
      return 0;
    }

    @Override public long getRowCount() {
      return rowCount;
    }

    @Override public List<ColumnEncoding> getEncodings() {
      return Collections.singletonList(ColumnEncoding.newBuilder()
          .setKind(ColumnEncoding.Kind.DIRECT).build());
    }

    @Override public String getWriterTimezone() {
      return "UTC";
    }

    @Override public RowIndexEntry getRowIndexEntry(int colIx, int rgIx) {
      return null;
    }

    @Override public RowIndex[] getRowIndexes() {
      return new RowIndex[0];
    }

    @Override public boolean supportsRowIndexes() {
      return false;
    }
  }

  private static Includes includes() {
    return new Includes() {
      @Override public List<Integer> getPhysicalColumnIds() {
        return Collections.emptyList();
      }

      @Override public List<Integer> getReaderLogicalColumnIds() {
        return Collections.emptyList();
      }

      @Override public List<Integer> getLogicalOrderedColumnIds() {
        return Collections.emptyList();
      }

      @Override public boolean[] generateFileIncludes(TypeDescription fileSchema) {
        throw new UnsupportedOperationException();
      }

      @Override public TypeDescription[] getBatchReaderTypes(TypeDescription fileSchema) {
        return new TypeDescription[0];
      }

      @Override public String[] getOriginalColumnNames(TypeDescription fileSchema) {
        throw new UnsupportedOperationException();
      }

      @Override public String getQueryId() {
        return "test-query";
      }

      @Override public boolean isProbeDecodeEnabled() {
        return false;
      }

      @Override public byte getProbeMjSmallTablePos() {
        return -1;
      }

      @Override public String getProbeCacheKey() {
        return null;
      }

      @Override public String getProbeColName() {
        return null;
      }

      @Override public int getProbeColIdx() {
        return -1;
      }
    };
  }

  // ---- fixture ----

  private static void writeFile(Path path, int stride, int[] stripeRows) throws IOException {
    try (Writer writer = OrcFile.createWriter(path, OrcFile.writerOptions(daemonConf)
        .setSchema(SCHEMA).rowIndexStride(stride).compress(CompressionKind.NONE))) {
      VectorizedRowBatch batch = SCHEMA.createRowBatch();
      LongColumnVector ids = (LongColumnVector) batch.cols[0];
      BytesColumnVector names = (BytesColumnVector) batch.cols[1];
      long row = 0;
      for (int s = 0; s < stripeRows.length; ++s) {
        for (int left = stripeRows[s]; left > 0; ) {
          int size = Math.min(left, batch.getMaxSize());
          batch.size = size;
          for (int i = 0; i < size; ++i, ++row) {
            ids.vector[i] = row;
            names.setVal(i, ("n" + row).getBytes(UTF_8));
          }
          writer.addRowBatch(batch);
          batch.reset();
          left -= size;
        }
        if (s < stripeRows.length - 1) {
          writer.writeIntermediateFooter();
        }
      }
    }
  }

  private static void assertLayout(Path path, int stride, int[] stripeRows) throws IOException {
    try (org.apache.orc.Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(daemonConf))) {
      assertEquals(stride, reader.getRowIndexStride());
      List<StripeInformation> stripes = reader.getStripes();
      assertEquals(stripeRows.length, stripes.size());
      for (int i = 0; i < stripeRows.length; ++i) {
        assertEquals("stripe " + i, stripeRows[i], stripes.get(i).getNumberOfRows());
      }
    }
  }

  private static JobConf jobConf(Path path) {
    JobConf job = new JobConf(daemonConf);
    HiveConf.setBoolVar(job, ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    HiveConf.setVar(job, ConfVars.PLAN, "//tmp");
    job.set(IOConstants.COLUMNS, "id,name");
    job.set(IOConstants.COLUMNS_TYPES, "bigint,string");
    job.set(ColumnProjectionUtils.ORC_SCHEMA_STRING, SCHEMA.toString());

    Properties tblProps = new Properties();
    tblProps.setProperty(META_TABLE_NAME, "default.test_orc_positions");
    TableDesc tableDesc = new TableDesc(OrcInputFormat.class, OrcOutputFormat.class, tblProps);

    MapWork mapWork = new MapWork();
    mapWork.setVectorMode(true);
    mapWork.setVectorizedRowBatchCtx(new VectorizedRowBatchCtx(
        new String[] {"id", "name"},
        new TypeInfo[] {TypeInfoFactory.longTypeInfo, TypeInfoFactory.stringTypeInfo},
        null, null, 0, 0, null, new String[0], null));
    PartitionDesc partitionDesc = new PartitionDesc();
    partitionDesc.setTableDesc(tableDesc);
    mapWork.addPathToPartitionInfo(path.getParent(), partitionDesc);
    Utilities.setMapWork(job, mapWork);
    return job;
  }

  // ---- expectations ----

  /** "startRowInFile:size" of every batch of a file whose stripes are read whole. */
  private static List<String> expectedBatches(int stride, int[] stripeRows) {
    List<String> expected = new ArrayList<>();
    long base = 0;
    for (int rows : stripeRows) {
      for (int rgStart = 0; rgStart < rows; rgStart += stride) {
        int rgRows = Math.min(stride, rows - rgStart);
        for (int off = 0; off < rgRows; off += VectorizedRowBatch.DEFAULT_SIZE) {
          expected.add((base + rgStart + off) + ":" + Math.min(VectorizedRowBatch.DEFAULT_SIZE, rgRows - off));
        }
      }
      base += rows;
    }
    return expected;
  }

  private static List<String> render(List<long[]> batches) {
    List<String> rendered = new ArrayList<>();
    for (long[] batch : batches) {
      rendered.add(batch[0] + ":" + batch[1]);
    }
    return rendered;
  }
}
