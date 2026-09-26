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
package org.apache.hadoop.hive.llap.io.decode;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.llap.io.encoded.ParquetEncodedColumnBatch;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.Version;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Page-level parity between {@link ParquetCachedPageReadStore} and parquet's own
 * ColumnChunkPageReadStore (what {@link ParquetFileReader#readNextRowGroup()} returns).
 *
 * <p>Both stores read the same file and the same row group. The stock one goes through
 * ParquetFileReader; ours gets a simulated LLAP cache, where the chunk bytes are split into small
 * {@link MemoryBuffer}s that do not line up with page or chunk boundaries, each one at a non-zero
 * position inside a larger backing buffer like the arena slices BuddyAllocator returns. So page
 * headers and page payloads end up split across buffers, which is the interesting case here.
 *
 * <p>The comparison covers the dictionary page and every data page of every projected column:
 * value/row/null counts, encodings, sizes, statistics and the decompressed bytes. {@code getCrc()}
 * is left out because the native reader does no page checksum verification at all, while the stock
 * reader does when {@code parquet.page.verify-checksum.enabled} is set.
 */
public class TestParquetCachedPageReadStore {

  /**
   * The parquet release {@link ParquetCachedPageReadStore} was last read against. Its javadoc lists
   * the two members that re-implement parquet-hadoop internals; bump this once they have been
   * diffed against the new release.
   */
  private static final String REVIEWED_PARQUET_VERSION = "1.18.0";

  private static final int ROWS = 3000;
  /** Small enough that every column chunk holds several pages. */
  private static final int PAGE_SIZE = 800;
  /** Small enough that the file gets several row groups. */
  private static final int ROW_GROUP_SIZE = 16 * 1024;
  /**
   * Cache buffer size for the sliced runs. Not a power of two and not a divisor of any chunk length,
   * so buffer boundaries land in the middle of page headers and page payloads.
   */
  private static final int BUFFER_GRAIN = 700;
  /** Filler around the payload of every simulated cache buffer; must never be read. */
  private static final byte POISON = (byte) 0xAB;
  private static final int PAD = 37;

  private static final MessageType SCHEMA = Types.buildMessage()
      // Required: no definition levels at all, so the V2 def-level slice is empty.
      .required(PrimitiveTypeName.INT32).named("id")
      // Low cardinality: stays dictionary-encoded, so there is a dictionary page to compare.
      .optional(PrimitiveTypeName.INT32).named("bucket")
      .optional(PrimitiveTypeName.INT64).named("big")
      .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("name")
      .optional(PrimitiveTypeName.DOUBLE).named("ratio")
      .optional(PrimitiveTypeName.BOOLEAN).named("flag")
      .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(4)
          .as(LogicalTypeAnnotation.decimalType(2, 7)).named("dec")
      // Repeated: the only column with a repetition level above zero, so it is the only one whose V2
      // pages carry repetition-level bytes. Without it the rep-level slice is always empty and the
      // order the three V2 slices are taken in would not be checked by anything.
      .repeated(PrimitiveTypeName.INT32).named("tags")
      .named("hive_schema");

  private static Configuration conf;
  private static java.nio.file.Path tmpDir;

  @BeforeClass
  public static void setUpClass() throws IOException {
    conf = new Configuration();
    tmpDir = Files.createTempDirectory("llap-parquet-page-store");
  }

  @AfterClass
  public static void tearDownClass() throws IOException {
    FileSystem.getLocal(conf).delete(new Path(tmpDir.toString()), true);
  }

  /**
   * Fails when the parquet dependency moves, because the page parity below only proves that the
   * copied logic still matches the release it is compiled against - it cannot tell anyone to go and
   * re-read the upstream code it was copied from.
   */
  @Test
  public void testParquetVersionWasReviewed() {
    assertEquals("Parquet went from " + REVIEWED_PARQUET_VERSION + " to " + Version.VERSION_NUMBER
        + ". ParquetCachedPageReadStore.readAllPages mirrors ParquetFileReader.Chunk.readAllPages and"
        + " CachedChunkPageReader mirrors ColumnChunkPageReadStore.ColumnChunkPageReader: diff both"
        + " against the new release and check PageReadStore for new default methods, then bump"
        + " REVIEWED_PARQUET_VERSION here.", REVIEWED_PARQUET_VERSION, Version.VERSION_NUMBER);
  }

  @Test
  public void testDataPageV1Uncompressed() throws Exception {
    assertParity(WriterVersion.PARQUET_1_0, CompressionCodecName.UNCOMPRESSED, BUFFER_GRAIN, SCHEMA);
  }

  @Test
  public void testDataPageV1Snappy() throws Exception {
    assertParity(WriterVersion.PARQUET_1_0, CompressionCodecName.SNAPPY, BUFFER_GRAIN, SCHEMA);
  }

  @Test
  public void testDataPageV1Gzip() throws Exception {
    assertParity(WriterVersion.PARQUET_1_0, CompressionCodecName.GZIP, BUFFER_GRAIN, SCHEMA);
  }

  @Test
  public void testDataPageV2Uncompressed() throws Exception {
    assertParity(WriterVersion.PARQUET_2_0, CompressionCodecName.UNCOMPRESSED, BUFFER_GRAIN, SCHEMA);
  }

  @Test
  public void testDataPageV2Snappy() throws Exception {
    assertParity(WriterVersion.PARQUET_2_0, CompressionCodecName.SNAPPY, BUFFER_GRAIN, SCHEMA);
  }

  /**
   * chunks[] is indexed by projection while the footer's block columns are indexed by file order, so
   * dropping columns from the middle of the schema catches any mix-up between the two.
   */
  @Test
  public void testProjectedSubset() throws Exception {
    MessageType projection = new MessageType(SCHEMA.getName(),
        SCHEMA.getType("bucket"), SCHEMA.getType("name"), SCHEMA.getType("dec"));
    assertParity(WriterVersion.PARQUET_1_0, CompressionCodecName.SNAPPY, BUFFER_GRAIN, projection);
  }

  /** The simple case: one cache buffer per chunk, so no page is split across buffers. */
  @Test
  public void testSingleBufferPerChunk() throws Exception {
    assertParity(WriterVersion.PARQUET_1_0, CompressionCodecName.SNAPPY, 0, SCHEMA);
  }

  /**
   * A page type this store does not handle has to be skipped by its compressed size, so that the walk
   * stays aligned on the next page header. Parquet 1.x never writes one inside a column chunk, so
   * there is no file to read: an index page is spliced in front of a real chunk's bytes by hand, and
   * the pages read back have to be the same ones the unspliced chunk gives.
   */
  @Test
  public void testUnknownPageTypeIsSkipped() throws Exception {
    Path file = new Path(tmpDir.toString(), "index-page.parquet");
    writeFile(file, WriterVersion.PARQUET_1_0, CompressionCodecName.SNAPPY);
    byte[] fileBytes = Files.readAllBytes(Paths.get(file.toUri().getPath()));
    ParquetMetadata footer = footerOf(file);
    // "bucket", which stays dictionary-encoded, so the dictionary page is read across the splice too.
    ColumnDescriptor descriptor = SCHEMA.getColumns().get(1);
    ColumnChunkMetaData chunk = chunkFor(footer.getBlocks().get(0), descriptor);
    byte[] chunkBytes = Arrays.copyOfRange(fileBytes, (int) chunk.getStartingPos(),
        (int) (chunk.getStartingPos() + chunk.getTotalSize()));

    ByteArrayOutputStream splicedBytes = new ByteArrayOutputStream();
    byte[] indexPage = new byte[PAD];
    Arrays.fill(indexPage, POISON);
    Util.writePageHeader(
        new PageHeader(PageType.INDEX_PAGE, indexPage.length, indexPage.length), splicedBytes);
    splicedBytes.write(indexPage);
    splicedBytes.write(chunkBytes);

    ParquetReadOptions options = HadoopReadOptions.builder(conf).build();
    CompressionCodecFactory codecFactory = options.getCodecFactory();
    ParquetMetadataConverter converter = new ParquetMetadataConverter(options);
    try {
      PageReader expected = new ParquetCachedPageReadStore(footer,
          singleChunkBatch(chunk, chunkBytes), codecFactory, converter).getPageReader(descriptor);
      PageReader actual = new ParquetCachedPageReadStore(footer,
          singleChunkBatch(chunkWithTotalSize(chunk, splicedBytes.size()), splicedBytes.toByteArray()),
          codecFactory, converter).getPageReader(descriptor);
      assertPageReaderParity("chunk behind an index page", expected, actual, new Coverage());
    } finally {
      codecFactory.release();
    }
  }

  /**
   * The cache ranges a batch carries have to tile the chunk exactly. When they do not - a buffer
   * missing, or one that stops short of the chunk's end - the walk would read whatever is on the other
   * side of the seam as page bytes and fail much later somewhere inside a decoder, so the store checks
   * the tiling up front and names the chunk that broke it.
   */
  @Test
  public void testCachedBuffersThatDoNotTileTheChunkAreRejected() throws Exception {
    Path file = new Path(tmpDir.toString(), "mistiled.parquet");
    writeFile(file, WriterVersion.PARQUET_1_0, CompressionCodecName.SNAPPY);
    byte[] fileBytes = Files.readAllBytes(Paths.get(file.toUri().getPath()));
    ParquetMetadata footer = footerOf(file);
    MessageType projection = new MessageType(SCHEMA.getName(), SCHEMA.getType("name"));
    String path = chunkFor(footer.getBlocks().getFirst(), projection.getColumns().getFirst())
        .getPath().toString();
    int buffers = cachedBatch(footer, 0, projection, fileBytes, BUFFER_GRAIN, new Coverage())
        .columnBuffers()[0].length;
    assertTrue("the fixture needs a chunk spread over several cache buffers", buffers > 2);

    ParquetEncodedColumnBatch withGap =
        cachedBatch(footer, 0, projection, fileBytes, BUFFER_GRAIN, new Coverage());
    dropBuffer(withGap, 1);
    IOException gap = assertThrows(IOException.class, () -> cachedStore(footer, withGap));
    assertTrue(gap.getMessage(), gap.getMessage().contains(path));
    assertTrue(gap.getMessage(), gap.getMessage().contains("does not continue at"));

    ParquetEncodedColumnBatch truncated =
        cachedBatch(footer, 0, projection, fileBytes, BUFFER_GRAIN, new Coverage());
    dropBuffer(truncated, buffers - 1);
    IOException tail = assertThrows(IOException.class, () -> cachedStore(footer, truncated));
    assertTrue(tail.getMessage(), tail.getMessage().contains(path));
    assertTrue(tail.getMessage(), tail.getMessage().contains("the chunk ends at"));
  }

  private static void assertParity(WriterVersion version, CompressionCodecName codec,
      int grain, MessageType projection) throws Exception {
    Path file = new Path(tmpDir.toString(), version + "-" + codec + "-" + grain + ".parquet");
    writeFile(file, version, codec);
    byte[] fileBytes = Files.readAllBytes(Paths.get(file.toUri().getPath()));

    ParquetMetadata footer = footerOf(file);
    assertTrue("the fixture should have several row groups", footer.getBlocks().size() > 1);

    ParquetReadOptions options = HadoopReadOptions.builder(conf).build();
    CompressionCodecFactory codecFactory = options.getCodecFactory();
    ParquetMetadataConverter converter = new ParquetMetadataConverter(options);

    Coverage coverage = new Coverage();
    try (ParquetFileReader stockReader = ParquetFileReader.open(HadoopInputFile.fromPath(file, conf))) {
      stockReader.setRequestedSchema(projection);
      for (int rowGroupIx = 0; rowGroupIx < footer.getBlocks().size(); ++rowGroupIx) {
        PageReadStore expected = stockReader.readNextRowGroup();
        assertNotNull("parquet ran out of row groups at " + rowGroupIx, expected);

        ParquetEncodedColumnBatch batch =
            cachedBatch(footer, rowGroupIx, projection, fileBytes, grain, coverage);
        PageReadStore actual =
            new ParquetCachedPageReadStore(footer, batch, codecFactory, converter);

        assertEquals("row group " + rowGroupIx + ": row count",
            expected.getRowCount(), actual.getRowCount());
        for (ColumnDescriptor descriptor : projection.getColumns()) {
          String column = "row group " + rowGroupIx + " column " + Arrays.toString(descriptor.getPath());
          assertPageReaderParity(column, expected.getPageReader(descriptor),
              actual.getPageReader(descriptor), coverage);
        }
        assertCacheBuffersUntouched("row group " + rowGroupIx, batch);
        ++coverage.rowGroups;
      }
      assertNull("parquet has row groups left over", stockReader.readNextRowGroup());
    } finally {
      codecFactory.release();
    }
    coverage.assertCovered(version, grain, projection.getColumns().size());
  }

  private static ParquetMetadata footerOf(Path file) throws IOException {
    return ParquetFileReader.readFooter(
        HadoopInputFile.fromPath(file, conf), ParquetMetadataConverter.NO_FILTER);
  }

  private static void assertPageReaderParity(String column, PageReader expected, PageReader actual,
      Coverage coverage) throws IOException {
    assertNotNull(column + ": parquet has no page reader", expected);
    assertNotNull(column + ": no cached page reader", actual);
    assertEquals(column + ": total value count",
        expected.getTotalValueCount(), actual.getTotalValueCount());
    assertDictionaryParity(column, expected.readDictionaryPage(), actual.readDictionaryPage(), coverage);

    int pages = 0;
    while (true) {
      DataPage expectedPage = expected.readPage();
      DataPage actualPage = actual.readPage();
      if (expectedPage == null || actualPage == null) {
        assertNull(column + ": cached reader is short by at least one page", expectedPage);
        assertNull(column + ": cached reader has " + (pages + 1) + " pages or more, parquet has "
            + pages, actualPage);
        break;
      }
      assertDataPageParity(column + " page " + pages, expectedPage, actualPage, coverage);
      ++pages;
    }
    assertTrue(column + ": no pages were compared", pages > 0);
    coverage.dataPages += pages;
    coverage.maxPagesPerColumn = Math.max(coverage.maxPagesPerColumn, pages);
    ++coverage.columns;
  }

  private static void assertDictionaryParity(String column, DictionaryPage expected,
      DictionaryPage actual, Coverage coverage) throws IOException {
    if (expected == null) {
      assertNull(column + ": unexpected dictionary page", actual);
      return;
    }
    assertNotNull(column + ": missing dictionary page", actual);
    assertEquals(column + ": dictionary encoding", expected.getEncoding(), actual.getEncoding());
    assertEquals(column + ": dictionary size", expected.getDictionarySize(), actual.getDictionarySize());
    assertEquals(column + ": dictionary uncompressed size",
        expected.getUncompressedSize(), actual.getUncompressedSize());
    assertBytes(column + ": dictionary bytes", expected.getBytes(), actual.getBytes());
    ++coverage.dictionaryPages;
  }

  private static void assertDataPageParity(String what, DataPage expected, DataPage actual,
      Coverage coverage) throws IOException {
    assertSame(what + ": page type", expected.getClass(), actual.getClass());
    assertEquals(what + ": value count", expected.getValueCount(), actual.getValueCount());
    assertEquals(what + ": uncompressed size",
        expected.getUncompressedSize(), actual.getUncompressedSize());
    assertEquals(what + ": compressed size",
        expected.getCompressedSize(), actual.getCompressedSize());
    assertEquals(what + ": index row count", expected.getIndexRowCount(), actual.getIndexRowCount());
    assertEquals(what + ": first row index", expected.getFirstRowIndex(), actual.getFirstRowIndex());
    if (expected instanceof DataPageV1 expectedV1 && actual instanceof DataPageV1 actualV1) {
      assertEquals(what + ": repetition level encoding",
          expectedV1.getRlEncoding(), actualV1.getRlEncoding());
      assertEquals(what + ": definition level encoding",
          expectedV1.getDlEncoding(), actualV1.getDlEncoding());
      assertEquals(what + ": value encoding",
          expectedV1.getValueEncoding(), actualV1.getValueEncoding());
      assertEquals(what + ": statistics", expectedV1.getStatistics(), actualV1.getStatistics());
      assertBytes(what + ": page bytes", expectedV1.getBytes(), actualV1.getBytes());
    } else if (expected instanceof DataPageV2 expectedV2 && actual instanceof DataPageV2 actualV2) {
      assertEquals(what + ": row count", expectedV2.getRowCount(), actualV2.getRowCount());
      assertEquals(what + ": null count", expectedV2.getNullCount(), actualV2.getNullCount());
      assertEquals(what + ": data encoding",
          expectedV2.getDataEncoding(), actualV2.getDataEncoding());
      assertEquals(what + ": compressed flag", expectedV2.isCompressed(), actualV2.isCompressed());
      assertEquals(what + ": statistics", expectedV2.getStatistics(), actualV2.getStatistics());
      assertBytes(what + ": repetition levels",
          expectedV2.getRepetitionLevels(), actualV2.getRepetitionLevels());
      assertBytes(what + ": definition levels",
          expectedV2.getDefinitionLevels(), actualV2.getDefinitionLevels());
      assertBytes(what + ": data", expectedV2.getData(), actualV2.getData());
      coverage.v2RepetitionLevelBytes += actualV2.getRepetitionLevels().size();
    } else {
      fail(what + ": unhandled page type " + expected.getClass());
    }
  }

  private static void assertBytes(String what, BytesInput expected, BytesInput actual)
      throws IOException {
    assertEquals(what + " size", expected.size(), actual.size());
    assertArrayEquals(what, expected.toByteArray(), actual.toByteArray());
  }

  // ---- simulated cache ----

  /**
   * Builds the batch the reader would hand the consumer: one entry per projected column, with the
   * chunk's bytes spread over cache buffers tiled on {@code grain} (or a single buffer when
   * {@code grain} is not positive).
   */
  private static ParquetEncodedColumnBatch cachedBatch(ParquetMetadata footer, int rowGroupIx,
      MessageType projection, byte[] fileBytes, int grain, Coverage coverage) {
    BlockMetaData block = footer.getBlocks().get(rowGroupIx);
    List<ColumnDescriptor> columns = projection.getColumns();
    ColumnChunkMetaData[] chunks = new ColumnChunkMetaData[columns.size()];
    for (int pc = 0; pc < chunks.length; ++pc) {
      chunks[pc] = chunkFor(block, columns.get(pc));
    }

    ParquetEncodedColumnBatch batch = new ParquetEncodedColumnBatch();
    batch.init("test-file-key", rowGroupIx, chunks);
    for (int pc = 0; pc < chunks.length; ++pc) {
      tileChunk(batch, pc, fileBytes, grain);
      coverage.maxBuffersPerChunk =
          Math.max(coverage.maxBuffersPerChunk, batch.columnBuffers()[pc].length);
    }
    return batch;
  }

  /** One projected column, whole chunk in a single cache buffer at the chunk's own file offset. */
  private static ParquetEncodedColumnBatch singleChunkBatch(ColumnChunkMetaData chunk, byte[] chunkBytes) {
    ParquetEncodedColumnBatch batch = new ParquetEncodedColumnBatch();
    batch.init("test-file-key", 0, new ColumnChunkMetaData[] {chunk});
    batch.columnBuffers()[0] = new MemoryBuffer[] {new CacheBuffer(chunkBytes, 0, chunkBytes.length)};
    batch.bufferOffsets()[0] = new long[] {chunk.getStartingPos()};
    batch.bufferLengths()[0] = new int[] {chunkBytes.length};
    return batch;
  }

  /**
   * The same chunk with a different byte length, for a chunk whose bytes were assembled by hand. The
   * page offsets are kept, so {@code getStartingPos()} still points at where the chunk starts.
   */
  private static ColumnChunkMetaData chunkWithTotalSize(ColumnChunkMetaData chunk, long totalSize) {
    return ColumnChunkMetaData.get(chunk.getPath(), chunk.getPrimitiveType(), chunk.getCodec(),
        chunk.getEncodingStats(), chunk.getEncodings(), chunk.getStatistics(),
        chunk.getFirstDataPageOffset(), chunk.getDictionaryPageOffset(), chunk.getValueCount(),
        totalSize, chunk.getTotalUncompressedSize());
  }

  /** Removes one cache buffer of the batch's single column, leaving a hole in the chunk's coverage. */
  private static void dropBuffer(ParquetEncodedColumnBatch batch, int ix) {
    MemoryBuffer[] buffers = batch.columnBuffers()[0];
    long[] offsets = batch.bufferOffsets()[0];
    int[] lengths = batch.bufferLengths()[0];
    List<MemoryBuffer> keptBuffers = new ArrayList<>();
    List<Long> keptOffsets = new ArrayList<>();
    List<Integer> keptLengths = new ArrayList<>();
    for (int i = 0; i < buffers.length; ++i) {
      if (i != ix) {
        keptBuffers.add(buffers[i]);
        keptOffsets.add(offsets[i]);
        keptLengths.add(lengths[i]);
      }
    }
    batch.columnBuffers()[0] = keptBuffers.toArray(new MemoryBuffer[0]);
    batch.bufferOffsets()[0] = keptOffsets.stream().mapToLong(Long::longValue).toArray();
    batch.bufferLengths()[0] = keptLengths.stream().mapToInt(Integer::intValue).toArray();
  }

  private static PageReadStore cachedStore(ParquetMetadata footer, ParquetEncodedColumnBatch batch)
      throws IOException {
    ParquetReadOptions options = HadoopReadOptions.builder(conf).build();
    CompressionCodecFactory codecFactory = options.getCodecFactory();
    try {
      return new ParquetCachedPageReadStore(footer, batch, codecFactory,
          new ParquetMetadataConverter(options));
    } finally {
      codecFactory.release();
    }
  }

  private static ColumnChunkMetaData chunkFor(BlockMetaData block, ColumnDescriptor descriptor) {
    for (ColumnChunkMetaData chunk : block.getColumns()) {
      if (Arrays.equals(chunk.getPath().toArray(), descriptor.getPath())) {
        return chunk;
      }
    }
    throw new AssertionError("no chunk for " + Arrays.toString(descriptor.getPath()));
  }

  /**
   * Fills the batch's buffer arrays for one projected column. Buffers are aligned to absolute file
   * offsets rather than to the chunk, so the first one usually starts before the chunk and the last
   * one ends after it; the store has to trim that overhang away.
   */
  private static void tileChunk(ParquetEncodedColumnBatch batch, int pc, byte[] fileBytes, int grain) {
    ColumnChunkMetaData chunk = batch.chunks()[pc];
    long chunkStart = chunk.getStartingPos();
    long chunkEnd = chunkStart + chunk.getTotalSize();

    List<MemoryBuffer> buffers = new ArrayList<>();
    List<Long> offsets = new ArrayList<>();
    List<Integer> lengths = new ArrayList<>();
    long pos = grain > 0 ? (chunkStart / grain) * grain : chunkStart;
    while (pos < chunkEnd) {
      long end = grain > 0 ? Math.min(pos + grain, fileBytes.length) : chunkEnd;
      int length = (int) (end - pos);
      buffers.add(new CacheBuffer(fileBytes, (int) pos, length));
      offsets.add(pos);
      lengths.add(length);
      pos = end;
    }

    batch.columnBuffers()[pc] = buffers.toArray(new MemoryBuffer[0]);
    batch.bufferOffsets()[pc] = offsets.stream().mapToLong(Long::longValue).toArray();
    batch.bufferLengths()[pc] = lengths.stream().mapToInt(Integer::intValue).toArray();
  }

  /**
   * A cache buffer holding {@code fileBytes[offset, offset + length)} at a non-zero position inside a
   * larger backing buffer, the way {@code LlapAllocatorBuffer.initialize} leaves an arena slice:
   * position at the payload, limit at its end. The padding is filled with {@link #POISON} so that an
   * off-by-one read picks up garbage rather than a zero that might still parse.
   *
   * <p>It also remembers that initial position and limit, so {@link #assertUntouched} can check the
   * store left them alone. A real cache buffer is shared with whoever else is reading that range, so
   * walking it by moving its own position instead of a dup would corrupt another reader's view.
   */
  private static final class CacheBuffer implements MemoryBuffer {
    private final ByteBuffer arena;
    private final int initialPosition;
    private final int initialLimit;

    CacheBuffer(byte[] fileBytes, int offset, int length) {
      arena = ByteBuffer.allocate(PAD + length + PAD);
      Arrays.fill(arena.array(), POISON);
      arena.position(PAD);
      arena.put(fileBytes, offset, length);
      arena.position(PAD);
      arena.limit(PAD + length);
      initialPosition = arena.position();
      initialLimit = arena.limit();
    }

    @Override
    public ByteBuffer getByteBufferRaw() {
      return arena;
    }

    @Override
    public ByteBuffer getByteBufferDup() {
      return arena.duplicate();
    }

    void assertUntouched(String what) {
      assertEquals(what + ": the store moved a shared cache buffer's position",
          initialPosition, arena.position());
      assertEquals(what + ": the store moved a shared cache buffer's limit",
          initialLimit, arena.limit());
    }
  }

  private static void assertCacheBuffersUntouched(String what, ParquetEncodedColumnBatch batch) {
    for (int pc = 0; pc < batch.chunks().length; ++pc) {
      for (MemoryBuffer buffer : batch.columnBuffers()[pc]) {
        ((CacheBuffer) buffer).assertUntouched(what + " column " + batch.chunks()[pc].getPath());
      }
    }
  }

  private static void writeFile(Path path, WriterVersion version, CompressionCodecName codec)
      throws IOException {
    FileSystem.getLocal(conf).delete(path, false);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withConf(conf)
        .withType(SCHEMA)
        .withWriterVersion(version)
        .withCompressionCodec(codec)
        .withRowGroupSize((long) ROW_GROUP_SIZE)
        .withPageSize(PAGE_SIZE)
        // The writer only reconsiders page/row-group boundaries at these row counts; keep them
        // small so PAGE_SIZE actually takes effect and chunks end up multi-page.
        .withMinRowCountForPageSizeCheck(8)
        .withMaxRowCountForPageSizeCheck(32)
        .build()) {
      SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
      for (int i = 0; i < ROWS; ++i) {
        Group g = factory.newGroup();
        g.append("id", i);
        g.append("bucket", i % 37);
        g.append("big", (long) i * 1_000_003L);
        if (i % 7 != 0) {
          g.append("name", "name-" + i);
        }
        g.append("ratio", i / 3.0);
        g.append("flag", i % 3 == 0);
        if (i % 11 != 0) {
          g.append("dec", Binary.fromConstantByteArray(ByteBuffer.allocate(4).putInt(i * 13).array()));
        }
        // 0, 1 or 2 values per row, so the repetition levels vary from row to row.
        for (int t = 0; t < i % 3; ++t) {
          g.append("tags", i * 10 + t);
        }
        writer.write(g);
      }
    }
  }

  /**
   * What a parity run actually compared. {@link #assertParity} checks it at the end of every run, so
   * if the fixture ever stops producing multi-page chunks, dictionary pages or split buffers, the test
   * fails instead of comparing next to nothing and passing.
   */
  private static final class Coverage {
    private int rowGroups;
    private int columns;
    private int dataPages;
    private int dictionaryPages;
    private int maxPagesPerColumn;
    private int maxBuffersPerChunk;
    private long v2RepetitionLevelBytes;

    void assertCovered(WriterVersion version, int grain, int columnsPerRowGroup) {
      assertTrue("no row groups compared", rowGroups > 0);
      assertEquals("every projected column of every row group should have been compared",
          rowGroups * columnsPerRowGroup, columns);
      assertTrue("no data pages compared", dataPages > 0);
      assertTrue("no dictionary page compared", dictionaryPages > 0);
      assertTrue("every chunk had a single page, so the multi-page path was not exercised",
          maxPagesPerColumn > 1);
      if (version == WriterVersion.PARQUET_2_0) {
        assertTrue("no V2 page carried repetition levels, so the order the rep/def/value slices are"
            + " taken in was not exercised", v2RepetitionLevelBytes > 0);
      }
      if (grain > 0) {
        assertTrue("every chunk fit in one cache buffer, so no page was split across buffers",
            maxBuffersPerChunk > 1);
      } else {
        assertEquals("this run should use one buffer per chunk", 1, maxBuffersPerChunk);
      }
    }
  }
}
