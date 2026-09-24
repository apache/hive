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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.llap.io.encoded.ParquetEncodedColumnBatch;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.PrimitiveType;

/**
 * {@link PageReadStore} over one row group's cached column-chunk buffers, so that the vectorized
 * column readers decode straight out of the LLAP cache: no ParquetFileReader, and no copy of the
 * chunk bytes.
 *
 * <p><b>Why this is a copy.</b> Parquet does the same thing over a file, but none of it is
 * reachable from here: {@code ParquetFileReader.Chunk.readAllPages} is private, the store it fills
 * ({@code org.apache.parquet.hadoop.ColumnChunkPageReadStore}) is package-private and so is its
 * {@code addColumn}, and the {@link PageReader} it puts in ({@code ColumnChunkPageReader}) is a
 * package-private {@code final} class. Only the glue below is re-implemented; page header parsing,
 * encoding and statistics conversion, the page value types and decompression are all parquet's own
 * public API.
 *
 * <p><b>On a parquet upgrade.</b> Exactly two members track upstream code, each with a counterpart
 * to diff against the new release:
 * <ul>
 *   <li>{@link #readAllPages} mirrors {@code ParquetFileReader.Chunk.readAllPages}: the page-header
 *       walk, the page-type switch and the mapping of header fields onto {@link DataPageV1},
 *       {@link DataPageV2} and {@link DictionaryPage}. A new page type, a new header field or a
 *       changed page constructor lands here.</li>
 *   <li>{@link CachedChunkPageReader} mirrors {@code ColumnChunkPageReader}: page queueing and
 *       lazy decompression. New {@link PageReader} behaviour lands here.</li>
 * </ul>
 * Also check whether {@link PageReadStore} gained or changed a {@code default} method, since this
 * class implements none of them ({@code getRowIndexOffset}, {@code getRowIndexes} and
 * {@code close} are all inherited). {@code TestParquetCachedPageReadStore} compares this store page
 * for page against the stock one and pins the parquet version, so the upgrade fails the build
 * rather than passing with a divergence.
 *
 * <p>Deliberately unsupported, because LLAP's Parquet path does not use them: the offset index (so
 * {@code getFirstRowIndex} and {@code getIndexRowCount} keep their no-offset-index defaults),
 * column encryption, and page checksum verification.
 *
 * <p>How it works:
 * <ol>
 *   <li><b>Input</b>: a {@link ParquetEncodedColumnBatch} holding the already-cached
 *       {@link MemoryBuffer}s for one row group's projected column chunks, plus the file footer.</li>
 *   <li><b>Stitching and parsing</b>: per chunk, {@link #chunkBuffers} turns the cache buffers into a
 *       list of {@link ByteBuffer} slices covering exactly the chunk's byte region; {@link #readAllPages}
 *       then walks that byte range with {@link Util#readPageHeader}, building {@link DataPageV1} /
 *       {@link DataPageV2} / {@link DictionaryPage} objects whose payloads are
 *       {@link ByteBufferInputStream} views over the cache buffers - the page bytes are never copied.</li>
 *   <li><b>Lazy decompression</b>: each {@link CachedChunkPageReader} keeps the raw bytes and its
 *       {@link BytesInputDecompressor}, and decompresses only when the vectorized column reader
 *       actually calls {@link PageReader#readPage()}, so pages that get pruned never pay the codec cost.</li>
 *   <li><b>Output</b>: the resulting {@code ColumnPath -> PageReader} map backs
 *       {@link #getPageReader(ColumnDescriptor)}, so parquet's VectorizedColumnReader never notices
 *       that it is not reading from a file.</li>
 * </ol>
 */
class ParquetCachedPageReadStore implements PageReadStore {

  private final Map<ColumnPath, PageReader> readers = new HashMap<>();
  private final long rowCount;

  ParquetCachedPageReadStore(ParquetMetadata footer, ParquetEncodedColumnBatch batch,
      CompressionCodecFactory codecFactory, ParquetMetadataConverter converter) throws IOException {
    BlockMetaData block = footer.getBlocks().get(batch.rowGroupIx());
    this.rowCount = block.getRowCount();
    String createdBy = footer.getFileMetaData().getCreatedBy();
    ColumnChunkMetaData[] chunks = batch.chunks();
    // pc is the projection index: chunks[pc] and the batch's three buffer arrays are all indexed by
    // it, while the footer's block lists columns in file order. Keying on the path keeps the two
    // apart, so a projection that drops columns from the middle of the schema still lines up.
    for (int pc = 0; pc < chunks.length; ++pc) {
      ColumnChunkMetaData chunk = chunks[pc];
      readers.put(chunk.getPath(), readAllPages(chunk, chunkBuffers(batch, pc), createdBy,
          codecFactory.getDecompressor(chunk.getCodec()), converter));
    }
  }

  /**
   * Slices of the cached buffers covering exactly the chunk's byte region, in file order.
   *
   * <p>This is the only place in the class that touches bytes, and the only place the cache appears
   * at all: the {@link MemoryBuffer}s are already-populated LLAP cache memory, so there is no read
   * here and nothing to decide - by the time this runs, a cache hit and a miss that
   * {@code ParquetEncodedDataReader} had to fetch look exactly the same.
   *
   * <p>The buffers are cache ranges aligned to absolute file offsets, not to this chunk, so the
   * first and last one usually overhang the chunk (the first may even start before it). That is what
   * the trimming below is for.
   *
   * <p>The trimmed slices have to tile the chunk exactly, which is checked as they are built: a
   * buffer that does not continue where the previous one ended, or one that reaches past the chunk,
   * means the batch is not carrying the buffers the chunk was planned with. Without the check the
   * page walk would read the wrong bytes as page bytes, and fail much later with a decoding error
   * about a corrupt header.
   */
  private static List<ByteBuffer> chunkBuffers(ParquetEncodedColumnBatch batch, int pc)
      throws IOException {
    ColumnChunkMetaData chunk = batch.chunks()[pc];
    long chunkStart = chunk.getStartingPos();
    long chunkEnd = chunkStart + chunk.getTotalSize();
    MemoryBuffer[] columnBuffers = batch.columnBuffers()[pc];
    long[] bufferOffsets = batch.bufferOffsets()[pc];
    int[] bufferLengths = batch.bufferLengths()[pc];
    List<ByteBuffer> slices = new ArrayList<>(columnBuffers.length);
    long covered = chunkStart;
    for (int i = 0; i < columnBuffers.length; ++i) {
      long bufferStart = bufferOffsets[i];
      long bufferEnd = bufferStart + bufferLengths[i];
      long sliceStart = Math.max(chunkStart, bufferStart);
      long sliceEnd = Math.min(chunkEnd, bufferEnd);
      if (sliceStart != covered || sliceEnd <= sliceStart) {
        throw new IOException("Cached buffer " + (i + 1) + " of " + columnBuffers.length + " for column chunk "
            + chunk.getPath() + " covers [" + sliceStart + ", " + sliceEnd + ") of the chunk at ["
            + chunkStart + ", " + chunkEnd + "), which does not continue at " + covered);
      }
      covered = sliceEnd;
      // A dup, because the cache buffer is shared: moving position/limit must not be visible to
      // another reader of the same buffer. slice() then keeps a view of the trimmed region, so the
      // chunk bytes are still the cache's bytes - nothing is copied here or below.
      ByteBuffer bb = columnBuffers[i].getByteBufferDup();
      bb.position(bb.position() + (int) (sliceStart - bufferStart));
      bb.limit(bb.position() + (int) (sliceEnd - sliceStart));
      slices.add(bb.slice());
    }
    if (covered != chunkEnd) {
      throw new IOException("Cached buffers for column chunk " + chunk.getPath() + " cover ["
          + chunkStart + ", " + covered + "), but the chunk ends at " + chunkEnd);
    }
    return slices;
  }

  private static PageReader readAllPages(ColumnChunkMetaData chunk, List<ByteBuffer> buffers,
      String createdBy, BytesInputDecompressor decompressor, ParquetMetadataConverter converter)
      throws IOException {
    // Reads across the buffer boundaries as if the chunk were contiguous, which it is not: a page
    // header or a payload can straddle two cache buffers. sliceBuffers below hands out views into
    // these same buffers, so every page this method builds points at cache memory for as long as it
    // lives - the batch has to keep its refs until the consumer is done decoding.
    ByteBufferInputStream stream = ByteBufferInputStream.wrap(buffers);
    PrimitiveType type = chunk.getPrimitiveType();
    List<DataPage> pages = new ArrayList<>();
    DictionaryPage dictionaryPage = null;
    long valuesRead = 0;
    // Only data pages count towards valuesRead, so the dictionary page and any page type skipped
    // below do not end the walk early; the footer's value count is the only terminator.
    while (valuesRead < chunk.getValueCount()) {
      PageHeader header = Util.readPageHeader(stream);
      int uncompressedSize = header.getUncompressed_page_size();
      int compressedSize = header.getCompressed_page_size();
      switch (header.getType()) {
        case DICTIONARY_PAGE:
          if (dictionaryPage != null) {
            throw new ParquetDecodingException("more than one dictionary page in column " + chunk.getPath());
          }
          DictionaryPageHeader dictHeader = header.getDictionary_page_header();
          dictionaryPage = new DictionaryPage(BytesInput.from(stream.sliceBuffers(compressedSize)),
              uncompressedSize, dictHeader.getNum_values(), converter.getEncoding(dictHeader.getEncoding()));
          break;
        case DATA_PAGE:
          DataPageHeader v1 = header.getData_page_header();
          pages.add(new DataPageV1(BytesInput.from(stream.sliceBuffers(compressedSize)), v1.getNum_values(),
              uncompressedSize, converter.fromParquetStatistics(createdBy, v1.getStatistics(), type),
              converter.getEncoding(v1.getRepetition_level_encoding()),
              converter.getEncoding(v1.getDefinition_level_encoding()),
              converter.getEncoding(v1.getEncoding())));
          valuesRead += v1.getNum_values();
          break;
        case DATA_PAGE_V2:
          DataPageHeaderV2 v2 = header.getData_page_header_v2();
          // In V2 the level bytes sit inside the page but are never compressed, so only what is left
          // after them is codec output. The three slices have to be taken in this order.
          int dataSize = compressedSize
              - v2.getRepetition_levels_byte_length() - v2.getDefinition_levels_byte_length();
          BytesInput repetitionLevels = BytesInput.from(stream.sliceBuffers(v2.getRepetition_levels_byte_length()));
          BytesInput definitionLevels = BytesInput.from(stream.sliceBuffers(v2.getDefinition_levels_byte_length()));
          BytesInput values = BytesInput.from(stream.sliceBuffers(dataSize));
          pages.add(new DataPageV2(v2.getNum_rows(), v2.getNum_nulls(), v2.getNum_values(),
              repetitionLevels, definitionLevels, converter.getEncoding(v2.getEncoding()), values,
              uncompressedSize, converter.fromParquetStatistics(createdBy, v2.getStatistics(), type),
              v2.isIs_compressed()));
          valuesRead += v2.getNum_values();
          break;
        default:
          // A page type this parquet release knows and we do not (an index page, say). Skipping it
          // by its compressed size keeps the walk aligned on the next header, as parquet does.
          stream.skipFully(compressedSize);
      }
    }
    if (valuesRead != chunk.getValueCount()) {
      throw new IOException("Expected " + chunk.getValueCount() + " values in column chunk " + chunk.getPath()
          + " at offset " + chunk.getStartingPos() + " but got " + valuesRead + " over " + pages.size() + " pages");
    }
    return new CachedChunkPageReader(decompressor, pages, dictionaryPage);
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public PageReader getPageReader(ColumnDescriptor descriptor) {
    return readers.get(ColumnPath.get(descriptor.getPath()));
  }

  private static final class CachedChunkPageReader implements PageReader {
    private final BytesInputDecompressor decompressor;
    private final Queue<DataPage> compressedPages;
    private final DictionaryPage compressedDictionaryPage;
    private final long valueCount;

    CachedChunkPageReader(BytesInputDecompressor decompressor, List<DataPage> compressedPages,
        DictionaryPage compressedDictionaryPage) {
      this.decompressor = decompressor;
      this.compressedPages = new ArrayDeque<>(compressedPages);
      this.compressedDictionaryPage = compressedDictionaryPage;
      long count = 0;
      for (DataPage p : compressedPages) {
        count += p.getValueCount();
      }
      this.valueCount = count;
    }

    @Override
    public long getTotalValueCount() {
      return valueCount;
    }

    /**
     * Decompresses one page, on demand. This is where the cached bytes stop being the cache's: the
     * decompressed copy is an ordinary heap/direct buffer that the column reader owns and drops after
     * the batch. Pages the reader never asks for are never decompressed, so a pruned page costs
     * nothing beyond the header parse.
     */
    @Override
    public DataPage readPage() {
      DataPage compressedPage = compressedPages.poll();
      if (compressedPage == null) {
        return null;
      }
      return compressedPage.accept(new DataPage.Visitor<DataPage>() {
        @Override
        public DataPage visit(DataPageV1 page) {
          try {
            return new DataPageV1(decompressor.decompress(page.getBytes(), page.getUncompressedSize()),
                page.getValueCount(), page.getUncompressedSize(), page.getStatistics(),
                page.getRlEncoding(), page.getDlEncoding(), page.getValueEncoding());
          } catch (IOException e) {
            throw new ParquetDecodingException("could not decompress page", e);
          }
        }

        @Override
        public DataPage visit(DataPageV2 page) {
          if (!page.isCompressed()) {
            // Handed straight through, still a view over the cache buffers. Safe for the same reason
            // the whole class is: the batch holds the refs until the consumer finishes the row group.
            return page;
          }
          // getUncompressedSize() covers the levels, which were not compressed, so the codec's own
          // output size is what is left after them.
          int uncompressedSize = Math.toIntExact(page.getUncompressedSize()
              - page.getDefinitionLevels().size() - page.getRepetitionLevels().size());
          BytesInput data;
          try {
            data = decompressor.decompress(page.getData(), uncompressedSize);
          } catch (IOException e) {
            throw new ParquetDecodingException("could not decompress page", e);
          }
          return DataPageV2.uncompressed(page.getRowCount(), page.getNullCount(), page.getValueCount(),
              page.getRepetitionLevels(), page.getDefinitionLevels(), page.getDataEncoding(), data,
              page.getStatistics());
        }
      });
    }

    @Override
    public DictionaryPage readDictionaryPage() {
      if (compressedDictionaryPage == null) {
        return null;
      }
      try {
        BytesInput bytes = decompressor.decompress(
            compressedDictionaryPage.getBytes(), compressedDictionaryPage.getUncompressedSize());
        return new DictionaryPage(bytes, compressedDictionaryPage.getDictionarySize(),
            compressedDictionaryPage.getEncoding());
      } catch (IOException e) {
        throw new ParquetDecodingException("Could not decompress dictionary page", e);
      }
    }
  }
}
