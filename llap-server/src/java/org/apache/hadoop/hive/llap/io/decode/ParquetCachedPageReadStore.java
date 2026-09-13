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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

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
 * {@link PageReadStore} over one row group's cached column-chunk buffers: page headers are parsed in
 * place, page bytes are views of the cache buffers and are decompressed lazily on readPage, mirroring
 * parquet-mr's ParquetFileReader.Chunk.readAllPages and ColumnChunkPageReader (no offset index, no decryption).
 */
class ParquetCachedPageReadStore implements PageReadStore {

  private final Map<ColumnPath, PageReader> readers = new HashMap<>();
  private final long rowCount;

  ParquetCachedPageReadStore(ParquetMetadata footer, ParquetEncodedColumnBatch batch,
      CompressionCodecFactory codecFactory, ParquetMetadataConverter converter) throws IOException {
    BlockMetaData block = footer.getBlocks().get(batch.rowGroupIx);
    this.rowCount = block.getRowCount();
    String createdBy = footer.getFileMetaData().getCreatedBy();
    for (int pc = 0; pc < batch.chunks.length; ++pc) {
      ColumnChunkMetaData chunk = batch.chunks[pc];
      readers.put(chunk.getPath(), readAllPages(chunk, chunkBuffers(batch, pc), createdBy,
          codecFactory.getDecompressor(chunk.getCodec()), converter));
    }
  }

  /** Slices of the cached buffers covering exactly the chunk's byte region, in file order. */
  private static List<ByteBuffer> chunkBuffers(ParquetEncodedColumnBatch batch, int pc) {
    long start = batch.chunks[pc].getStartingPos(), end = start + batch.chunks[pc].getTotalSize();
    List<ByteBuffer> slices = new ArrayList<>(batch.columnBuffers[pc].length);
    for (int i = 0; i < batch.columnBuffers[pc].length; ++i) {
      long offset = batch.bufferOffsets[pc][i];
      long from = Math.max(start, offset), to = Math.min(end, offset + batch.bufferLengths[pc][i]);
      ByteBuffer bb = batch.columnBuffers[pc][i].getByteBufferDup();
      bb.position(bb.position() + (int) (from - offset));
      bb.limit(bb.position() + (int) (to - from));
      slices.add(bb.slice());
    }
    return slices;
  }

  private static PageReader readAllPages(ColumnChunkMetaData chunk, List<ByteBuffer> buffers,
      String createdBy, BytesInputDecompressor decompressor, ParquetMetadataConverter converter)
      throws IOException {
    ByteBufferInputStream stream = ByteBufferInputStream.wrap(buffers);
    PrimitiveType type = chunk.getPrimitiveType();
    List<DataPage> pages = new ArrayList<>();
    DictionaryPage dictionaryPage = null;
    long valuesRead = 0;
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
            return page;
          }
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
