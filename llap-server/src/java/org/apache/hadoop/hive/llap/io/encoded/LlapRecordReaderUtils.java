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

package org.apache.hadoop.hive.llap.io.encoded;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.ql.io.orc.encoded.LlapDataReader;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.BufferChunk;
import org.apache.orc.impl.DataReaderProperties;
import org.apache.orc.impl.DirectDecompressionCodec;
import org.apache.orc.impl.HadoopShims;
import org.apache.orc.impl.HadoopShimsFactory;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcCodecPool;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.RecordReaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Supplier;

public class LlapRecordReaderUtils {

  private static final HadoopShims SHIMS = HadoopShimsFactory.get();
  private static final Logger LOG = LoggerFactory.getLogger(LlapRecordReaderUtils.class);

  static HadoopShims.ZeroCopyReaderShim createZeroCopyShim(FSDataInputStream file, CompressionCodec codec,
      RecordReaderUtils.ByteBufferAllocatorPool pool) throws IOException {
    return codec == null || (codec instanceof DirectDecompressionCodec && ((DirectDecompressionCodec) codec)
        .isAvailable()) ? SHIMS.getZeroCopyReader(file, pool): null;
  }

  public static LlapDataReader createDefaultLlapDataReader(DataReaderProperties properties) {
    return new LlapRecordReaderUtils.DefaultLLapDataReader(properties);
  }

  /**
   * Read the list of ranges from the file.
   * @param file the file to read
   * @param base the base of the stripe
   * @param range the disk ranges within the stripe to read
   * @return the bytes read for each disk range, which is the same length as
   *    ranges
   * @throws IOException
   */
  static DiskRangeList readDiskRanges(FSDataInputStream file,
      HadoopShims.ZeroCopyReaderShim zcr,
      long base,
      DiskRangeList range,
      boolean doForceDirect, int maxChunkLimit) throws IOException {
    if (range == null)
      return null;
    DiskRangeList prev = range.prev;
    if (prev == null) {
      prev = new DiskRangeList.MutateHelper(range);
    }
    while (range != null) {
      if (range.hasData()) {
        range = range.next;
        continue;
      }
      boolean firstRead = true;
      long len = range.getEnd() - range.getOffset();
      long off = range.getOffset();
      while (len > 0) {
        // Stripe could be too large to read fully into a single buffer and
        // will need to be chunked
        int readSize = (len >= maxChunkLimit) ? maxChunkLimit : (int) len;
        ByteBuffer partial;

        // create chunk
        if (zcr != null) {
          if (firstRead) {
            file.seek(base + off);
          }

          partial = zcr.readBuffer(readSize, false);
          readSize = partial.remaining();
        } else {
          // Don't use HDFS ByteBuffer API because it has no readFully, and is
          // buggy and pointless.
          byte[] buffer = new byte[readSize];
          file.readFully((base + off), buffer, 0, buffer.length);
          if (doForceDirect) {
            partial = ByteBuffer.allocateDirect(readSize);
            partial.put(buffer);
            partial.position(0);
            partial.limit(readSize);
          } else {
            partial = ByteBuffer.wrap(buffer);
          }
        }
        BufferChunk bc = new BufferChunk(partial, off);
        if (firstRead) {
          range.replaceSelfWith(bc);
        } else {
          range.insertAfter(bc);
        }
        firstRead = false;
        range = bc;
        len -= readSize;
        off += readSize;
      }
      range = range.next;
    }
    return prev.next;
  }

  /**
   * Plans the list of disk ranges that the given stripe needs to read the
   * indexes. All of the positions are relative to the start of the stripe.
   * @param  fileSchema the schema for the file
   * @param footer the stripe footer
   * @param ignoreNonUtf8BloomFilter should the reader ignore non-utf8
   *                                 encoded bloom filters
   * @param fileIncluded the columns (indexed by file columns) that should be
   *                     read
   * @param sargColumns true for the columns (indexed by file columns) that
   *                    we need bloom filters for
   * @param version the version of the software that wrote the file
   * @param bloomFilterKinds (output) the stream kind of the bloom filters
   * @return a list of merged disk ranges to read
   */
  public static DiskRangeList planIndexReading(TypeDescription fileSchema,
      OrcProto.StripeFooter footer,
      boolean ignoreNonUtf8BloomFilter,
      boolean[] fileIncluded,
      boolean[] sargColumns,
      OrcFile.WriterVersion version,
      OrcProto.Stream.Kind[] bloomFilterKinds) {
    DiskRangeList.CreateHelper result = new DiskRangeList.CreateHelper();
    List<OrcProto.Stream> streams = footer.getStreamsList();
    // figure out which kind of bloom filter we want for each column
    // picks bloom_filter_utf8 if its available, otherwise bloom_filter
    if (sargColumns != null) {
      for (OrcProto.Stream stream : streams) {
        if (stream.hasKind() && stream.hasColumn()) {
          int column = stream.getColumn();
          if (sargColumns[column]) {
            switch (stream.getKind()) {
            case BLOOM_FILTER:
              if (bloomFilterKinds[column] == null &&
                  !(ignoreNonUtf8BloomFilter &&
                      hadBadBloomFilters(fileSchema.findSubtype(column)
                          .getCategory(), version))) {
                bloomFilterKinds[column] = OrcProto.Stream.Kind.BLOOM_FILTER;
              }
              break;
            case BLOOM_FILTER_UTF8:
              bloomFilterKinds[column] = OrcProto.Stream.Kind.BLOOM_FILTER_UTF8;
              break;
            default:
              break;
            }
          }
        }
      }
    }
    long offset = 0;
    for(OrcProto.Stream stream: footer.getStreamsList()) {
      if (stream.hasKind() && stream.hasColumn()) {
        int column = stream.getColumn();
        if (fileIncluded == null || fileIncluded[column]) {
          boolean needStream = false;
          switch (stream.getKind()) {
          case ROW_INDEX:
            needStream = true;
            break;
          case BLOOM_FILTER:
            needStream = bloomFilterKinds[column] == OrcProto.Stream.Kind.BLOOM_FILTER;
            break;
          case BLOOM_FILTER_UTF8:
            needStream = bloomFilterKinds[column] == OrcProto.Stream.Kind.BLOOM_FILTER_UTF8;
            break;
          default:
            // PASS
            break;
          }
          if (needStream) {
            result.addOrMerge(offset, offset + stream.getLength(), true, false);
          }
        }
      }
      offset += stream.getLength();
    }
    return result.get();
  }

  static boolean hadBadBloomFilters(TypeDescription.Category category,
      OrcFile.WriterVersion version) {
    switch(category) {
    case STRING:
    case CHAR:
    case VARCHAR:
      return !version.includes(OrcFile.WriterVersion.HIVE_12055);
    case DECIMAL:
      return true;
    case TIMESTAMP:
      return !version.includes(OrcFile.WriterVersion.ORC_135);
    default:
      return false;
    }
  }

  private static class DefaultLLapDataReader implements LlapDataReader {
    private FSDataInputStream file;
    private RecordReaderUtils.ByteBufferAllocatorPool pool;
    private HadoopShims.ZeroCopyReaderShim zcr = null;
    private final Supplier<FileSystem> fileSystemSupplier;
    private final Path path;
    private final boolean useZeroCopy;
    private CompressionCodec codec;
    private final int bufferSize;
    private CompressionKind compressionKind;
    private final int maxDiskRangeChunkLimit;
    private boolean isOpen = false;

    private DefaultLLapDataReader(DataReaderProperties properties) {
      this.fileSystemSupplier = properties.getFileSystemSupplier();
      this.path = properties.getPath();
      this.file = properties.getFile();
      this.useZeroCopy = properties.getZeroCopy();
      this.codec = properties.getCompression() == null ? null : properties.getCompression().getCodec();
      this.compressionKind = codec == null ? CompressionKind.NONE : codec.getKind();
      this.bufferSize = codec == null ? 0 : properties.getCompression().getBufferSize();
      this.maxDiskRangeChunkLimit = properties.getMaxDiskRangeChunkLimit();
    }

    @Override
    public void open() throws IOException {
      if (file == null) {
        this.file = fileSystemSupplier.get().open(path);
      }
      if (useZeroCopy) {
        // ZCR only uses codec for boolean checks.
        pool = new RecordReaderUtils.ByteBufferAllocatorPool();
        zcr = LlapRecordReaderUtils.createZeroCopyShim(file, codec, pool);
      } else {
        zcr = null;
      }
      isOpen = true;
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
      if (!isOpen) {
        open();
      }
      if (footer == null) {
        footer = readStripeFooter(stripe);
      }
      if (indexes == null) {
        indexes = new OrcProto.RowIndex[fileSchema.getMaximumId() + 1];
      }
      if (bloomFilterKinds == null) {
        bloomFilterKinds = new OrcProto.Stream.Kind[fileSchema.getMaximumId() + 1];
      }
      if (bloomFilterIndices == null) {
        bloomFilterIndices = new OrcProto.BloomFilterIndex[fileSchema.getMaximumId() + 1];
      }
      DiskRangeList ranges = planIndexReading(fileSchema, footer,
          ignoreNonUtf8BloomFilter, included, sargColumns, version,
          bloomFilterKinds);
      ranges = readDiskRanges(file, zcr, stripe.getOffset(), ranges, false, maxDiskRangeChunkLimit);
      long offset = 0;
      DiskRangeList range = ranges;
      for(OrcProto.Stream stream: footer.getStreamsList()) {
        // advance to find the next range
        while (range != null && range.getEnd() <= offset) {
          range = range.next;
        }
        // no more ranges, so we are done
        if (range == null) {
          break;
        }
        InStream.StreamOptions compression = null;
        try (CompressionCodec codec = OrcCodecPool.getCodec(compressionKind)) {
          if (codec != null) {
            compression = InStream.options().withCodec(codec).withBufferSize(bufferSize);
          }
        }

        int column = stream.getColumn();
        if (stream.hasKind() && range.getOffset() <= offset) {
          switch (stream.getKind()) {
          case ROW_INDEX:
            if (included == null || included[column]) {
              ByteBuffer bb = range.getData().duplicate();
              bb.position((int) (offset - range.getOffset()));
              bb.limit((int) (bb.position() + stream.getLength()));
              indexes[column] = OrcProto.RowIndex.parseFrom(
                  InStream.createCodedInputStream(
                      InStream.create("index",
                      new BufferChunk(bb, 0), 0,
                      stream.getLength(), compression)));
            }
            break;
          case BLOOM_FILTER:
          case BLOOM_FILTER_UTF8:
            if (sargColumns != null && sargColumns[column]) {
              ByteBuffer bb = range.getData().duplicate();
              bb.position((int) (offset - range.getOffset()));
              bb.limit((int) (bb.position() + stream.getLength()));
              bloomFilterIndices[column] = OrcProto.BloomFilterIndex.parseFrom(
                  InStream.createCodedInputStream(
                      InStream.create("bloom_filter",
                      new BufferChunk(bb, 0), 0,
                      stream.getLength(), compression)));
            }
            break;
          default:
            break;
          }
        }
        offset += stream.getLength();
      }
      return new OrcIndex(indexes, bloomFilterKinds, bloomFilterIndices);
    }

    @Override
    public OrcProto.StripeFooter readStripeFooter(StripeInformation stripe) throws IOException {
      if (!isOpen) {
        open();
      }
      long offset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
      int tailLength = (int) stripe.getFooterLength();

      InStream.StreamOptions compression = null;
      try (CompressionCodec codec = OrcCodecPool.getCodec(compressionKind)) {
        if (codec != null) {
          compression = InStream.options().withCodec(codec).withBufferSize(bufferSize);
        }
      }

      // read the footer
      ByteBuffer tailBuf = ByteBuffer.allocate(tailLength);
      file.readFully(offset, tailBuf.array(), tailBuf.arrayOffset(), tailLength);
      return OrcProto.StripeFooter.parseFrom(
          InStream.createCodedInputStream(
              InStream.create("footer",
                  new BufferChunk(tailBuf, 0), 0,
                  tailLength, compression)));
    }

    @Override
    public DiskRangeList readFileData(
        DiskRangeList range, long baseOffset, boolean doForceDirect) throws IOException {
      return readDiskRanges(file, zcr, baseOffset, range, doForceDirect, maxDiskRangeChunkLimit);
    }

    @Override
    public void close() throws IOException {
      if (codec != null) {
        OrcCodecPool.returnCodec(compressionKind, codec);
        codec = null;
      }
      if (pool != null) {
        pool.clear();
      }
      // close both zcr and file
      try (HadoopShims.ZeroCopyReaderShim myZcr = zcr) {
        if (file != null) {
          file.close();
          file = null;
        }
      }
    }

    @Override
    public boolean isTrackingDiskRanges() {
      return zcr != null;
    }

    @Override
    public void releaseBuffer(ByteBuffer buffer) {
      zcr.releaseBuffer(buffer);
    }

    @Override
    public LlapDataReader clone() {
      if (this.file != null) {
        // We should really throw here, but that will cause failures in Hive.
        // While Hive uses clone, just log a warning.
        LOG.warn("Cloning an opened DataReader; the stream will be reused and closed twice");
      }
      try {
        DefaultLLapDataReader clone = (DefaultLLapDataReader) super.clone();
        if (codec != null) {
          // Make sure we don't share the same codec between two readers.
          clone.codec = OrcCodecPool.getCodec(clone.compressionKind);
        }
        return clone;
      } catch (CloneNotSupportedException e) {
        throw new UnsupportedOperationException("uncloneable", e);
      }
    }

    @Override
    public CompressionCodec getCompressionCodec() {
      return codec;
    }
  }
}
