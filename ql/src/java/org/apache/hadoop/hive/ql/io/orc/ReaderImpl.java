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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.orc.impl.BufferChunk;
import org.apache.orc.CompressionCodec;
import org.apache.orc.FileMetaInfo;
import org.apache.orc.FileMetadata;
import org.apache.orc.impl.InStream;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.orc.OrcProto;

import com.google.common.collect.Lists;
import com.google.protobuf.CodedInputStream;

public class ReaderImpl extends org.apache.orc.impl.ReaderImpl
                        implements Reader {

  private static final Logger LOG = LoggerFactory.getLogger(ReaderImpl.class);

  private static final int DIRECTORY_SIZE_GUESS = 16 * 1024;

  private final ObjectInspector inspector;

  //serialized footer - Keeping this around for use by getFileMetaInfo()
  // will help avoid cpu cycles spend in deserializing at cost of increased
  // memory footprint.
  private ByteBuffer footerByteBuffer;
  // Same for metastore cache - maintains the same background buffer, but includes postscript.
  // This will only be set if the file footer/metadata was read from disk.
  private ByteBuffer footerMetaAndPsBuffer;

  @Override
  public ObjectInspector getObjectInspector() {
    return inspector;
  }

  @Override
  public org.apache.hadoop.hive.ql.io.orc.CompressionKind getCompression() {
    for (CompressionKind value: org.apache.hadoop.hive.ql.io.orc.CompressionKind.values()) {
      if (value.getUnderlying() == compressionKind) {
        return value;
      }
    }
    throw new IllegalArgumentException("Unknown compression kind " +
        compressionKind);
  }

  /**
  * Constructor that let's the user specify additional options.
   * @param path pathname for file
   * @param options options for reading
   * @throws IOException
   */
  public ReaderImpl(Path path,
                    OrcFile.ReaderOptions options) throws IOException {
    super(path, options);
    FileMetadata fileMetadata = options.getFileMetadata();
    if (fileMetadata != null) {
      this.inspector =  OrcStruct.createObjectInspector(0, fileMetadata.getTypes());
    } else {
      FileMetaInfo footerMetaData;
      if (options.getFileMetaInfo() != null) {
        footerMetaData = options.getFileMetaInfo();
      } else {
        footerMetaData = extractMetaInfoFromFooter(fileSystem, path,
            options.getMaxLength());
      }
      this.footerMetaAndPsBuffer = footerMetaData.footerMetaAndPsBuffer;
      MetaInfoObjExtractor rInfo =
          new MetaInfoObjExtractor(footerMetaData.compressionType,
                                   footerMetaData.bufferSize,
                                   footerMetaData.metadataSize,
                                   footerMetaData.footerBuffer
                                   );
      this.footerByteBuffer = footerMetaData.footerBuffer;
      this.inspector = rInfo.inspector;
    }
  }

  /** Extracts the necessary metadata from an externally store buffer (fullFooterBuffer). */
  public static FooterInfo extractMetaInfoFromFooter(
      ByteBuffer bb, Path srcPath) throws IOException {
    // Read the PostScript. Be very careful as some parts of this historically use bb position
    // and some use absolute offsets that have to take position into account.
    int baseOffset = bb.position();
    int lastByteAbsPos = baseOffset + bb.remaining() - 1;
    int psLen = bb.get(lastByteAbsPos) & 0xff;
    int psAbsPos = lastByteAbsPos - psLen;
    OrcProto.PostScript ps = extractPostScript(bb, srcPath, psLen, psAbsPos);
    assert baseOffset == bb.position();

    // Extract PS information.
    int footerSize = (int)ps.getFooterLength(), metadataSize = (int)ps.getMetadataLength(),
        footerAbsPos = psAbsPos - footerSize, metadataAbsPos = footerAbsPos - metadataSize;
    String compressionType = ps.getCompression().toString();
    CompressionCodec codec =
        WriterImpl.createCodec(org.apache.orc.CompressionKind.valueOf
            (compressionType));
    int bufferSize = (int)ps.getCompressionBlockSize();
    bb.position(metadataAbsPos);
    bb.mark();

    // Extract metadata and footer.
    OrcProto.Metadata metadata = extractMetadata(
        bb, metadataAbsPos, metadataSize, codec, bufferSize);
    List<StripeStatistics> stats = new ArrayList<>(metadata.getStripeStatsCount());
    for (OrcProto.StripeStatistics ss : metadata.getStripeStatsList()) {
      stats.add(new StripeStatistics(ss.getColStatsList()));
    }
    OrcProto.Footer footer = extractFooter(bb, footerAbsPos, footerSize, codec, bufferSize);
    bb.position(metadataAbsPos);
    bb.limit(psAbsPos);
    // TODO: do we need footer buffer here? FileInfo/FileMetaInfo is a mess...
    FileMetaInfo fmi = new FileMetaInfo(
        compressionType, bufferSize, metadataSize, bb, extractWriterVersion(ps));
    return new FooterInfo(stats, footer, fmi);
  }

  private static OrcProto.Footer extractFooter(ByteBuffer bb, int footerAbsPos,
      int footerSize, CompressionCodec codec, int bufferSize) throws IOException {
    bb.position(footerAbsPos);
    bb.limit(footerAbsPos + footerSize);
    return OrcProto.Footer.parseFrom(InStream.createCodedInputStream("footer",
        Lists.<DiskRange>newArrayList(new BufferChunk(bb, 0)), footerSize, codec, bufferSize));
  }

  private static OrcProto.Metadata extractMetadata(ByteBuffer bb, int metadataAbsPos,
      int metadataSize, CompressionCodec codec, int bufferSize) throws IOException {
    bb.position(metadataAbsPos);
    bb.limit(metadataAbsPos + metadataSize);
    return OrcProto.Metadata.parseFrom(InStream.createCodedInputStream("metadata",
        Lists.<DiskRange>newArrayList(new BufferChunk(bb, 0)), metadataSize, codec, bufferSize));
  }

  private static OrcProto.PostScript extractPostScript(ByteBuffer bb, Path path,
      int psLen, int psAbsOffset) throws IOException {
    // TODO: when PB is upgraded to 2.6, newInstance(ByteBuffer) method should be used here.
    assert bb.hasArray();
    CodedInputStream in = CodedInputStream.newInstance(
        bb.array(), bb.arrayOffset() + psAbsOffset, psLen);
    OrcProto.PostScript ps = OrcProto.PostScript.parseFrom(in);
    checkOrcVersion(LOG, path, ps.getVersionList());

    // Check compression codec.
    switch (ps.getCompression()) {
      case NONE:
        break;
      case ZLIB:
        break;
      case SNAPPY:
        break;
      case LZO:
        break;
      default:
        throw new IllegalArgumentException("Unknown compression");
    }
    return ps;
  }

  private static FileMetaInfo extractMetaInfoFromFooter(FileSystem fs,
                                                        Path path,
                                                        long maxFileLength
                                                        ) throws IOException {
    FSDataInputStream file = fs.open(path);
    ByteBuffer buffer = null, fullFooterBuffer = null;
    OrcProto.PostScript ps = null;
    OrcFile.WriterVersion writerVersion = null;
    try {
      // figure out the size of the file using the option or filesystem
      long size;
      if (maxFileLength == Long.MAX_VALUE) {
        size = fs.getFileStatus(path).getLen();
      } else {
        size = maxFileLength;
      }

      //read last bytes into buffer to get PostScript
      int readSize = (int) Math.min(size, DIRECTORY_SIZE_GUESS);
      buffer = ByteBuffer.allocate(readSize);
      assert buffer.position() == 0;
      file.readFully((size - readSize),
          buffer.array(), buffer.arrayOffset(), readSize);
      buffer.position(0);

      //read the PostScript
      //get length of PostScript
      int psLen = buffer.get(readSize - 1) & 0xff;
      ensureOrcFooter(file, path, psLen, buffer);
      int psOffset = readSize - 1 - psLen;
      ps = extractPostScript(buffer, path, psLen, psOffset);

      int footerSize = (int) ps.getFooterLength();
      int metadataSize = (int) ps.getMetadataLength();
      writerVersion = extractWriterVersion(ps);

      //check if extra bytes need to be read
      int extra = Math.max(0, psLen + 1 + footerSize + metadataSize - readSize);
      if (extra > 0) {
        //more bytes need to be read, seek back to the right place and read extra bytes
        ByteBuffer extraBuf = ByteBuffer.allocate(extra + readSize);
        file.readFully((size - readSize - extra), extraBuf.array(),
            extraBuf.arrayOffset() + extraBuf.position(), extra);
        extraBuf.position(extra);
        //append with already read bytes
        extraBuf.put(buffer);
        buffer = extraBuf;
        buffer.position(0);
        fullFooterBuffer = buffer.slice();
        buffer.limit(footerSize + metadataSize);
      } else {
        //footer is already in the bytes in buffer, just adjust position, length
        buffer.position(psOffset - footerSize - metadataSize);
        fullFooterBuffer = buffer.slice();
        buffer.limit(psOffset);
      }

      // remember position for later TODO: what later? this comment is useless
      buffer.mark();
    } finally {
      try {
        file.close();
      } catch (IOException ex) {
        LOG.error("Failed to close the file after another error", ex);
      }
    }

    return new FileMetaInfo(
        ps.getCompression().toString(),
        (int) ps.getCompressionBlockSize(),
        (int) ps.getMetadataLength(),
        buffer,
        ps.getVersionList(),
        writerVersion,
        fullFooterBuffer
        );
  }

  /**
   * MetaInfoObjExtractor - has logic to create the values for the fields in ReaderImpl
   *  from serialized fields.
   * As the fields are final, the fields need to be initialized in the constructor and
   *  can't be done in some helper function. So this helper class is used instead.
   *
   */
  private static class MetaInfoObjExtractor{
    final org.apache.orc.CompressionKind compressionKind;
    final CompressionCodec codec;
    final int bufferSize;
    final int metadataSize;
    final OrcProto.Metadata metadata;
    final OrcProto.Footer footer;
    final ObjectInspector inspector;

    MetaInfoObjExtractor(String codecStr, int bufferSize, int metadataSize, 
        ByteBuffer footerBuffer) throws IOException {

      this.compressionKind = org.apache.orc.CompressionKind.valueOf(codecStr.toUpperCase());
      this.bufferSize = bufferSize;
      this.codec = WriterImpl.createCodec(compressionKind);
      this.metadataSize = metadataSize;

      int position = footerBuffer.position();
      int footerBufferSize = footerBuffer.limit() - footerBuffer.position() - metadataSize;

      this.metadata = extractMetadata(footerBuffer, position, metadataSize, codec, bufferSize);
      this.footer = extractFooter(
          footerBuffer, position + metadataSize, footerBufferSize, codec, bufferSize);

      footerBuffer.position(position);
      this.inspector = OrcStruct.createObjectInspector(0, footer.getTypesList());
    }
  }

  public FileMetaInfo getFileMetaInfo() {
    return new FileMetaInfo(compressionKind.toString(), bufferSize,
        getMetadataSize(), footerByteBuffer, getVersionList(),
        getWriterVersion(), footerMetaAndPsBuffer);
  }

  /** Same as FileMetaInfo, but with extra fields. FileMetaInfo is serialized for splits
   * and so we don't just add fields to it, it's already messy and confusing. */
  public static final class FooterInfo {
    private final OrcProto.Footer footer;
    private final List<StripeStatistics> metadata;
    private final List<StripeInformation> stripes;
    private final FileMetaInfo fileMetaInfo;

    private FooterInfo(
        List<StripeStatistics> metadata, OrcProto.Footer footer, FileMetaInfo fileMetaInfo) {
      this.metadata = metadata;
      this.footer = footer;
      this.fileMetaInfo = fileMetaInfo;
      this.stripes = convertProtoStripesToStripes(footer.getStripesList());
    }

    public OrcProto.Footer getFooter() {
      return footer;
    }

    public List<StripeStatistics> getMetadata() {
      return metadata;
    }

    public FileMetaInfo getFileMetaInfo() {
      return fileMetaInfo;
    }

    public List<StripeInformation> getStripes() {
      return stripes;
    }
  }

  @Override
  public ByteBuffer getSerializedFileFooter() {
    return footerMetaAndPsBuffer;
  }

  @Override
  public RecordReader rows() throws IOException {
    return rowsOptions(new Options());
  }

  @Override
  public RecordReader rowsOptions(Options options) throws IOException {
    LOG.info("Reading ORC rows from " + path + " with " + options);
    boolean[] include = options.getInclude();
    // if included columns is null, then include all columns
    if (include == null) {
      include = new boolean[types.size()];
      Arrays.fill(include, true);
      options.include(include);
    }
    return new RecordReaderImpl(this, options);
  }


  @Override
  public RecordReader rows(boolean[] include) throws IOException {
    return rowsOptions(new Options().include(include));
  }

  @Override
  public RecordReader rows(long offset, long length, boolean[] include
                           ) throws IOException {
    return rowsOptions(new Options().include(include).range(offset, length));
  }

  @Override
  public RecordReader rows(long offset, long length, boolean[] include,
                           SearchArgument sarg, String[] columnNames
                           ) throws IOException {
    return rowsOptions(new Options().include(include).range(offset, length)
        .searchArgument(sarg, columnNames));
  }

  @Override
  public String toString() {
    return "Hive " + super.toString();
  }
}
