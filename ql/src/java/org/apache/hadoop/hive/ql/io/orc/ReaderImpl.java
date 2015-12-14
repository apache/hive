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
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.orc.impl.BufferChunk;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.impl.ColumnStatisticsImpl;
import org.apache.orc.CompressionCodec;
import org.apache.orc.DataReader;
import org.apache.orc.FileMetaInfo;
import org.apache.orc.FileMetadata;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.MetadataReader;
import org.apache.orc.impl.MetadataReaderImpl;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.ql.io.FileFormatException;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.orc.OrcProto;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.CodedInputStream;

public class ReaderImpl implements Reader {

  private static final Logger LOG = LoggerFactory.getLogger(ReaderImpl.class);

  private static final int DIRECTORY_SIZE_GUESS = 16 * 1024;

  protected final FileSystem fileSystem;
  private final long maxLength;
  protected final Path path;
  protected final org.apache.orc.CompressionKind compressionKind;
  protected final CompressionCodec codec;
  protected final int bufferSize;
  private final List<OrcProto.StripeStatistics> stripeStats;
  private final int metadataSize;
  protected final List<OrcProto.Type> types;
  private final List<OrcProto.UserMetadataItem> userMetadata;
  private final List<OrcProto.ColumnStatistics> fileStats;
  private final List<StripeInformation> stripes;
  protected final int rowIndexStride;
  private final long contentLength, numberOfRows;


  private final ObjectInspector inspector;
  private long deserializedSize = -1;
  protected final Configuration conf;
  private final List<Integer> versionList;
  private final OrcFile.WriterVersion writerVersion;

  //serialized footer - Keeping this around for use by getFileMetaInfo()
  // will help avoid cpu cycles spend in deserializing at cost of increased
  // memory footprint.
  private final ByteBuffer footerByteBuffer;
  // Same for metastore cache - maintains the same background buffer, but includes postscript.
  // This will only be set if the file footer/metadata was read from disk.
  private final ByteBuffer footerMetaAndPsBuffer;

  public static class StripeInformationImpl
      implements StripeInformation {
    private final OrcProto.StripeInformation stripe;

    public StripeInformationImpl(OrcProto.StripeInformation stripe) {
      this.stripe = stripe;
    }

    @Override
    public long getOffset() {
      return stripe.getOffset();
    }

    @Override
    public long getLength() {
      return stripe.getDataLength() + getIndexLength() + getFooterLength();
    }

    @Override
    public long getDataLength() {
      return stripe.getDataLength();
    }

    @Override
    public long getFooterLength() {
      return stripe.getFooterLength();
    }

    @Override
    public long getIndexLength() {
      return stripe.getIndexLength();
    }

    @Override
    public long getNumberOfRows() {
      return stripe.getNumberOfRows();
    }

    @Override
    public String toString() {
      return "offset: " + getOffset() + " data: " + getDataLength() +
        " rows: " + getNumberOfRows() + " tail: " + getFooterLength() +
        " index: " + getIndexLength();
    }
  }

  @Override
  public long getNumberOfRows() {
    return numberOfRows;
  }

  @Override
  public List<String> getMetadataKeys() {
    List<String> result = new ArrayList<String>();
    for(OrcProto.UserMetadataItem item: userMetadata) {
      result.add(item.getName());
    }
    return result;
  }

  @Override
  public ByteBuffer getMetadataValue(String key) {
    for(OrcProto.UserMetadataItem item: userMetadata) {
      if (item.hasName() && item.getName().equals(key)) {
        return item.getValue().asReadOnlyByteBuffer();
      }
    }
    throw new IllegalArgumentException("Can't find user metadata " + key);
  }

  public boolean hasMetadataValue(String key) {
    for(OrcProto.UserMetadataItem item: userMetadata) {
      if (item.hasName() && item.getName().equals(key)) {
        return true;
      }
    }
    return false;
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

  @Override
  public org.apache.orc.CompressionKind getCompressionKind() {
    return compressionKind;
  }

  @Override
  public int getCompressionSize() {
    return bufferSize;
  }

  @Override
  public List<StripeInformation> getStripes() {
    return stripes;
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return inspector;
  }

  @Override
  public long getContentLength() {
    return contentLength;
  }

  @Override
  public List<OrcProto.Type> getTypes() {
    return types;
  }

  @Override
  public OrcFile.Version getFileVersion() {
    for (OrcFile.Version version: OrcFile.Version.values()) {
      if (version.getMajor() == versionList.get(0) &&
          version.getMinor() == versionList.get(1)) {
        return version;
      }
    }
    return OrcFile.Version.V_0_11;
  }

  @Override
  public OrcFile.WriterVersion getWriterVersion() {
    return writerVersion;
  }

  @Override
  public int getRowIndexStride() {
    return rowIndexStride;
  }

  @Override
  public ColumnStatistics[] getStatistics() {
    ColumnStatistics[] result = new ColumnStatistics[types.size()];
    for(int i=0; i < result.length; ++i) {
      result[i] = ColumnStatisticsImpl.deserialize(fileStats.get(i));
    }
    return result;
  }

  /**
   * Ensure this is an ORC file to prevent users from trying to read text
   * files or RC files as ORC files.
   * @param in the file being read
   * @param path the filename for error messages
   * @param psLen the postscript length
   * @param buffer the tail of the file
   * @throws IOException
   */
  static void ensureOrcFooter(FSDataInputStream in,
                                      Path path,
                                      int psLen,
                                      ByteBuffer buffer) throws IOException {
    int len = OrcFile.MAGIC.length();
    if (psLen < len + 1) {
      throw new FileFormatException("Malformed ORC file " + path +
          ". Invalid postscript length " + psLen);
    }
    int offset = buffer.arrayOffset() + buffer.position() + buffer.limit() - 1 - len;
    byte[] array = buffer.array();
    // now look for the magic string at the end of the postscript.
    if (!Text.decode(array, offset, len).equals(OrcFile.MAGIC)) {
      // If it isn't there, this may be the 0.11.0 version of ORC.
      // Read the first 3 bytes of the file to check for the header
      byte[] header = new byte[len];
      in.readFully(0, header, 0, len);
      // if it isn't there, this isn't an ORC file
      if (!Text.decode(header, 0 , len).equals(OrcFile.MAGIC)) {
        throw new FileFormatException("Malformed ORC file " + path +
            ". Invalid postscript.");
      }
    }
  }

  /**
   * Build a version string out of an array.
   * @param version the version number as a list
   * @return the human readable form of the version string
   */
  private static String versionString(List<Integer> version) {
    StringBuilder buffer = new StringBuilder();
    for(int i=0; i < version.size(); ++i) {
      if (i != 0) {
        buffer.append('.');
      }
      buffer.append(version.get(i));
    }
    return buffer.toString();
  }

  /**
   * Check to see if this ORC file is from a future version and if so,
   * warn the user that we may not be able to read all of the column encodings.
   * @param log the logger to write any error message to
   * @param path the data source path for error messages
   * @param version the version of hive that wrote the file.
   */
  static void checkOrcVersion(Logger log, Path path, List<Integer> version) {
    if (version.size() >= 1) {
      int major = version.get(0);
      int minor = 0;
      if (version.size() >= 2) {
        minor = version.get(1);
      }
      if (major > OrcFile.Version.CURRENT.getMajor() ||
          (major == OrcFile.Version.CURRENT.getMajor() &&
           minor > OrcFile.Version.CURRENT.getMinor())) {
        log.warn(path + " was written by a future Hive version " +
                 versionString(version) +
                 ". This file may not be readable by this version of Hive.");
      }
    }
  }

  /**
  * Constructor that let's the user specify additional options.
   * @param path pathname for file
   * @param options options for reading
   * @throws IOException
   */
  public ReaderImpl(Path path, OrcFile.ReaderOptions options) throws IOException {
    FileSystem fs = options.getFilesystem();
    if (fs == null) {
      fs = path.getFileSystem(options.getConfiguration());
    }
    this.fileSystem = fs;
    this.path = path;
    this.conf = options.getConfiguration();
    this.maxLength = options.getMaxLength();

    FileMetadata fileMetadata = options.getFileMetadata();
    if (fileMetadata != null) {
      this.compressionKind = fileMetadata.getCompressionKind();
      this.bufferSize = fileMetadata.getCompressionBufferSize();
      this.codec = WriterImpl.createCodec(compressionKind);
      this.metadataSize = fileMetadata.getMetadataSize();
      this.stripeStats = fileMetadata.getStripeStats();
      this.versionList = fileMetadata.getVersionList();
      this.writerVersion = OrcFile.WriterVersion.from(fileMetadata.getWriterVersionNum());
      this.types = fileMetadata.getTypes();
      this.rowIndexStride = fileMetadata.getRowIndexStride();
      this.contentLength = fileMetadata.getContentLength();
      this.numberOfRows = fileMetadata.getNumberOfRows();
      this.fileStats = fileMetadata.getFileStats();
      this.stripes = fileMetadata.getStripes();
      this.inspector =  OrcStruct.createObjectInspector(0, fileMetadata.getTypes());
      this.footerByteBuffer = null; // not cached and not needed here
      this.userMetadata = null; // not cached and not needed here
      this.footerMetaAndPsBuffer = null;
    } else {
      FileMetaInfo footerMetaData;
      if (options.getFileMetaInfo() != null) {
        footerMetaData = options.getFileMetaInfo();
        this.footerMetaAndPsBuffer = null;
      } else {
        footerMetaData = extractMetaInfoFromFooter(fs, path,
            options.getMaxLength());
        this.footerMetaAndPsBuffer = footerMetaData.footerMetaAndPsBuffer;
      }
      MetaInfoObjExtractor rInfo =
          new MetaInfoObjExtractor(footerMetaData.compressionType,
                                   footerMetaData.bufferSize,
                                   footerMetaData.metadataSize,
                                   footerMetaData.footerBuffer
                                   );
      this.footerByteBuffer = footerMetaData.footerBuffer;
      this.compressionKind = rInfo.compressionKind;
      this.codec = rInfo.codec;
      this.bufferSize = rInfo.bufferSize;
      this.metadataSize = rInfo.metadataSize;
      this.stripeStats = rInfo.metadata.getStripeStatsList();
      this.types = rInfo.footer.getTypesList();
      this.rowIndexStride = rInfo.footer.getRowIndexStride();
      this.contentLength = rInfo.footer.getContentLength();
      this.numberOfRows = rInfo.footer.getNumberOfRows();
      this.userMetadata = rInfo.footer.getMetadataList();
      this.fileStats = rInfo.footer.getStatisticsList();
      this.inspector = rInfo.inspector;
      this.versionList = footerMetaData.versionList;
      this.writerVersion = footerMetaData.writerVersion;
      this.stripes = convertProtoStripesToStripes(rInfo.footer.getStripesList());
    }
  }
  /**
   * Get the WriterVersion based on the ORC file postscript.
   * @param writerVersion the integer writer version
   * @return
   */
  static OrcFile.WriterVersion getWriterVersion(int writerVersion) {
    for(OrcFile.WriterVersion version: OrcFile.WriterVersion.values()) {
      if (version.getId() == writerVersion) {
        return version;
      }
    }
    return OrcFile.WriterVersion.FUTURE;
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

    // figure out the size of the file using the option or filesystem
    long size;
    if (maxFileLength == Long.MAX_VALUE) {
      size = fs.getFileStatus(path).getLen();
    } else {
      size = maxFileLength;
    }

    //read last bytes into buffer to get PostScript
    int readSize = (int) Math.min(size, DIRECTORY_SIZE_GUESS);
    ByteBuffer buffer = ByteBuffer.allocate(readSize);
    assert buffer.position() == 0;
    file.readFully((size - readSize),
        buffer.array(), buffer.arrayOffset(), readSize);
    buffer.position(0);

    //read the PostScript
    //get length of PostScript
    int psLen = buffer.get(readSize - 1) & 0xff;
    ensureOrcFooter(file, path, psLen, buffer);
    int psOffset = readSize - 1 - psLen;
    OrcProto.PostScript ps = extractPostScript(buffer, path, psLen, psOffset);

    int footerSize = (int) ps.getFooterLength();
    int metadataSize = (int) ps.getMetadataLength();
    OrcFile.WriterVersion writerVersion = extractWriterVersion(ps);


    //check if extra bytes need to be read
    ByteBuffer fullFooterBuffer = null;
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

    // remember position for later
    buffer.mark();

    file.close();

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

  private static OrcFile.WriterVersion extractWriterVersion(OrcProto.PostScript ps) {
    return (ps.hasWriterVersion()
        ? getWriterVersion(ps.getWriterVersion()) : OrcFile.WriterVersion.ORIGINAL);
  }

  private static List<StripeInformation> convertProtoStripesToStripes(
      List<OrcProto.StripeInformation> stripes) {
    List<StripeInformation> result = new ArrayList<StripeInformation>(stripes.size());
    for (OrcProto.StripeInformation info : stripes) {
      result.add(new StripeInformationImpl(info));
    }
    return result;
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

      this.compressionKind = org.apache.orc.CompressionKind.valueOf(codecStr);
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
        metadataSize, footerByteBuffer, versionList, writerVersion, footerMetaAndPsBuffer);
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
    return new RecordReaderImpl(this.getStripes(), fileSystem, path,
        options, types, codec, bufferSize, rowIndexStride, conf);
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
  public long getRawDataSize() {
    // if the deserializedSize is not computed, then compute it, else
    // return the already computed size. since we are reading from the footer
    // we don't have to compute deserialized size repeatedly
    if (deserializedSize == -1) {
      List<Integer> indices = Lists.newArrayList();
      for (int i = 0; i < fileStats.size(); ++i) {
        indices.add(i);
      }
      deserializedSize = getRawDataSizeFromColIndices(indices);
    }
    return deserializedSize;
  }

  @Override
  public long getRawDataSizeFromColIndices(List<Integer> colIndices) {
    return getRawDataSizeFromColIndices(colIndices, types, fileStats);
  }

  public static long getRawDataSizeFromColIndices(
      List<Integer> colIndices, List<OrcProto.Type> types,
      List<OrcProto.ColumnStatistics> stats) {
    long result = 0;
    for (int colIdx : colIndices) {
      result += getRawDataSizeOfColumn(colIdx, types, stats);
    }
    return result;
  }

  private static long getRawDataSizeOfColumn(int colIdx, List<OrcProto.Type> types,
      List<OrcProto.ColumnStatistics> stats) {
    OrcProto.ColumnStatistics colStat = stats.get(colIdx);
    long numVals = colStat.getNumberOfValues();
    OrcProto.Type type = types.get(colIdx);

    switch (type.getKind()) {
    case BINARY:
      // old orc format doesn't support binary statistics. checking for binary
      // statistics is not required as protocol buffers takes care of it.
      return colStat.getBinaryStatistics().getSum();
    case STRING:
    case CHAR:
    case VARCHAR:
      // old orc format doesn't support sum for string statistics. checking for
      // existence is not required as protocol buffers takes care of it.

      // ORC strings are deserialized to java strings. so use java data model's
      // string size
      numVals = numVals == 0 ? 1 : numVals;
      int avgStrLen = (int) (colStat.getStringStatistics().getSum() / numVals);
      return numVals * JavaDataModel.get().lengthForStringOfLength(avgStrLen);
    case TIMESTAMP:
      return numVals * JavaDataModel.get().lengthOfTimestamp();
    case DATE:
      return numVals * JavaDataModel.get().lengthOfDate();
    case DECIMAL:
      return numVals * JavaDataModel.get().lengthOfDecimal();
    case DOUBLE:
    case LONG:
      return numVals * JavaDataModel.get().primitive2();
    case FLOAT:
    case INT:
    case SHORT:
    case BOOLEAN:
    case BYTE:
      return numVals * JavaDataModel.get().primitive1();
    default:
      LOG.debug("Unknown primitive category: " + type.getKind());
      break;
    }

    return 0;
  }

  @Override
  public long getRawDataSizeOfColumns(List<String> colNames) {
    List<Integer> colIndices = getColumnIndicesFromNames(colNames);
    return getRawDataSizeFromColIndices(colIndices);
  }

  private List<Integer> getColumnIndicesFromNames(List<String> colNames) {
    // top level struct
    OrcProto.Type type = types.get(0);
    List<Integer> colIndices = Lists.newArrayList();
    List<String> fieldNames = type.getFieldNamesList();
    int fieldIdx = 0;
    for (String colName : colNames) {
      if (fieldNames.contains(colName)) {
        fieldIdx = fieldNames.indexOf(colName);
      } else {
        String s = "Cannot find field for: " + colName + " in ";
        for (String fn : fieldNames) {
          s += fn + ", ";
        }
        LOG.warn(s);
        continue;
      }

      // a single field may span multiple columns. find start and end column
      // index for the requested field
      int idxStart = type.getSubtypes(fieldIdx);

      int idxEnd;

      // if the specified is the last field and then end index will be last
      // column index
      if (fieldIdx + 1 > fieldNames.size() - 1) {
        idxEnd = getLastIdx() + 1;
      } else {
        idxEnd = type.getSubtypes(fieldIdx + 1);
      }

      // if start index and end index are same then the field is a primitive
      // field else complex field (like map, list, struct, union)
      if (idxStart == idxEnd) {
        // simple field
        colIndices.add(idxStart);
      } else {
        // complex fields spans multiple columns
        for (int i = idxStart; i < idxEnd; i++) {
          colIndices.add(i);
        }
      }
    }
    return colIndices;
  }

  private int getLastIdx() {
    Set<Integer> indices = Sets.newHashSet();
    for (OrcProto.Type type : types) {
      indices.addAll(type.getSubtypesList());
    }
    return Collections.max(indices);
  }

  @Override
  public List<OrcProto.StripeStatistics> getOrcProtoStripeStatistics() {
    return stripeStats;
  }

  @Override
  public List<OrcProto.ColumnStatistics> getOrcProtoFileStatistics() {
    return fileStats;
  }

  @Override
  public List<StripeStatistics> getStripeStatistics() {
    List<StripeStatistics> result = Lists.newArrayList();
    for (OrcProto.StripeStatistics ss : stripeStats) {
      result.add(new StripeStatistics(ss.getColStatsList()));
    }
    return result;
  }

  public List<OrcProto.UserMetadataItem> getOrcProtoUserMetadata() {
    return userMetadata;
  }

  @Override
  public MetadataReader metadata() throws IOException {
    return new MetadataReaderImpl(fileSystem, path, codec, bufferSize, types.size());
  }

  @Override
  public List<Integer> getVersionList() {
    return versionList;
  }

  @Override
  public int getMetadataSize() {
    return metadataSize;
  }

  @Override
  public DataReader createDefaultDataReader(boolean useZeroCopy) {
    return RecordReaderUtils.createDefaultDataReader(fileSystem, path, useZeroCopy, codec);
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("ORC Reader(");
    buffer.append(path);
    if (maxLength != -1) {
      buffer.append(", ");
      buffer.append(maxLength);
    }
    buffer.append(")");
    return buffer.toString();
  }
}
