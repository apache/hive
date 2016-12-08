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

package org.apache.orc.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionCodec.Modifier;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFile.CompressionStrategy;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcProto.BloomFilterIndex;
import org.apache.orc.OrcProto.Footer;
import org.apache.orc.OrcProto.Metadata;
import org.apache.orc.OrcProto.PostScript;
import org.apache.orc.OrcProto.Stream.Kind;
import org.apache.orc.OrcProto.StripeFooter;
import org.apache.orc.OrcProto.StripeInformation;
import org.apache.orc.OrcProto.RowIndex.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.CodedOutputStream;

public class PhysicalFsWriter implements PhysicalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(PhysicalFsWriter.class);

  private static final int HDFS_BUFFER_SIZE = 256 * 1024;

  private FSDataOutputStream rawWriter = null;
  // the compressed metadata information outStream
  private OutStream writer = null;
  // a protobuf outStream around streamFactory
  private CodedOutputStream protobufWriter = null;

  private final FileSystem fs;
  private final Path path;
  private final long blockSize;
  private final int bufferSize;
  private final CompressionCodec codec;
  private final double paddingTolerance;
  private final long defaultStripeSize;
  private final CompressionKind compress;
  private final boolean addBlockPadding;
  private final CompressionStrategy compressionStrategy;

  // the streams that make up the current stripe
  private final Map<StreamName, BufferedStream> streams =
    new TreeMap<StreamName, BufferedStream>();

  private long adjustedStripeSize;
  private long headerLength;
  private long stripeStart;
  private int metadataLength;
  private int footerLength;

  public PhysicalFsWriter(FileSystem fs, Path path, int numColumns, OrcFile.WriterOptions opts) {
    this.fs = fs;
    this.path = path;
    this.defaultStripeSize = this.adjustedStripeSize = opts.getStripeSize();
    this.addBlockPadding = opts.getBlockPadding();
    if (opts.isEnforceBufferSize()) {
      this.bufferSize = opts.getBufferSize();
    } else {
      this.bufferSize = getEstimatedBufferSize(defaultStripeSize, numColumns, opts.getBufferSize());
    }
    this.compress = opts.getCompress();
    this.compressionStrategy = opts.getCompressionStrategy();
    codec = createCodec(compress);
    this.paddingTolerance = opts.getPaddingTolerance();
    this.blockSize = opts.getBlockSize();
    LOG.info("ORC writer created for path: {} with stripeSize: {} blockSize: {}" +
        " compression: {} bufferSize: {}", path, defaultStripeSize, blockSize,
        compress, bufferSize);
  }

  @Override
  public void initialize() throws IOException {
    if (rawWriter != null) return;
    rawWriter = fs.create(path, false, HDFS_BUFFER_SIZE,
                          fs.getDefaultReplication(path), blockSize);
    rawWriter.writeBytes(OrcFile.MAGIC);
    headerLength = rawWriter.getPos();
    writer = new OutStream("metadata", bufferSize, codec,
                           new DirectStream(rawWriter));
    protobufWriter = CodedOutputStream.newInstance(writer);
  }

  private void padStripe(long indexSize, long dataSize, int footerSize) throws IOException {
    this.stripeStart = rawWriter.getPos();
    final long currentStripeSize = indexSize + dataSize + footerSize;
    final long available = blockSize - (stripeStart % blockSize);
    final long overflow = currentStripeSize - adjustedStripeSize;
    final float availRatio = (float) available / (float) defaultStripeSize;

    if (availRatio > 0.0f && availRatio < 1.0f
        && availRatio > paddingTolerance) {
      // adjust default stripe size to fit into remaining space, also adjust
      // the next stripe for correction based on the current stripe size
      // and user specified padding tolerance. Since stripe size can overflow
      // the default stripe size we should apply this correction to avoid
      // writing portion of last stripe to next hdfs block.
      double correction = overflow > 0 ? (double) overflow
          / (double) adjustedStripeSize : 0.0;

      // correction should not be greater than user specified padding
      // tolerance
      correction = correction > paddingTolerance ? paddingTolerance
          : correction;

      // adjust next stripe size based on current stripe estimate correction
      adjustedStripeSize = (long) ((1.0f - correction) * (availRatio * defaultStripeSize));
    } else if (availRatio >= 1.0) {
      adjustedStripeSize = defaultStripeSize;
    }

    if (availRatio < paddingTolerance && addBlockPadding) {
      long padding = blockSize - (stripeStart % blockSize);
      byte[] pad = new byte[(int) Math.min(HDFS_BUFFER_SIZE, padding)];
      LOG.info(String.format("Padding ORC by %d bytes (<=  %.2f * %d)", 
          padding, availRatio, defaultStripeSize));
      stripeStart += padding;
      while (padding > 0) {
        int writeLen = (int) Math.min(padding, pad.length);
        rawWriter.write(pad, 0, writeLen);
        padding -= writeLen;
      }
      adjustedStripeSize = defaultStripeSize;
    } else if (currentStripeSize < blockSize
        && (stripeStart % blockSize) + currentStripeSize > blockSize) {
      // even if you don't pad, reset the default stripe size when crossing a
      // block boundary
      adjustedStripeSize = defaultStripeSize;
    }
  }

  /**
   * An output receiver that writes the ByteBuffers to the output stream
   * as they are received.
   */
  private class DirectStream implements OutStream.OutputReceiver {
    private final FSDataOutputStream output;

    DirectStream(FSDataOutputStream output) {
      this.output = output;
    }

    @Override
    public void output(ByteBuffer buffer) throws IOException {
      output.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
    }
  }

  @Override
  public long getPhysicalStripeSize() {
    return adjustedStripeSize;
  }

  @Override
  public boolean isCompressed() {
    return codec != null;
  }


  public static CompressionCodec createCodec(CompressionKind kind) {
    switch (kind) {
      case NONE:
        return null;
      case ZLIB:
        return new ZlibCodec();
      case SNAPPY:
        return new SnappyCodec();
      case LZO:
        try {
          ClassLoader loader = Thread.currentThread().getContextClassLoader();
          if (loader == null) {
            loader = WriterImpl.class.getClassLoader();
          }
          @SuppressWarnings("unchecked")
          Class<? extends CompressionCodec> lzo =
              (Class<? extends CompressionCodec>)
              loader.loadClass("org.apache.hadoop.hive.ql.io.orc.LzoCodec");
          return lzo.newInstance();
        } catch (ClassNotFoundException e) {
          throw new IllegalArgumentException("LZO is not available.", e);
        } catch (InstantiationException e) {
          throw new IllegalArgumentException("Problem initializing LZO", e);
        } catch (IllegalAccessException e) {
          throw new IllegalArgumentException("Insufficient access to LZO", e);
        }
      default:
        throw new IllegalArgumentException("Unknown compression codec: " +
            kind);
    }
  }

  private void writeStripeFooter(StripeFooter footer, long dataSize, long indexSize,
      StripeInformation.Builder dirEntry) throws IOException {
    footer.writeTo(protobufWriter);
    protobufWriter.flush();
    writer.flush();
    dirEntry.setOffset(stripeStart);
    dirEntry.setFooterLength(rawWriter.getPos() - stripeStart - dataSize - indexSize);
  }

  @VisibleForTesting
  public static int getEstimatedBufferSize(long stripeSize, int numColumns,
                                           int bs) {
    // The worst case is that there are 2 big streams per a column and
    // we want to guarantee that each stream gets ~10 buffers.
    // This keeps buffers small enough that we don't get really small stripe
    // sizes.
    int estBufferSize = (int) (stripeSize / (20 * numColumns));
    estBufferSize = getClosestBufferSize(estBufferSize);
    return estBufferSize > bs ? bs : estBufferSize;
  }

  private static int getClosestBufferSize(int estBufferSize) {
    final int kb4 = 4 * 1024;
    final int kb8 = 8 * 1024;
    final int kb16 = 16 * 1024;
    final int kb32 = 32 * 1024;
    final int kb64 = 64 * 1024;
    final int kb128 = 128 * 1024;
    final int kb256 = 256 * 1024;
    if (estBufferSize <= kb4) {
      return kb4;
    } else if (estBufferSize > kb4 && estBufferSize <= kb8) {
      return kb8;
    } else if (estBufferSize > kb8 && estBufferSize <= kb16) {
      return kb16;
    } else if (estBufferSize > kb16 && estBufferSize <= kb32) {
      return kb32;
    } else if (estBufferSize > kb32 && estBufferSize <= kb64) {
      return kb64;
    } else if (estBufferSize > kb64 && estBufferSize <= kb128) {
      return kb128;
    } else {
      return kb256;
    }
  }

  @Override
  public void writeFileMetadata(Metadata.Builder builder) throws IOException {
    long startPosn = rawWriter.getPos();
    Metadata metadata = builder.build();
    metadata.writeTo(protobufWriter);
    protobufWriter.flush();
    writer.flush();
    this.metadataLength = (int) (rawWriter.getPos() - startPosn);
  }

  @Override
  public void writeFileFooter(Footer.Builder builder) throws IOException {
    long bodyLength = rawWriter.getPos() - metadataLength;
    builder.setContentLength(bodyLength);
    builder.setHeaderLength(headerLength);
    long startPosn = rawWriter.getPos();
    Footer footer = builder.build();
    footer.writeTo(protobufWriter);
    protobufWriter.flush();
    writer.flush();
    this.footerLength = (int) (rawWriter.getPos() - startPosn);
  }

  @Override
  public void writePostScript(PostScript.Builder builder) throws IOException {
    builder.setCompression(writeCompressionKind(compress));
    builder.setFooterLength(footerLength);
    builder.setMetadataLength(metadataLength);
    if (compress != CompressionKind.NONE) {
      builder.setCompressionBlockSize(bufferSize);
    }
    PostScript ps = builder.build();
    // need to write this uncompressed
    long startPosn = rawWriter.getPos();
    ps.writeTo(rawWriter);
    long length = rawWriter.getPos() - startPosn;
    if (length > 255) {
      throw new IllegalArgumentException("PostScript too large at " + length);
    }
    rawWriter.writeByte((int)length);
  }

  @Override
  public void close() throws IOException {
    rawWriter.close();
  }

  private OrcProto.CompressionKind writeCompressionKind(CompressionKind kind) {
    switch (kind) {
      case NONE: return OrcProto.CompressionKind.NONE;
      case ZLIB: return OrcProto.CompressionKind.ZLIB;
      case SNAPPY: return OrcProto.CompressionKind.SNAPPY;
      case LZO: return OrcProto.CompressionKind.LZO;
      default:
        throw new IllegalArgumentException("Unknown compression " + kind);
    }
  }

  @Override
  public void flush() throws IOException {
    rawWriter.hflush();
    // TODO: reset?
  }

  @Override
  public long getRawWriterPosition() throws IOException {
    return rawWriter.getPos();
  }

  @Override
  public void appendRawStripe(byte[] stripe, int offset, int length,
      StripeInformation.Builder dirEntry) throws IOException {
    long start = rawWriter.getPos();
    long availBlockSpace = blockSize - (start % blockSize);

    // see if stripe can fit in the current hdfs block, else pad the remaining
    // space in the block
    if (length < blockSize && length > availBlockSpace &&
        addBlockPadding) {
      byte[] pad = new byte[(int) Math.min(HDFS_BUFFER_SIZE, availBlockSpace)];
      LOG.info(String.format("Padding ORC by %d bytes while merging..",
          availBlockSpace));
      start += availBlockSpace;
      while (availBlockSpace > 0) {
        int writeLen = (int) Math.min(availBlockSpace, pad.length);
        rawWriter.write(pad, 0, writeLen);
        availBlockSpace -= writeLen;
      }
    }

    rawWriter.write(stripe);
    dirEntry.setOffset(start);
  }


  /**
   * This class is used to hold the contents of streams as they are buffered.
   * The TreeWriters write to the outStream and the codec compresses the
   * data as buffers fill up and stores them in the output list. When the
   * stripe is being written, the whole stream is written to the file.
   */
  private class BufferedStream implements OutStream.OutputReceiver {
    private final OutStream outStream;
    private final List<ByteBuffer> output = new ArrayList<ByteBuffer>();

    BufferedStream(String name, int bufferSize,
                   CompressionCodec codec) throws IOException {
      outStream = new OutStream(name, bufferSize, codec, this);
    }

    /**
     * Receive a buffer from the compression codec.
     * @param buffer the buffer to save
     */
    @Override
    public void output(ByteBuffer buffer) {
      output.add(buffer);
    }

    /**
     * @return the number of bytes in buffers that are allocated to this stream.
     */
    public long getBufferSize() {
      long result = 0;
      for (ByteBuffer buf: output) {
        result += buf.capacity();
      }
      return outStream.getBufferSize() + result;
    }

    /**
     * Write any saved buffers to the OutputStream if needed, and clears all the buffers.
     */
    public void spillToDiskAndClear() throws IOException {
      if (!outStream.isSuppressed()) {
        for (ByteBuffer buffer: output) {
          rawWriter.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
            buffer.remaining());
        }
      }
      outStream.clear();
      output.clear();
    }

    /**
     * @return The number of bytes that will be written to the output. Assumes the stream writing
     *         into this receiver has already been flushed.
     */
    public long getOutputSize() {
      long result = 0;
      for (ByteBuffer buffer: output) {
        result += buffer.remaining();
      }
      return result;
    }

    @Override
    public String toString() {
      return outStream.toString();
    }
  }

  @Override
  public OutStream getOrCreatePhysicalStream(StreamName name) throws IOException {
    BufferedStream result = streams.get(name);
    if (result == null) {
      EnumSet<Modifier> modifiers = createCompressionModifiers(name.getKind());
      result = new BufferedStream(name.toString(), bufferSize,
          codec == null ? null : codec.modify(modifiers));
      streams.put(name, result);
    }
    return result.outStream;
  }

  private EnumSet<Modifier> createCompressionModifiers(Kind kind) {
    switch (kind) {
      case BLOOM_FILTER:
      case DATA:
      case DICTIONARY_DATA:
        return EnumSet.of(Modifier.TEXT,
            compressionStrategy == CompressionStrategy.SPEED ? Modifier.FAST : Modifier.DEFAULT);
      case LENGTH:
      case DICTIONARY_COUNT:
      case PRESENT:
      case ROW_INDEX:
      case SECONDARY:
        // easily compressed using the fastest modes
        return EnumSet.of(CompressionCodec.Modifier.FASTEST, CompressionCodec.Modifier.BINARY);
      default:
        LOG.warn("Missing ORC compression modifiers for " + kind);
        return null;
    }
  }

  @Override
  public void finalizeStripe(StripeFooter.Builder footerBuilder,
      StripeInformation.Builder dirEntry) throws IOException {
    long indexSize = 0;
    long dataSize = 0;
    for (Map.Entry<StreamName, BufferedStream> pair: streams.entrySet()) {
      BufferedStream receiver = pair.getValue();
      OutStream outStream = receiver.outStream;
      if (!outStream.isSuppressed()) {
        outStream.flush();
        long streamSize = receiver.getOutputSize();
        StreamName name = pair.getKey();
        footerBuilder.addStreams(OrcProto.Stream.newBuilder().setColumn(name.getColumn())
            .setKind(name.getKind()).setLength(streamSize));
        if (StreamName.Area.INDEX == name.getArea()) {
          indexSize += streamSize;
        } else {
          dataSize += streamSize;
        }
      }
    }
    dirEntry.setIndexLength(indexSize).setDataLength(dataSize);

    OrcProto.StripeFooter footer = footerBuilder.build();
    // Do we need to pad the file so the stripe doesn't straddle a block boundary?
    padStripe(indexSize, dataSize, footer.getSerializedSize());

    // write out the data streams
    for (Map.Entry<StreamName, BufferedStream> pair : streams.entrySet()) {
      pair.getValue().spillToDiskAndClear();
    }
    // Write out the footer.
    writeStripeFooter(footer, dataSize, indexSize, dirEntry);
  }

  @Override
  public long estimateMemory() {
    long result = 0;
    for (BufferedStream stream: streams.values()) {
      result += stream.getBufferSize();
    }
    return result;
  }

  @Override
  public void writeIndexStream(StreamName name, Builder rowIndex) throws IOException {
    OutStream stream = getOrCreatePhysicalStream(name);
    rowIndex.build().writeTo(stream);
    stream.flush();
  }

  @Override
  public void writeBloomFilterStream(
      StreamName name, BloomFilterIndex.Builder bloomFilterIndex) throws IOException {
    OutStream stream = getOrCreatePhysicalStream(name);
    bloomFilterIndex.build().writeTo(stream);
    stream.flush();
  }

  @VisibleForTesting
  public OutputStream getStream() throws IOException {
    initialize();
    return rawWriter;
  }
}
