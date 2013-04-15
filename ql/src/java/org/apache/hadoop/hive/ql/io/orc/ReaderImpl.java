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

import com.google.protobuf.CodedInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

final class ReaderImpl implements Reader {

  private static final int DIRECTORY_SIZE_GUESS = 16 * 1024;

  private final FileSystem fileSystem;
  private final Path path;
  private final CompressionKind compressionKind;
  private final CompressionCodec codec;
  private final int bufferSize;
  private final OrcProto.Footer footer;
  private final ObjectInspector inspector;

  private static class StripeInformationImpl
      implements StripeInformation {
    private final OrcProto.StripeInformation stripe;

    StripeInformationImpl(OrcProto.StripeInformation stripe) {
      this.stripe = stripe;
    }

    @Override
    public long getOffset() {
      return stripe.getOffset();
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
    return footer.getNumberOfRows();
  }

  @Override
  public Iterable<String> getMetadataKeys() {
    List<String> result = new ArrayList<String>();
    for(OrcProto.UserMetadataItem item: footer.getMetadataList()) {
      result.add(item.getName());
    }
    return result;
  }

  @Override
  public ByteBuffer getMetadataValue(String key) {
    for(OrcProto.UserMetadataItem item: footer.getMetadataList()) {
      if (item.hasName() && item.getName().equals(key)) {
        return item.getValue().asReadOnlyByteBuffer();
      }
    }
    throw new IllegalArgumentException("Can't find user metadata " + key);
  }

  @Override
  public CompressionKind getCompression() {
    return compressionKind;
  }

  @Override
  public int getCompressionSize() {
    return bufferSize;
  }

  @Override
  public Iterable<StripeInformation> getStripes() {
    return new Iterable<org.apache.hadoop.hive.ql.io.orc.StripeInformation>(){

      @Override
      public Iterator<org.apache.hadoop.hive.ql.io.orc.StripeInformation> iterator() {
        return new Iterator<org.apache.hadoop.hive.ql.io.orc.StripeInformation>(){
          private final Iterator<OrcProto.StripeInformation> inner =
            footer.getStripesList().iterator();

          @Override
          public boolean hasNext() {
            return inner.hasNext();
          }

          @Override
          public org.apache.hadoop.hive.ql.io.orc.StripeInformation next() {
            return new StripeInformationImpl(inner.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException("remove unsupported");
          }
        };
      }
    };
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return inspector;
  }

  @Override
  public long getContentLength() {
    return footer.getContentLength();
  }

  @Override
  public List<OrcProto.Type> getTypes() {
    return footer.getTypesList();
  }

  @Override
  public int getRowIndexStride() {
    return footer.getRowIndexStride();
  }

  @Override
  public ColumnStatistics[] getStatistics() {
    ColumnStatistics[] result = new ColumnStatistics[footer.getTypesCount()];
    for(int i=0; i < result.length; ++i) {
      result[i] = ColumnStatisticsImpl.deserialize(footer.getStatistics(i));
    }
    return result;
  }

  ReaderImpl(FileSystem fs, Path path) throws IOException {
    this.fileSystem = fs;
    this.path = path;
    FSDataInputStream file = fs.open(path);
    long size = fs.getFileStatus(path).getLen();
    int readSize = (int) Math.min(size, DIRECTORY_SIZE_GUESS);
    file.seek(size - readSize);
    ByteBuffer buffer = ByteBuffer.allocate(readSize);
    file.readFully(buffer.array(), buffer.arrayOffset() + buffer.position(),
      buffer.remaining());
    int psLen = buffer.get(readSize - 1);
    int psOffset = readSize - 1 - psLen;
    CodedInputStream in = CodedInputStream.newInstance(buffer.array(),
      buffer.arrayOffset() + psOffset, psLen);
    OrcProto.PostScript ps = OrcProto.PostScript.parseFrom(in);
    int footerSize = (int) ps.getFooterLength();
    bufferSize = (int) ps.getCompressionBlockSize();
    switch (ps.getCompression()) {
      case NONE:
        compressionKind = CompressionKind.NONE;
        break;
      case ZLIB:
        compressionKind = CompressionKind.ZLIB;
        break;
      case SNAPPY:
        compressionKind = CompressionKind.SNAPPY;
        break;
      case LZO:
        compressionKind = CompressionKind.LZO;
        break;
      default:
        throw new IllegalArgumentException("Unknown compression");
    }
    codec = WriterImpl.createCodec(compressionKind);
    int extra = Math.max(0, psLen + 1 + footerSize - readSize);
    if (extra > 0) {
      file.seek(size - readSize - extra);
      ByteBuffer extraBuf = ByteBuffer.allocate(extra + readSize);
      file.readFully(extraBuf.array(),
        extraBuf.arrayOffset() + extraBuf.position(), extra);
      extraBuf.position(extra);
      extraBuf.put(buffer);
      buffer = extraBuf;
      buffer.position(0);
      buffer.limit(footerSize);
    } else {
      buffer.position(psOffset - footerSize);
      buffer.limit(psOffset);
    }
    InputStream instream = InStream.create("footer", buffer, codec, bufferSize);
    footer = OrcProto.Footer.parseFrom(instream);
    inspector = OrcStruct.createObjectInspector(0, footer.getTypesList());
    file.close();
  }

  @Override
  public RecordReader rows(boolean[] include) throws IOException {
    return rows(0, Long.MAX_VALUE, include);
  }

  @Override
  public RecordReader rows(long offset, long length, boolean[] include
                           ) throws IOException {
    return new RecordReaderImpl(this.getStripes(), fileSystem,  path, offset,
      length, footer.getTypesList(), codec, bufferSize,
      include, footer.getRowIndexStride());
  }

}
