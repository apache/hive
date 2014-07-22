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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileSplit;



/**
 * OrcFileSplit. Holds file meta info
 *
 */
public class OrcSplit extends FileSplit {
  private ReaderImpl.FileMetaInfo fileMetaInfo;
  private boolean hasFooter;
  private boolean isOriginal;
  private boolean hasBase;
  private final List<Long> deltas = new ArrayList<Long>();

  static final int BASE_FLAG = 4;
  static final int ORIGINAL_FLAG = 2;
  static final int FOOTER_FLAG = 1;

  protected OrcSplit(){
    //The FileSplit() constructor in hadoop 0.20 and 1.x is package private so can't use it.
    //This constructor is used to create the object and then call readFields()
    // so just pass nulls to this super constructor.
    super(null, 0, 0, (String[]) null);
  }

  public OrcSplit(Path path, long offset, long length, String[] hosts,
      ReaderImpl.FileMetaInfo fileMetaInfo, boolean isOriginal, boolean hasBase,
      List<Long> deltas) {
    super(path, offset, length, hosts);
    this.fileMetaInfo = fileMetaInfo;
    hasFooter = this.fileMetaInfo != null;
    this.isOriginal = isOriginal;
    this.hasBase = hasBase;
    this.deltas.addAll(deltas);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    //serialize path, offset, length using FileSplit
    super.write(out);

    int flags = (hasBase ? BASE_FLAG : 0) |
        (isOriginal ? ORIGINAL_FLAG : 0) |
        (hasFooter ? FOOTER_FLAG : 0);
    out.writeByte(flags);
    out.writeInt(deltas.size());
    for(Long delta: deltas) {
      out.writeLong(delta);
    }
    if (hasFooter) {
      // serialize FileMetaInfo fields
      Text.writeString(out, fileMetaInfo.compressionType);
      WritableUtils.writeVInt(out, fileMetaInfo.bufferSize);
      WritableUtils.writeVInt(out, fileMetaInfo.metadataSize);

      // serialize FileMetaInfo field footer
      ByteBuffer footerBuff = fileMetaInfo.footerBuffer;
      footerBuff.reset();

      // write length of buffer
      WritableUtils.writeVInt(out, footerBuff.limit() - footerBuff.position());
      out.write(footerBuff.array(), footerBuff.position(),
          footerBuff.limit() - footerBuff.position());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    //deserialize path, offset, length using FileSplit
    super.readFields(in);

    byte flags = in.readByte();
    hasFooter = (FOOTER_FLAG & flags) != 0;
    isOriginal = (ORIGINAL_FLAG & flags) != 0;
    hasBase = (BASE_FLAG & flags) != 0;

    deltas.clear();
    int numDeltas = in.readInt();
    for(int i=0; i < numDeltas; i++) {
      deltas.add(in.readLong());
    }
    if (hasFooter) {
      // deserialize FileMetaInfo fields
      String compressionType = Text.readString(in);
      int bufferSize = WritableUtils.readVInt(in);
      int metadataSize = WritableUtils.readVInt(in);

      // deserialize FileMetaInfo field footer
      int footerBuffSize = WritableUtils.readVInt(in);
      ByteBuffer footerBuff = ByteBuffer.allocate(footerBuffSize);
      in.readFully(footerBuff.array(), 0, footerBuffSize);

      fileMetaInfo = new ReaderImpl.FileMetaInfo(compressionType, bufferSize,
          metadataSize, footerBuff);
    }
  }

  ReaderImpl.FileMetaInfo getFileMetaInfo(){
    return fileMetaInfo;
  }

  public boolean hasFooter() {
    return hasFooter;
  }

  public boolean isOriginal() {
    return isOriginal;
  }

  public boolean hasBase() {
    return hasBase;
  }

  public List<Long> getDeltas() {
    return deltas;
  }
}
