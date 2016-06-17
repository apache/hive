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
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileSplit;



/**
 * OrcFileSplit. Holds file meta info
 *
 */
public class OrcSplit extends FileSplit {
  private OrcTail orcTail;
  private boolean hasFooter;
  private boolean isOriginal;
  private boolean hasBase;
  private final List<AcidInputFormat.DeltaMetaData> deltas = new ArrayList<>();
  private OrcFile.WriterVersion writerVersion;
  private long projColsUncompressedSize;
  private long fileLen;

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
      OrcTail orcTail, boolean isOriginal, boolean hasBase,
      List<AcidInputFormat.DeltaMetaData> deltas, long projectedDataSize, long fileLen) {
    super(path, offset, length, hosts);
    this.orcTail = orcTail;
    hasFooter = this.orcTail != null;
    this.isOriginal = isOriginal;
    this.hasBase = hasBase;
    this.deltas.addAll(deltas);
    this.projColsUncompressedSize = projectedDataSize;
    // setting file length to Long.MAX_VALUE will let orc reader read file length from file system
    this.fileLen = fileLen <= 0 ? Long.MAX_VALUE : fileLen;
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
    for(AcidInputFormat.DeltaMetaData delta: deltas) {
      delta.write(out);
    }
    if (hasFooter) {
      OrcProto.FileTail fileTail = orcTail.getMinimalFileTail();
      byte[] tailBuffer = fileTail.toByteArray();
      int tailLen = tailBuffer.length;
      WritableUtils.writeVInt(out, tailLen);
      out.write(tailBuffer);
    }
    out.writeLong(fileLen);
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
      AcidInputFormat.DeltaMetaData dmd = new AcidInputFormat.DeltaMetaData();
      dmd.readFields(in);
      deltas.add(dmd);
    }
    if (hasFooter) {
      int tailLen = WritableUtils.readVInt(in);
      byte[] tailBuffer = new byte[tailLen];
      in.readFully(tailBuffer);
      OrcProto.FileTail fileTail = OrcProto.FileTail.parseFrom(tailBuffer);
      orcTail = new OrcTail(fileTail, null);
    }
    fileLen = in.readLong();
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

  public List<AcidInputFormat.DeltaMetaData> getDeltas() {
    return deltas;
  }

  /**
   * If this method returns true, then for sure it is ACID.
   * However, if it returns false.. it could be ACID or non-ACID.
   * @return
   */
  public boolean isAcid() {
    return hasBase || deltas.size() > 0;
  }

  public long getProjectedColumnsUncompressedSize() {
    return projColsUncompressedSize;
  }

  public long getFileLength() {
    return fileLen;
  }

  public OrcTail getOrcTail() {
    return orcTail;
  }

  @Override
  public String toString() {
    return "OrcSplit [" + getPath() + ", start=" + getStart() + ", length=" + getLength()
            + ", isOriginal=" + isOriginal + ", fileLength=" + fileLen + ", hasFooter=" + hasFooter +
            ", hasBase=" + hasBase + ", deltas=" + (deltas == null ? 0 : deltas.size()) + "]";
  }
}
