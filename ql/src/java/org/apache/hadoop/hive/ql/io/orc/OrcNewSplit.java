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

import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * OrcFileSplit. Holds file meta info
 *
 */
public class OrcNewSplit extends FileSplit {
  private OrcTail orcTail;
  private boolean hasFooter;
  private boolean isOriginal;
  private boolean hasBase;
  private final List<AcidInputFormat.DeltaMetaData> deltas = new ArrayList<>();
  private OrcFile.WriterVersion writerVersion;

  protected OrcNewSplit(){
    //The FileSplit() constructor in hadoop 0.20 and 1.x is package private so can't use it.
    //This constructor is used to create the object and then call readFields()
    // so just pass nulls to this super constructor.
    super(null, 0, 0, null);
  }
  
  public OrcNewSplit(OrcSplit inner) throws IOException {
    super(inner.getPath(), inner.getStart(), inner.getLength(),
          inner.getLocations());
    this.orcTail = inner.getOrcTail();
    this.hasFooter = inner.hasFooter();
    this.isOriginal = inner.isOriginal();
    this.hasBase = inner.hasBase();
    this.deltas.addAll(inner.getDeltas());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    //serialize path, offset, length using FileSplit
    super.write(out);

    int flags = (hasBase ? OrcSplit.BASE_FLAG : 0) |
        (isOriginal ? OrcSplit.ORIGINAL_FLAG : 0) |
        (hasFooter ? OrcSplit.FOOTER_FLAG : 0);
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
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    //deserialize path, offset, length using FileSplit
    super.readFields(in);

    byte flags = in.readByte();
    hasFooter = (OrcSplit.FOOTER_FLAG & flags) != 0;
    isOriginal = (OrcSplit.ORIGINAL_FLAG & flags) != 0;
    hasBase = (OrcSplit.BASE_FLAG & flags) != 0;

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

  public OrcTail getOrcTail() {
    return orcTail;
  }
}
