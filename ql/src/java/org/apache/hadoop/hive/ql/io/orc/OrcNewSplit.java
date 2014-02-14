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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.Reader.FileMetaInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * OrcFileSplit. Holds file meta info
 *
 */
public class OrcNewSplit extends FileSplit {
  private Reader.FileMetaInfo fileMetaInfo;
  private boolean hasFooter;
  
  protected OrcNewSplit(){
    //The FileSplit() constructor in hadoop 0.20 and 1.x is package private so can't use it.
    //This constructor is used to create the object and then call readFields()
    // so just pass nulls to this super constructor.
    super(null, 0, 0, (String[])null);
  }
  
  public OrcNewSplit(Path path, long offset, long length, String[] hosts,
      FileMetaInfo fileMetaInfo) {
    super(path, offset, length, hosts);
    this.fileMetaInfo = fileMetaInfo;
    hasFooter = this.fileMetaInfo != null;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    //serialize path, offset, length using FileSplit
    super.write(out);

    // Whether footer information follows.
    out.writeBoolean(hasFooter);

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

    hasFooter = in.readBoolean();

    if (hasFooter) {
      // deserialize FileMetaInfo fields
      String compressionType = Text.readString(in);
      int bufferSize = WritableUtils.readVInt(in);
      int metadataSize = WritableUtils.readVInt(in);

      // deserialize FileMetaInfo field footer
      int footerBuffSize = WritableUtils.readVInt(in);
      ByteBuffer footerBuff = ByteBuffer.allocate(footerBuffSize);
      in.readFully(footerBuff.array(), 0, footerBuffSize);

      fileMetaInfo = new FileMetaInfo(compressionType, bufferSize, metadataSize, footerBuff);
    }
  }

  public FileMetaInfo getFileMetaInfo(){
    return fileMetaInfo;
  }

  public boolean hasFooter() {
    return hasFooter;
  }
}
