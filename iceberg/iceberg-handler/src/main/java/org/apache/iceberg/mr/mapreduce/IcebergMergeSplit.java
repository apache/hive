/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.util.SerializationUtil;

public class IcebergMergeSplit extends FileSplit implements org.apache.hadoop.mapred.InputSplit {
  private transient Configuration conf;
  private ContentFile contentFile;

  // public no-argument constructor for deserialization
  public IcebergMergeSplit() {
  }

  public IcebergMergeSplit(Configuration conf,
                           CombineHiveInputFormat.CombineHiveInputSplit split,
                           Integer partition, Properties properties) throws IOException {
    super(split.getPaths()[partition], split
            .getStartOffsets()[partition], split.getLengths()[partition], split
            .getLocations());
    this.conf = conf;
    Path path = split.getPaths()[partition];
    contentFile = (ContentFile) properties.get(path);
  }

  @Override
  public long getLength() {
    return contentFile.fileSizeInBytes();
  }

  @Override
  public String[] getLocations() {
    return new String[]{"*"};
  }

  @Override
  public void write(DataOutput out) throws IOException {
    byte[] data = SerializationUtil.serializeToBytes(this.contentFile);
    out.writeInt(data.length);
    out.write(data);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte[] data = new byte[in.readInt()];
    in.readFully(data);
    this.contentFile = SerializationUtil.deserializeFromBytes(data);
  }

  public ContentFile getContentFile() {
    return contentFile;
  }
}
