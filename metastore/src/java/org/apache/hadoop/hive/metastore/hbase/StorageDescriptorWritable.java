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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Wrapper for {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor} to make it writable.
 */
public class StorageDescriptorWritable implements Writable {
  static final private Log LOG = LogFactory.getLog(StorageDescriptorWritable.class.getName());
  final StorageDescriptor sd;

  StorageDescriptorWritable() {
    sd = new SharedStorageDescriptor();
  }

  StorageDescriptorWritable(StorageDescriptor sd) {
    this.sd = sd;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    HBaseUtils.writeStr(out, sd.getLocation());
    HBaseUtils.writeStrStrMap(out, sd.getParameters());
    byte[] hash = HBaseReadWrite.getInstance().putStorageDescriptor(sd);
    out.writeInt(hash.length);
    out.write(hash);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    sd.setLocation(HBaseUtils.readStr(in));
    sd.setParameters(HBaseUtils.readStrStrMap(in));
    int len = in.readInt();
    byte[] hash = new byte[len];
    in.readFully(hash, 0, len);
    ((SharedStorageDescriptor)sd).readShared(hash);
  }


}
