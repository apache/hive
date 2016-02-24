/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataInputStream;
import java.io.ByteArrayInputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapred.InputSplitWithLocationInfo;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.AutoExpandingBufferWriteTransport;
import org.apache.thrift.transport.AutoExpandingBuffer;

import com.google.common.base.Preconditions;

public class LlapInputSplit implements InputSplitWithLocationInfo {

  byte[] queryFragment;
  SplitLocationInfo[] locations;
  Schema schema;

  public LlapInputSplit() {}

  public LlapInputSplit(byte[] queryFragment, SplitLocationInfo[] locations, Schema schema) {
    this.queryFragment = queryFragment;
    this.locations = locations;
    this.schema = schema;
  }

  public Schema getSchema() {
    return schema;
  }

  @Override
  public long getLength() throws IOException {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException {
    String[] locs = new String[locations.length];
    for (int i = 0; i < locations.length; ++i) {
      locs[i] = locations[i].getLocation();
    }
    return locs;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(queryFragment.length);
    out.write(queryFragment);

    out.writeInt(locations.length);
    for (int i = 0; i < locations.length; ++i) {
      out.writeUTF(locations[i].getLocation());
    }

    byte[] binarySchema;

    try {
      AutoExpandingBufferWriteTransport transport = new AutoExpandingBufferWriteTransport(1024, 2d);
      TProtocol protocol = new TBinaryProtocol(transport);
      schema.write(protocol);
      binarySchema = transport.getBuf().array();
    } catch (Exception e) {
      throw new IOException(e);
    }

    out.writeInt(binarySchema.length);
    out.write(binarySchema);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte[] queryFragment;

    int length = in.readInt();
    queryFragment = new byte[length];
    in.readFully(queryFragment);

    length = in.readInt();
    locations = new SplitLocationInfo[length];

    for (int i = 0; i < length; ++i) {
      locations[i] = new SplitLocationInfo(in.readUTF(), false);
    }

    length = in.readInt();

    try {
      AutoExpandingBufferWriteTransport transport = new AutoExpandingBufferWriteTransport(length, 2d);
      AutoExpandingBuffer buf = transport.getBuf();
      in.readFully(buf.array(), 0, length);

      TProtocol protocol = new TBinaryProtocol(transport);
      schema = new Schema();
      schema.read(protocol);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public SplitLocationInfo[] getLocationInfo() throws IOException {
    return locations;
  }
}
