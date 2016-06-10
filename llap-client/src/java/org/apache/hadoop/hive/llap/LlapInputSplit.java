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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hive.llap.Schema;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;
import org.apache.hadoop.mapred.InputSplitWithLocationInfo;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.security.token.Token;

public class LlapInputSplit implements InputSplitWithLocationInfo {

  private int splitNum;
  private byte[] planBytes;
  private byte[] fragmentBytes;
  private SplitLocationInfo[] locations;
  private Schema schema;
  private String llapUser;
  private byte[] fragmentBytesSignature;
  private byte[] tokenBytes;

  public LlapInputSplit() {
  }

  public LlapInputSplit(int splitNum, byte[] planBytes, byte[] fragmentBytes,
      byte[] fragmentBytesSignature, SplitLocationInfo[] locations, Schema schema,
      String llapUser, byte[] tokenBytes) {
    this.planBytes = planBytes;
    this.fragmentBytes = fragmentBytes;
    this.fragmentBytesSignature = fragmentBytesSignature;
    this.locations = locations;
    this.schema = schema;
    this.splitNum = splitNum;
    this.llapUser = llapUser;
    this.tokenBytes = tokenBytes;
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

  public int getSplitNum() {
    return splitNum;
  }

  public byte[] getPlanBytes() {
    return planBytes;
  }

  public byte[] getFragmentBytes() {
    return fragmentBytes;
  }

  public byte[] getFragmentBytesSignature() {
    return fragmentBytesSignature;
  }

  public byte[] getTokenBytes() {
    return tokenBytes;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(splitNum);
    out.writeInt(planBytes.length);
    out.write(planBytes);

    out.writeInt(fragmentBytes.length);
    out.write(fragmentBytes);
    if (fragmentBytesSignature != null) {
      out.writeInt(fragmentBytesSignature.length);
      out.write(fragmentBytesSignature);
    } else {
      out.writeInt(0);
    }

    out.writeInt(locations.length);
    for (int i = 0; i < locations.length; ++i) {
      out.writeUTF(locations[i].getLocation());
    }

    schema.write(out);
    out.writeUTF(llapUser);
    if (tokenBytes != null) {
      out.writeInt(tokenBytes.length);
      out.write(tokenBytes);
    } else {
      out.writeInt(0);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    splitNum = in.readInt();
    int length = in.readInt();
    planBytes = new byte[length];
    in.readFully(planBytes);

    length = in.readInt();
    fragmentBytes = new byte[length];
    in.readFully(fragmentBytes);
    length = in.readInt();
    if (length > 0) {
      fragmentBytesSignature = new byte[length];
      in.readFully(fragmentBytesSignature);
    }

    length = in.readInt();
    locations = new SplitLocationInfo[length];

    for (int i = 0; i < length; ++i) {
      locations[i] = new SplitLocationInfo(in.readUTF(), false);
    }

    schema = new Schema();
    schema.readFields(in);
    llapUser = in.readUTF();
    length = in.readInt();
    if (length > 0) {
      tokenBytes = new byte[length];
      in.readFully(tokenBytes);
    }
  }

  @Override
  public SplitLocationInfo[] getLocationInfo() throws IOException {
    return locations;
  }

  public String getLlapUser() {
    return llapUser;
  }
}
