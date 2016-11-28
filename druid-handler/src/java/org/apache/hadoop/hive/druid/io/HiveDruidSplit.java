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
package org.apache.hadoop.hive.druid.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

/**
 * Druid split. Its purpose is to trigger query execution in Druid.
 */
public class HiveDruidSplit extends FileSplit implements org.apache.hadoop.mapred.InputSplit {

  private String address;

  private String druidQuery;

  // required for deserialization
  public HiveDruidSplit() {
    super((Path) null, 0, 0, (String[]) null);
  }

  public HiveDruidSplit(String address, String druidQuery, Path dummyPath) {
    super(dummyPath, 0, 0, (String[]) null);
    this.address = address;
    this.druidQuery = druidQuery;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(address);
    out.writeUTF(druidQuery);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    address = in.readUTF();
    druidQuery = in.readUTF();
  }

  @Override
  public long getLength() {
    return 0L;
  }

  @Override
  public String[] getLocations() {
    return new String[] { "" };
  }

  public String getAddress() {
    return address;
  }

  public String getDruidQuery() {
    return druidQuery;
  }

  @Override
  public String toString() {
    return "HiveDruidSplit{" + address + ", " + druidQuery + "}";
  }

}
