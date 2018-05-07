/*
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
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

/**
 * Druid split. Its purpose is to trigger query execution in Druid.
 */
public class HiveDruidSplit extends FileSplit implements org.apache.hadoop.mapred.InputSplit {

  private String druidQuery;

  private String[] hosts;

  // required for deserialization
  public HiveDruidSplit() {
    super((Path) null, 0, 0, (String[]) null);
  }

  public HiveDruidSplit(String druidQuery, Path dummyPath, String hosts[]) {
    super(dummyPath, 0, 0, hosts);
    this.druidQuery = druidQuery;
    this.hosts = hosts;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(druidQuery);
    out.writeInt(hosts.length);
    for (String host : hosts) {
      out.writeUTF(host);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    druidQuery = in.readUTF();
    int length = in.readInt();
    String[] listHosts = new String[length];
    for (int i = 0; i < length; i++) {
      listHosts[i] = in.readUTF();
    }
    hosts = listHosts;
  }

  public String getDruidQuery() {
    return druidQuery;
  }

  @Override
  public String[] getLocations() throws IOException {
    return hosts;
  }

  @Override
  public String toString() {
    return "HiveDruidSplit{" + druidQuery + ", "
            + (hosts == null ? "empty hosts" : Arrays.toString(hosts)) + "}";
  }

}
