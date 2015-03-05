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

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A class to serialize a list of grant infos.  There is not a corresponding thrift object.
 */
public class GrantInfoList implements Writable{
  List<GrantInfoWritable> grantInfos;

  GrantInfoList() {
    grantInfos = new ArrayList<GrantInfoWritable>();
  }

  GrantInfoList(List<GrantInfoWritable> infos) {
    grantInfos = infos;
  }


  @Override
  public void write(DataOutput out) throws IOException {
    if (grantInfos == null) {
      out.writeInt(0);
    } else {
      out.writeInt(grantInfos.size());
      for (GrantInfoWritable info : grantInfos) {
        info.write(out);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    if (size == 0) {
      grantInfos = new ArrayList<GrantInfoWritable>();
    } else {
      grantInfos = new ArrayList<GrantInfoWritable>(size);
      for (int i = 0; i < size; i++) {
        GrantInfoWritable info = new GrantInfoWritable();
        info.readFields(in);
        grantInfos.add(info);
      }
    }
  }
}
