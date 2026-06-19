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

package org.apache.hadoop.hive.ql.exec.tez;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.ql.plan.TezWork.VertexType;
import org.apache.hadoop.io.Writable;

/*
 * This class is the payload for custom vertex. It serializes and de-serializes
 * @numBuckets: the number of buckets of the "big table"
 * @vertexType: this is the type of vertex and differentiates between bucket map join and SMB joins
 * @numInputs: The number of inputs that are directly connected to the vertex (MRInput/MultiMRInput).
 *             In case of bucket map join, it is always 1.
 * @inputName: This is the name of the input. Used in case of SMB joins. Empty in case of BucketMapJoin
 */
public class CustomVertexConfiguration implements Writable {

  private int numBuckets;
  private VertexType vertexType = VertexType.AUTO_INITIALIZED_EDGES;
  private int numInputs;
  private String inputName;
  private Map<String, Integer> inputToBucketMap;

  public CustomVertexConfiguration() {
  }

  // this is the constructor to use for the Bucket map join case.
  public CustomVertexConfiguration(int numBuckets, VertexType vertexType) {
    this(numBuckets, vertexType, "", 1, null);
  }

  // this is the constructor to use for SMB.
  public CustomVertexConfiguration(int numBuckets, VertexType vertexType, String inputName,
                                   int numInputs, Map<String, Integer> inputToBucketMap) {
    this.numBuckets = numBuckets;
    this.vertexType = vertexType;
    this.numInputs = numInputs;
    this.inputName = inputName;
    this.inputToBucketMap = inputToBucketMap;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.vertexType.ordinal());
    out.writeInt(this.numBuckets);
    out.writeInt(numInputs);
    out.writeUTF(inputName);
    int sz = inputToBucketMap != null ? inputToBucketMap.size() : 0;
    out.writeInt(sz);
    if (sz > 0) {
      for (Map.Entry<String, Integer> entry : inputToBucketMap.entrySet()) {
        out.writeUTF(entry.getKey());
        out.writeInt(entry.getValue());
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.vertexType = VertexType.values()[in.readInt()];
    this.numBuckets = in.readInt();
    this.numInputs = in.readInt();
    this.inputName = in.readUTF();
    int sz = in.readInt();
    Preconditions.checkState(sz >= 0);
    if (sz == 0) {
      this.inputToBucketMap = null;
    } else {
      this.inputToBucketMap = new HashMap<>();
      for (int i = 0; i < sz; i++) {
        this.inputToBucketMap.put(in.readUTF(), in.readInt());
      }
    }
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public VertexType getVertexType() {
    return vertexType;
  }

  public String getInputName() {
    return inputName;
  }

  public int getNumInputs() {
    return numInputs;
  }

  public Map<String, Integer> getInputToBucketMap() {
    return inputToBucketMap;
  }
}
