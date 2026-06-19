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

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;

public class SubmitWorkInfo implements Writable {

  private ApplicationId fakeAppId;
  private long creationTime;
  private byte[] vertexSpec, vertexSpecSignature;

  // This is used to communicate over the LlapUmbilicalProtocol. Not related to tokens used to
  // talk to LLAP daemons itself via the securit work.
  private Token<JobTokenIdentifier> token;
  private int vertexParallelism;

  public SubmitWorkInfo(ApplicationId fakeAppId, long creationTime,
      int vertexParallelism, byte[] vertexSpec, byte[] vertexSpecSignature,
      Token<JobTokenIdentifier> token) {
    this.fakeAppId = fakeAppId;
    this.token = token;
    this.creationTime = creationTime;
    this.vertexSpec = vertexSpec;
    this.vertexSpecSignature = vertexSpecSignature;
    this.vertexParallelism = vertexParallelism;
  }

  // Empty constructor for writable etc.
  public SubmitWorkInfo() {
  }

  public ApplicationId getFakeAppId() {
    return fakeAppId;
  }

  public String getTokenIdentifier() {
    return fakeAppId.toString();
  }

  public Token<JobTokenIdentifier> getToken() {
    return token;
  }

  public long getCreationTime() {
    return creationTime;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(fakeAppId.getClusterTimestamp());
    out.writeInt(fakeAppId.getId());
    token.write(out);
    out.writeLong(creationTime);
    out.writeInt(vertexParallelism);
    if (vertexSpec != null) {
      out.writeInt(vertexSpec.length);
      out.write(vertexSpec);
    } else {
      out.writeInt(0);
    }
    if (vertexSpecSignature != null) {
      out.writeInt(vertexSpecSignature.length);
      out.write(vertexSpecSignature);
    } else {
      out.writeInt(0);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    long appIdTs = in.readLong();
    int appIdId = in.readInt();
    fakeAppId = ApplicationId.newInstance(appIdTs, appIdId);
    token = new Token<>();
    token.readFields(in);
    creationTime = in.readLong();
    vertexParallelism = in.readInt();
    int vertexSpecBytes = in.readInt();
    if (vertexSpecBytes > 0) {
      vertexSpec = new byte[vertexSpecBytes];
      in.readFully(vertexSpec);
    }
    int vertexSpecSignBytes = in.readInt();
    if (vertexSpecSignBytes > 0) {
      vertexSpecSignature = new byte[vertexSpecSignBytes];
      in.readFully(vertexSpecSignature);
    }
  }

  public static byte[] toBytes(SubmitWorkInfo submitWorkInfo) throws IOException {
    DataOutputBuffer dob = new DataOutputBuffer();
    submitWorkInfo.write(dob);
    return dob.getData();
  }

  public static SubmitWorkInfo fromBytes(byte[] submitWorkInfoBytes) throws IOException {
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(submitWorkInfoBytes, 0, submitWorkInfoBytes.length);
    SubmitWorkInfo submitWorkInfo = new SubmitWorkInfo();
    submitWorkInfo.readFields(dib);
    return submitWorkInfo;
  }

  public byte[] getVertexBinary() {
    return vertexSpec;
  }

  public byte[] getVertexSignature() {
    return vertexSpecSignature;
  }

  public int getVertexParallelism() {
    return vertexParallelism;
  }
}
