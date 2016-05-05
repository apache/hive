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
import org.apache.tez.runtime.api.impl.TaskSpec;

public class SubmitWorkInfo implements Writable {

  private TaskSpec taskSpec;
  private ApplicationId fakeAppId;
  private long creationTime;

  // This is used to communicate over the LlapUmbilicalProtocol. Not related to tokens used to
  // talk to LLAP daemons itself via the securit work.
  private Token<JobTokenIdentifier> token;

  public SubmitWorkInfo(TaskSpec taskSpec, ApplicationId fakeAppId, long creationTime) {
    this.taskSpec = taskSpec;
    this.fakeAppId = fakeAppId;
    this.token = createJobToken();
    this.creationTime = creationTime;
  }

  // Empty constructor for writable etc.
  public SubmitWorkInfo() {
  }

  public TaskSpec getTaskSpec() {
    return taskSpec;
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
    taskSpec.write(out);
    out.writeLong(fakeAppId.getClusterTimestamp());
    out.writeInt(fakeAppId.getId());
    token.write(out);
    out.writeLong(creationTime);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    taskSpec = new TaskSpec();
    taskSpec.readFields(in);
    long appIdTs = in.readLong();
    int appIdId = in.readInt();
    fakeAppId = ApplicationId.newInstance(appIdTs, appIdId);
    token = new Token<>();
    token.readFields(in);
    creationTime = in.readLong();
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


  private Token<JobTokenIdentifier> createJobToken() {
    String tokenIdentifier = fakeAppId.toString();
    JobTokenIdentifier identifier = new JobTokenIdentifier(new Text(
        tokenIdentifier));
    Token<JobTokenIdentifier> sessionToken = new Token<JobTokenIdentifier>(identifier,
        new JobTokenSecretManager());
    sessionToken.setService(identifier.getJobId());
    return sessionToken;
  }
}
