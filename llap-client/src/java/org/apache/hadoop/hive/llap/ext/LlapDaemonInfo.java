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

package org.apache.hadoop.hive.llap.ext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Writable;

/**
 * LlapDaemonInfo - contains llap daemon information
 *   - host - hostname of llap daemon
 *   - rpcPort  - rpc port of llap daemon to submit fragments
 *   - outputFormatPort - output port of llap daemon to read data corresponding to the submitted fragment
 */
public class LlapDaemonInfo implements Writable {

  private String host;
  private int rpcPort;
  private int outputFormatPort;

  public LlapDaemonInfo(String host, int rpcPort, int outputFormatPort) {
    this.host = host;
    this.rpcPort = rpcPort;
    this.outputFormatPort = outputFormatPort;
  }

  public LlapDaemonInfo() {
  }

  public String getHost() {
    return host;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public int getOutputFormatPort() {
    return outputFormatPort;
  }

  @Override
  public String toString() {
    return "LlapDaemonInfo{" +
        "host='" + host + '\'' +
        ", rpcPort=" + rpcPort +
        ", outputFormatPort=" + outputFormatPort +
        '}';
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    LlapDaemonInfo that = (LlapDaemonInfo) o;
    return rpcPort == that.rpcPort && outputFormatPort == that.outputFormatPort && Objects.equals(host, that.host);
  }

  @Override public int hashCode() {
    return Objects.hash(host, rpcPort, outputFormatPort);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(host);
    out.writeInt(rpcPort);
    out.writeInt(outputFormatPort);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    host = in.readUTF();
    rpcPort = in.readInt();
    outputFormatPort = in.readInt();
  }
}
