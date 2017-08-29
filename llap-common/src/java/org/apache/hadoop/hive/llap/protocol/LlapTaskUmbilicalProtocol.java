/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.llap.protocol;

import org.apache.hadoop.io.ArrayWritable;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.apache.tez.runtime.api.impl.TezHeartbeatResponse;
import org.apache.tez.runtime.common.security.JobTokenSelector;

@TokenInfo(JobTokenSelector.class)
public interface LlapTaskUmbilicalProtocol extends VersionedProtocol {

  // Why are we still using writables in 2017?
  public class TezAttemptArray extends ArrayWritable {
    public TezAttemptArray() {
      super(TezTaskAttemptID.class);
    }
  }

  public static final long versionID = 1L;

  // From Tez. Eventually changes over to the LLAP protocol and ProtocolBuffers
  boolean canCommit(TezTaskAttemptID taskid) throws IOException;
  public TezHeartbeatResponse heartbeat(TezHeartbeatRequest request)
      throws IOException, TezException;

  public void nodeHeartbeat(
      Text hostname, Text uniqueId, int port, TezAttemptArray aw) throws IOException;

  public void taskKilled(TezTaskAttemptID taskAttemptId) throws IOException;

}
