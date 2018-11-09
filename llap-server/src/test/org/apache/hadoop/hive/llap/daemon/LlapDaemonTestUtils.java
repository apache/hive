/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.daemon;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.tezplugins.LlapTezUtils;
import org.apache.hadoop.security.Credentials;
import java.io.IOException;

/**
 * Utils class for testing Llap Daemon.
 */
public class LlapDaemonTestUtils {
  private LlapDaemonTestUtils() {}

  public static SubmitWorkRequestProto buildSubmitProtoRequest(int fragmentNumber,
      String appId, int dagId, int vId, String dagName,
      int dagStartTime, int attemptStartTime, int numSelfAndUpstreamTasks, int numSelfAndUpstreamComplete,
      int withinDagPriority, Credentials credentials) throws IOException {
    return SubmitWorkRequestProto
        .newBuilder()
        .setAttemptNumber(0)
        .setFragmentNumber(fragmentNumber)
        .setWorkSpec(
            LlapDaemonProtocolProtos.VertexOrBinary.newBuilder().setVertex(
                LlapDaemonProtocolProtos.SignableVertexSpec
                    .newBuilder()
                    .setQueryIdentifier(
                        LlapDaemonProtocolProtos.QueryIdentifierProto.newBuilder()
                            .setApplicationIdString(appId)
                            .setAppAttemptNumber(0)
                            .setDagIndex(dagId)
                            .build())
                    .setVertexIndex(vId)
                    .setDagName(dagName)
                    .setHiveQueryId(dagName)
                    .setVertexName("MockVertex")
                    .setUser("MockUser")
                    .setTokenIdentifier("MockToken_1")
                    .setProcessorDescriptor(
                        LlapDaemonProtocolProtos.EntityDescriptorProto.newBuilder()
                            .setClassName("MockProcessor").build())
                    .build()).build())
        .setAmHost("localhost")
        .setAmPort(12345)
        .setCredentialsBinary(ByteString.copyFrom(LlapTezUtils.serializeCredentials(credentials)))
        .setContainerIdString("MockContainer_1")
        .setFragmentRuntimeInfo(LlapDaemonProtocolProtos
            .FragmentRuntimeInfo
            .newBuilder()
            .setDagStartTime(dagStartTime)
            .setFirstAttemptStartTime(attemptStartTime)
            .setNumSelfAndUpstreamTasks(numSelfAndUpstreamTasks)
            .setNumSelfAndUpstreamCompletedTasks(numSelfAndUpstreamComplete)
            .setWithinDagPriority(withinDagPriority)
            .build())
        .build();
  }
}
