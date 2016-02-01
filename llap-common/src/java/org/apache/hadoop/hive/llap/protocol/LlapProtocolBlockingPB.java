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

package org.apache.hadoop.hive.llap.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.security.LlapTokenSelector;

@ProtocolInfo(protocolName = "org.apache.hadoop.hive.llap.protocol.LlapProtocolBlockingPB", protocolVersion = 1)
@KerberosInfo(serverPrincipal = HiveConf.HIVE_LLAP_DAEMON_SERVICE_PRINCIPAL_NAME)
@TokenInfo(LlapTokenSelector.class)
@InterfaceAudience.Private
public interface LlapProtocolBlockingPB extends LlapDaemonProtocolProtos.LlapDaemonProtocol.BlockingInterface {
}
