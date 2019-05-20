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

package org.apache.hadoop.hive.llap.impl;

import javax.net.SocketFactory;
import java.io.IOException;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hive.llap.plugin.rpc.LlapPluginProtocolProtos.UpdateQueryRequestProto;
import org.apache.hadoop.hive.llap.plugin.rpc.LlapPluginProtocolProtos.UpdateQueryResponseProto;
import org.apache.hadoop.hive.llap.protocol.LlapPluginProtocolPB;

public class LlapPluginProtocolClientImpl implements LlapPluginProtocolPB {
  private ProtobufProxy<LlapPluginProtocolPB> protobuf;

  public LlapPluginProtocolClientImpl(Configuration conf, String hostname, int port,
      RetryPolicy retryPolicy, SocketFactory socketFactory, UserGroupInformation ugi) {
    protobuf = new ProtobufProxy<>(
        conf, ugi, hostname, port, retryPolicy, socketFactory, LlapPluginProtocolPB.class);
  }

  @Override
  public UpdateQueryResponseProto updateQuery(RpcController controller,
      UpdateQueryRequestProto request) throws ServiceException {
    try {
      return protobuf.getProxy().updateQuery(null, request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
