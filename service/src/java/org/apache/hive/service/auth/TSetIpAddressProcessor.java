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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.auth;

import java.net.Socket;

import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Processor;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCLIService.Iface;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for setting the ipAddress for operations executed via HiveServer2.
 * <p>
 * <ul>
 * <li>Ipaddress is only set for operations that calls listeners with hookContext @see ExecuteWithHookContext.</li>
 * <li>Ipaddress is only set if the underlying transport mechanism is socket. </li>
 * </ul>
 * </p>
 */
public class TSetIpAddressProcessor<I extends Iface> extends TCLIService.Processor<Iface> {

  private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class.getName());

  public TSetIpAddressProcessor(Iface iface) {
    super(iface);
  }

  @Override
  public boolean process(final TProtocol in, final TProtocol out) throws TException {
    setIpAddress(in);
    setUserName(in);
    return super.process(in, out);
  }

  private void setUserName(final TProtocol in) {
    TTransport transport = in.getTransport();
    if (transport instanceof TSaslServerTransport) {
      String userName = ((TSaslServerTransport)transport).getSaslServer().getAuthorizationID();
      SessionManager.setUserName(userName);
    }
  }

  protected void setIpAddress(final TProtocol in) {
    TTransport transport = in.getTransport();
    TSocket tSocket = getUnderlyingSocketFromTransport(transport);
    if (tSocket != null) {
     setIpAddress(tSocket.getSocket());
    } else {
      LOGGER.warn("Unknown Transport, cannot determine ipAddress");
    }
  }

  private void setIpAddress(Socket socket) {
    SessionManager.setIpAddress(socket.getInetAddress().toString());
  }

  private TSocket getUnderlyingSocketFromTransport(TTransport transport) {
    while (transport != null) {
      if (transport instanceof TSaslServerTransport) {
        transport = ((TSaslServerTransport) transport).getUnderlyingTransport();
      }
      if (transport instanceof TSaslClientTransport) {
        transport = ((TSaslClientTransport) transport).getUnderlyingTransport();
      }
      if (transport instanceof TSocket) {
        return (TSocket) transport;
      }
    }
    return null;
  }
}