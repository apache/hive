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

package org.apache.hadoop.hive.metastore;

import java.net.SocketException;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

/**
 * TServerSocketKeepAlive - like TServerSocket, but will enable keepalive for
 * accepted sockets.
 *
 */
public class TServerSocketKeepAlive extends TServerSocket {
  public TServerSocketKeepAlive(TServerSocket serverSocket) throws TTransportException {
    super(serverSocket.getServerSocket());
  }

  @Override
  protected TSocket acceptImpl() throws TTransportException {
    TSocket ts = super.acceptImpl();
    try {
      ts.getSocket().setKeepAlive(true);
    } catch (SocketException e) {
      throw new TTransportException(e);
    }
    return ts;
  }
}
