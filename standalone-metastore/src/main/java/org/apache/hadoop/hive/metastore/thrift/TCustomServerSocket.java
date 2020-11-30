/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.thrift;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

/**
 * ServerSocket implementation of the TTransport interface. This is a copy
 * of the Thrift TServerSocket class.  In this version, the bufferSize_
 * member variable was added to be able to change the buffer size
 * used to wrap the socket stream.  The 1024 value in the 0.9.3 thrift
 * library causes performance issues.
 * THRIFT-5319 was filed for this.
 */
public class TCustomServerSocket extends TServerSocket {

  private static final Logger LOGGER = LoggerFactory.getLogger(TCustomServerSocket.class.getName());

  private int timeout;

  private int bufferSize;

  /**
   * Creates a server socket from underlying socket object
   */
  public TCustomServerSocket(ServerSocket serverSocket, int timeout, int bufferSize) throws TTransportException {
    super(serverSocket, timeout);
    this.timeout = timeout;
    this.bufferSize = bufferSize;
  }

  public TCustomServerSocket(InetSocketAddress bindAddr, int bufferSize) throws TTransportException {
    super(bindAddr);
    this.bufferSize = bufferSize;
    this.timeout = 0;
  }

  @Override
  protected TSocket acceptImpl() throws TTransportException {
    ServerSocket serverSocket = getServerSocket();
    if (serverSocket == null) {
      throw new TTransportException(TTransportException.NOT_OPEN, "No underlying server socket.");
    }
    try {
      Socket result = serverSocket.accept();
      TCustomSocket result2 = new TCustomSocket(result, bufferSize);
      result2.setTimeout(timeout);
      return result2;
    } catch (IOException iox) {
      throw new TTransportException(iox);
    }
  }

  public int getTimeout() {
    return timeout;
  }

  public int getBufferSize() {
    return bufferSize;
  }
}
