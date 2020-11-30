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
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

/**
 * Socket implementation of the TTransport interface.
 * In this version, the bufferSize_ member variable was added to be able
 * to change the buffer size used to wrap the socket stream.
 * The 1024 value in the 0.9.3 thrift library causes performance issues.
 * THRIFT-5319 was filed for this.
 */
public class TCustomSocket extends TSocket {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(TCustomSocket.class.getName());

  /**
   * init the buffer size from Configuration
   */
  private final int bufferSize;

  /**
   * Constructor that takes an already created socket.
   *
   * @param socket Already created socket object
   * @param bufferSize buffer size used for the socket stream.
   * @throws TTransportException if there is an error setting up the streams
   */
  public TCustomSocket(Socket socket, int bufferSize) throws TTransportException {
    super(socket);
    this.bufferSize = bufferSize;
    // CDPD-20183: change to debug
    LOGGER.info("Buffer size for TSocket is: " + bufferSize);
    if (isOpen()) {
      try {
        if (bufferSize == -1) {
          inputStream_ = getSocket().getInputStream();
          outputStream_ = getSocket().getOutputStream();
        } else {
          inputStream_ = new BufferedInputStream(getSocket().getInputStream(), bufferSize);
          outputStream_ = new BufferedOutputStream(getSocket().getOutputStream(), bufferSize);
        }
      } catch (IOException iox) {
        close();
        throw new TTransportException(TTransportException.NOT_OPEN, iox);
      }
    }
  }

  /**
   * Creates a new unconnected socket that will connect to the given host on the given
   * port, with a specific connection timeout and a specific socket timeout and a
   * specific buffer size for the socket stream.
   *
   * @param host           Remote host
   * @param port           Remote port
   * @param socketTimeout  Socket timeout
   * @param connectTimeout Connection timeout
   * @param bufferSize     buffer size for socket input stream
   */
  public TCustomSocket(String host, int port, int socketTimeout,
      int connectTimeout, int bufferSize) {
    super(host, port, socketTimeout, connectTimeout);
    this.bufferSize = bufferSize;
    // CDPD-20183: change to debug
    LOGGER.info("Buffer size for TSocket is: " + bufferSize);
  }

  /**
   * Connects the socket, creating a new socket object if necessary.
   */
  @Override
  public void open() throws TTransportException {
    super.open();
    try {
      if (bufferSize == -1) {
        inputStream_ = getSocket().getInputStream();
        outputStream_ = getSocket().getOutputStream();
      } else {
        inputStream_ = new BufferedInputStream(getSocket().getInputStream(), bufferSize);
        outputStream_ = new BufferedOutputStream(getSocket().getOutputStream(), bufferSize);
      }
    } catch (IOException iox) {
      close();
      throw new TTransportException(TTransportException.NOT_OPEN, iox);
    }
  }
}
