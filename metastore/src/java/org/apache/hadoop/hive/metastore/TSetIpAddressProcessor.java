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

package org.apache.hadoop.hive.metastore;

import java.lang.reflect.InvocationTargetException;
import java.net.Socket;

import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * TSetIpAddressProcessor passes the IP address of the Thrift client to the HMSHandler.
 */
public class TSetIpAddressProcessor<I extends Iface> extends ThriftHiveMetastore.Processor<Iface> {

  @SuppressWarnings("unchecked")
  public TSetIpAddressProcessor(I iface) throws SecurityException, NoSuchFieldException,
    IllegalArgumentException, IllegalAccessException, NoSuchMethodException,
    InvocationTargetException {
    super(iface);
  }

  @Override
  public boolean process(final TProtocol in, final TProtocol out) throws TException {
    setIpAddress(in);

    return super.process(in, out);
  }

  protected void setIpAddress(final TProtocol in) {
    TTransport transport = in.getTransport();
    if (!(transport instanceof TSocket)) {
      return;
    }
    setIpAddress(((TSocket)transport).getSocket());
  }

  protected void setIpAddress(final Socket inSocket) {
    HMSHandler.setIpAddress(inSocket.getInetAddress().toString());
  }
}
