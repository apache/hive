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

package org.apache.hadoop.hive.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * SessionState for hive cli.
 *
 */
public class CliSessionState extends SessionState {
  /**
   * -database option if any that the session has been invoked with.
   */
  public String database;

  /**
   * -e option if any that the session has been invoked with.
   */
  public String execString;

  /**
   * -f option if any that the session has been invoked with.
   */
  public String fileName;

  /**
   * properties set from -hiveconf via cmdline.
   */
  public Properties cmdProperties = new Properties();

  /**
   * -i option if any that the session has been invoked with.
   */
  public List<String> initFiles = new ArrayList<String>();

  /**
   * host name and port number of remote Hive server
   */
  protected String host;
  protected int port;

  private boolean remoteMode;

  private TTransport transport;
  private HiveClient client;

  public CliSessionState(HiveConf conf) {
    super(conf);
    remoteMode = false;
  }

  /**
   * Connect to Hive Server
   */
  public void connect() throws TTransportException {
    transport = new TSocket(host, port);
    TProtocol protocol = new TBinaryProtocol(transport);
    client = new HiveClient(protocol);
    transport.open();
    remoteMode = true;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  @Override
  public void close() {
    try {
      super.close();
      if (remoteMode) {
        client.clean();
        transport.close();
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    } catch (TException e) {
      e.printStackTrace();
    }
  }

  public boolean isRemoteMode() {
    return remoteMode;
  }

  public HiveClient getClient() {
    return client;
  }

}
