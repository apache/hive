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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * CliSessionState.
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

  private Hive hive; // currently only used (and init'ed) in getCurrentDbName

  public CliSessionState() {
    super();
    remoteMode = false;
  }

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

  public void close() {
    try {
      if (remoteMode) {
        client.clean();
        transport.close();
      }
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

  /**
   * Return the name of the current database
   * @return the name of the current database or, if an error, null
   */
  public String getCurrentDbName() {
    if (hive == null) {
      try {
        hive = Hive.get(conf);
      } catch (HiveException e) {
        return null;
      }
    }
    return hive.getCurrentDatabase();
  }
}
