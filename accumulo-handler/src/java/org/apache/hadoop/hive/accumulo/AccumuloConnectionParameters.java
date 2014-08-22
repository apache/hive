/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

/**
 *
 */
public class AccumuloConnectionParameters {
  public static final String USER_NAME = "accumulo.user.name";
  public static final String USER_PASS = "accumulo.user.pass";
  public static final String ZOOKEEPERS = "accumulo.zookeepers";
  public static final String INSTANCE_NAME = "accumulo.instance.name";
  public static final String TABLE_NAME = "accumulo.table.name";

  public static final String USE_MOCK_INSTANCE = "accumulo.mock.instance";

  protected Configuration conf;
  protected boolean useMockInstance = false;

  public AccumuloConnectionParameters(Configuration conf) {
    // TableDesc#getDeserializer will ultimately instantiate the AccumuloSerDe with a null
    // Configuration
    // We have to accept this and just fail late if data is attempted to be pulled from the
    // Configuration
    this.conf = conf;
  }

  public Configuration getConf() {
    return conf;
  }

  public String getAccumuloUserName() {
    Preconditions.checkNotNull(conf);
    return conf.get(USER_NAME);
  }

  public String getAccumuloPassword() {
    Preconditions.checkNotNull(conf);
    return conf.get(USER_PASS);
  }

  public String getAccumuloInstanceName() {
    Preconditions.checkNotNull(conf);
    return conf.get(INSTANCE_NAME);
  }

  public String getZooKeepers() {
    Preconditions.checkNotNull(conf);
    return conf.get(ZOOKEEPERS);
  }

  public String getAccumuloTableName() {
    Preconditions.checkNotNull(conf);
    return conf.get(TABLE_NAME);
  }

  public boolean useMockInstance() {
    Preconditions.checkNotNull(conf);
    return conf.getBoolean(USE_MOCK_INSTANCE, false);
  }

  public Instance getInstance() {
    String instanceName = getAccumuloInstanceName();

    // Fail with a good message
    if (null == instanceName) {
      throw new IllegalArgumentException("Accumulo instance name must be provided in hiveconf using " + INSTANCE_NAME);
    }

    if (useMockInstance()) {
      return new MockInstance(instanceName);
    }

    String zookeepers = getZooKeepers();

    // Fail with a good message
    if (null == zookeepers) {
      throw new IllegalArgumentException("ZooKeeper quorum string must be provided in hiveconf using " + ZOOKEEPERS);
    }

    return new ZooKeeperInstance(instanceName, zookeepers);
  }

  public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    Instance inst = getInstance();
    return getConnector(inst);
  }

  public Connector getConnector(Instance inst) throws AccumuloException, AccumuloSecurityException {
    String username = getAccumuloUserName(), password = getAccumuloPassword();

    // Fail with a good message
    if (null == username) {
      throw new IllegalArgumentException("Accumulo user name must be provided in hiveconf using " + USER_NAME);
    }
    if (null == password) {
      throw new IllegalArgumentException("Accumulo password must be provided in hiveconf using " + USER_PASS);
    }

    return inst.getConnector(username, new PasswordToken(password));
  }
}
