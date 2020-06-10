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

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
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

  // SASL/Kerberos properties
  public static final String SASL_ENABLED = "accumulo.sasl.enabled";
  public static final String USER_KEYTAB = "accumulo.user.keytab";

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

  public boolean useSasl() {
    Preconditions.checkNotNull(conf);
    return conf.getBoolean(SASL_ENABLED, false);
  }

  public String getAccumuloKeytab() {
    Preconditions.checkNotNull(conf);
    return conf.get(USER_KEYTAB);
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
    ClientConfiguration clientConf = ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zookeepers).withSasl(useSasl());

    return new ZooKeeperInstance(clientConf);
  }

  public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    Instance inst = getInstance();
    return getConnector(inst);
  }

  public Connector getConnector(Instance inst) throws AccumuloException, AccumuloSecurityException {
    String username = getAccumuloUserName();

    // Fail with a good message
    if (null == username) {
      throw new IllegalArgumentException("Accumulo user name must be provided in hiveconf using " + USER_NAME);
    }

    if (useSasl()) {
      return inst.getConnector(username, getKerberosToken(username));
    } else {
      // Not using SASL/Kerberos -- use the password
      String password = getAccumuloPassword();

      if (null == password) {
        throw new IllegalArgumentException("Accumulo password must be provided in hiveconf using " + USER_PASS);
      }

      return inst.getConnector(username, new PasswordToken(password));
    }
  }

  public AuthenticationToken getKerberosToken() {
    if (!useSasl()) {
      throw new IllegalArgumentException("Cannot construct KerberosToken when SASL is disabled");
    }

    final String keytab = getAccumuloKeytab(), username = getAccumuloUserName();

    if (null != keytab) {
      // Use the keytab if one was provided
      return getKerberosToken(username, keytab);
    } else {
      // Otherwise, expect the user is already logged in
      return getKerberosToken(username);
    }
  }

  /**
   * Instantiate a KerberosToken in a backwards compatible manner.
   * @param username Kerberos principal
   */
  KerberosToken getKerberosToken(String username) {
    try {
      return new KerberosToken(username);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to instantiate KerberosToken.", e);
    }
  }

  /**
   * Instantiate a KerberosToken in a backwards compatible manner.
   * @param username Kerberos principal
   * @param keytab Keytab on local filesystem
   */
  KerberosToken getKerberosToken(String username, String keytab) {
    try {
      return new KerberosToken(username, new File(keytab), true);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to instantiate KerberosToken.", e);
    }
  }
}
