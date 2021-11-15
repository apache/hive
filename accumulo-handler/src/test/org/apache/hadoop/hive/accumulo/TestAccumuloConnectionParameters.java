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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 *
 */
public class TestAccumuloConnectionParameters {

  @Test
  public void testInstantiatesWithNullConfiguration() {
    // TableDesc#getDeserializer() passes a null Configuration into the SerDe.
    // We shouldn't fail immediately in this case
    AccumuloConnectionParameters cnxnParams = new AccumuloConnectionParameters(null);

    // We should fail if we try to get info out of the params
    try {
      cnxnParams.getAccumuloInstanceName();
      Assert.fail("Should have gotten an NPE");
    } catch (NullPointerException e) {}
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingInstanceName() {
    Configuration conf = new Configuration(false);
    conf.set(AccumuloConnectionParameters.ZOOKEEPERS, "localhost:2181");
    conf.set(AccumuloConnectionParameters.USER_NAME, "user");
    conf.set(AccumuloConnectionParameters.USER_PASS, "password");

    AccumuloConnectionParameters cnxnParams = new AccumuloConnectionParameters(conf);
    cnxnParams.getInstance();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingZooKeepers() {
    Configuration conf = new Configuration(false);
    conf.set(AccumuloConnectionParameters.INSTANCE_NAME, "accumulo");
    conf.set(AccumuloConnectionParameters.USER_NAME, "user");
    conf.set(AccumuloConnectionParameters.USER_PASS, "password");

    AccumuloConnectionParameters cnxnParams = new AccumuloConnectionParameters(conf);
    cnxnParams.getInstance();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingUserName() throws AccumuloException, AccumuloSecurityException {
    Configuration conf = new Configuration(false);
    conf.set(AccumuloConnectionParameters.INSTANCE_NAME, "accumulo");
    conf.set(AccumuloConnectionParameters.ZOOKEEPERS, "localhost:2181");
    conf.set(AccumuloConnectionParameters.USER_PASS, "password");

    Instance instance = Mockito.mock(Instance.class);

    AccumuloConnectionParameters cnxnParams = new AccumuloConnectionParameters(conf);

    // Provide an instance of the code doesn't try to make a real Instance
    // We just want to test that we fail before trying to make a connector
    // with null username
    cnxnParams.getConnector(instance);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingPassword() throws AccumuloException, AccumuloSecurityException {
    Configuration conf = new Configuration(false);
    conf.set(AccumuloConnectionParameters.INSTANCE_NAME, "accumulo");
    conf.set(AccumuloConnectionParameters.ZOOKEEPERS, "localhost:2181");
    conf.set(AccumuloConnectionParameters.USER_NAME, "user");

    Instance instance = Mockito.mock(Instance.class);

    AccumuloConnectionParameters cnxnParams = new AccumuloConnectionParameters(conf);

    // Provide an instance of the code doesn't try to make a real Instance
    // We just want to test that we fail before trying to make a connector
    // with null password
    cnxnParams.getConnector(instance);
  }

  @Test
  public void testSasl() {
    Configuration conf = new Configuration(false);

    // Default is false
    AccumuloConnectionParameters cnxnParams = new AccumuloConnectionParameters(conf);
    assertFalse(cnxnParams.useSasl());

    conf.set(AccumuloConnectionParameters.SASL_ENABLED, "true");

    cnxnParams = new AccumuloConnectionParameters(conf);

    assertTrue(cnxnParams.useSasl());

    conf.set(AccumuloConnectionParameters.SASL_ENABLED, "false");
    assertFalse(cnxnParams.useSasl());
  }
}
