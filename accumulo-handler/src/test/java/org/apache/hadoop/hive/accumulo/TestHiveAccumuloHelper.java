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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapred.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestHiveAccumuloHelper {
  private static final Logger log = Logger.getLogger(TestHiveAccumuloHelper.class);

  private HiveAccumuloHelper helper;

  @Before
  public void setup() {
    helper = new HiveAccumuloHelper();
  }

  @Test
  public void testTokenMerge() throws Exception {
    final Text service = new Text("service");
    Token<?> token = Mockito.mock(Token.class);
    JobConf jobConf = new JobConf();

    Mockito.when(token.getService()).thenReturn(service);

    try {
      helper.mergeTokenIntoJobConf(jobConf, token);
    } catch (IOException e) {
      // Hadoop 1 doesn't support credential merging, so this will fail.
      log.info("Ignoring exception, likely coming from Hadoop 1", e);
      return;
    }

    Collection<Token<?>> tokens = jobConf.getCredentials().getAllTokens();
    assertEquals(1, tokens.size());
    assertEquals(service, tokens.iterator().next().getService());
  }

  @Test
  public void testTokenToConfFromUser() throws Exception {
    UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
    JobConf jobConf = new JobConf();
    ArrayList<Token<?>> tokens = new ArrayList<>();
    Text service = new Text("service");
    Token<?> token = Mockito.mock(Token.class);
    tokens.add(token);

    Mockito.when(ugi.getTokens()).thenReturn(tokens);
    Mockito.when(token.getKind()).thenReturn(HiveAccumuloHelper.ACCUMULO_SERVICE);
    Mockito.when(token.getService()).thenReturn(service);

    try {
      helper.addTokenFromUserToJobConf(ugi, jobConf);
    } catch (IOException e) {
      // Hadoop 1 doesn't support credential merging, so this will fail.
      log.info("Ignoring exception, likely coming from Hadoop 1", e);
      return;
    }

    Collection<Token<?>> credTokens = jobConf.getCredentials().getAllTokens();
    assertEquals(1, credTokens.size());
    assertEquals(service, credTokens.iterator().next().getService());
  }

  @Test
  public void testInputFormatWithKerberosToken() throws Exception {
    final JobConf jobConf = new JobConf();
    final HiveAccumuloHelper helper = Mockito.mock(HiveAccumuloHelper.class);
    final AuthenticationToken authToken = Mockito.mock(AuthenticationToken.class);
    final Token hadoopToken = Mockito.mock(Token.class);
    final AccumuloConnectionParameters cnxnParams = Mockito.mock(AccumuloConnectionParameters.class);
    final Connector connector = Mockito.mock(Connector.class);

    final String user = "bob";
    final String instanceName = "accumulo";
    final String zookeepers = "host1:2181,host2:2181,host3:2181";
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(user, new String[0]);

    // Call the real methods for these
    Mockito.doCallRealMethod().when(helper).updateOutputFormatConfWithAccumuloToken(jobConf, ugi, cnxnParams);
    Mockito.doCallRealMethod().when(helper).updateInputFormatConfWithAccumuloToken(jobConf, ugi, cnxnParams);
    Mockito.doCallRealMethod().when(helper).updateConfWithAccumuloToken(jobConf, ugi, cnxnParams, true);

    // Return our mocked objects
    Mockito.when(cnxnParams.getConnector()).thenReturn(connector);
    Mockito.when(helper.getDelegationToken(connector)).thenReturn(authToken);
    Mockito.when(helper.getHadoopToken(authToken)).thenReturn(hadoopToken);

    // Stub AccumuloConnectionParameters actions
    Mockito.when(cnxnParams.useSasl()).thenReturn(true);
    Mockito.when(cnxnParams.getAccumuloUserName()).thenReturn(user);
    Mockito.when(cnxnParams.getAccumuloInstanceName()).thenReturn(instanceName);
    Mockito.when(cnxnParams.getZooKeepers()).thenReturn(zookeepers);

    // Test the InputFormat execution path
    //
    Mockito.when(helper.hasKerberosCredentials(ugi)).thenReturn(true);
    // Invoke the InputFormat entrypoint
    helper.updateInputFormatConfWithAccumuloToken(jobConf, ugi, cnxnParams);
    Mockito.verify(helper).setInputFormatConnectorInfo(jobConf, user, authToken);
    Mockito.verify(helper).mergeTokenIntoJobConf(jobConf, hadoopToken);
    Mockito.verify(helper).addTokenFromUserToJobConf(ugi, jobConf);

    // Make sure the token made it into the UGI
    Collection<Token<? extends TokenIdentifier>> tokens = ugi.getTokens();
    Assert.assertEquals(1, tokens.size());
    Assert.assertEquals(hadoopToken, tokens.iterator().next());
  }

  @Test
  public void testInputFormatWithoutKerberosToken() throws Exception {
    final JobConf jobConf = new JobConf();
    final HiveAccumuloHelper helper = Mockito.mock(HiveAccumuloHelper.class);
    final AuthenticationToken authToken = Mockito.mock(AuthenticationToken.class);
    final Token hadoopToken = Mockito.mock(Token.class);
    final AccumuloConnectionParameters cnxnParams = Mockito.mock(AccumuloConnectionParameters.class);
    final Connector connector = Mockito.mock(Connector.class);

    final String user = "bob";
    final String instanceName = "accumulo";
    final String zookeepers = "host1:2181,host2:2181,host3:2181";
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(user, new String[0]);

    // Call the real methods for these
    Mockito.doCallRealMethod().when(helper).updateOutputFormatConfWithAccumuloToken(jobConf, ugi, cnxnParams);
    Mockito.doCallRealMethod().when(helper).updateInputFormatConfWithAccumuloToken(jobConf, ugi, cnxnParams);
    Mockito.doCallRealMethod().when(helper).updateConfWithAccumuloToken(jobConf, ugi, cnxnParams, true);

    // Return our mocked objects
    Mockito.when(cnxnParams.getConnector()).thenReturn(connector);
    Mockito.when(helper.getDelegationToken(connector)).thenReturn(authToken);
    Mockito.when(helper.getHadoopToken(authToken)).thenReturn(hadoopToken);

    // Stub AccumuloConnectionParameters actions
    Mockito.when(cnxnParams.useSasl()).thenReturn(true);
    Mockito.when(cnxnParams.getAccumuloUserName()).thenReturn(user);
    Mockito.when(cnxnParams.getAccumuloInstanceName()).thenReturn(instanceName);
    Mockito.when(cnxnParams.getZooKeepers()).thenReturn(zookeepers);

    // Verify that when we have no kerberos credentials, we pull the serialized Token
    Mockito.when(helper.hasKerberosCredentials(ugi)).thenReturn(false);
    helper.updateInputFormatConfWithAccumuloToken(jobConf, ugi, cnxnParams);
    Mockito.verify(helper).addTokenFromUserToJobConf(ugi, jobConf);
  }

  @Test
  public void testOutputFormatSaslConfigurationWithoutKerberosToken() throws Exception {
    final JobConf jobConf = new JobConf();
    final HiveAccumuloHelper helper = Mockito.mock(HiveAccumuloHelper.class);
    final AuthenticationToken authToken = Mockito.mock(AuthenticationToken.class);
    final Token hadoopToken = Mockito.mock(Token.class);
    final AccumuloConnectionParameters cnxnParams = Mockito.mock(AccumuloConnectionParameters.class);
    final Connector connector = Mockito.mock(Connector.class);

    final String user = "bob";
    final String instanceName = "accumulo";
    final String zookeepers = "host1:2181,host2:2181,host3:2181";
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(user, new String[0]);

    // Call the real methods for these
    Mockito.doCallRealMethod().when(helper).updateOutputFormatConfWithAccumuloToken(jobConf, ugi, cnxnParams);
    Mockito.doCallRealMethod().when(helper).updateConfWithAccumuloToken(jobConf, ugi, cnxnParams, false);

    // Return our mocked objects
    Mockito.when(cnxnParams.getConnector()).thenReturn(connector);
    Mockito.when(helper.getDelegationToken(connector)).thenReturn(authToken);
    Mockito.when(helper.getHadoopToken(authToken)).thenReturn(hadoopToken);

    // Stub AccumuloConnectionParameters actions
    Mockito.when(cnxnParams.useSasl()).thenReturn(true);
    Mockito.when(cnxnParams.getAccumuloUserName()).thenReturn(user);
    Mockito.when(cnxnParams.getAccumuloInstanceName()).thenReturn(instanceName);
    Mockito.when(cnxnParams.getZooKeepers()).thenReturn(zookeepers);

    // Verify that when we have no kerberos credentials, we pull the serialized Token
    Mockito.when(helper.hasKerberosCredentials(ugi)).thenReturn(false);
    helper.updateOutputFormatConfWithAccumuloToken(jobConf, ugi, cnxnParams);
    Mockito.verify(helper).addTokenFromUserToJobConf(ugi, jobConf);
  }

  @Test
  public void testOutputFormatSaslConfigurationWithKerberosToken() throws Exception {
    final JobConf jobConf = new JobConf();
    final HiveAccumuloHelper helper = Mockito.mock(HiveAccumuloHelper.class);
    final AuthenticationToken authToken = Mockito.mock(AuthenticationToken.class);
    final Token hadoopToken = Mockito.mock(Token.class);
    final AccumuloConnectionParameters cnxnParams = Mockito.mock(AccumuloConnectionParameters.class);
    final Connector connector = Mockito.mock(Connector.class);

    final String user = "bob";
    final String instanceName = "accumulo";
    final String zookeepers = "host1:2181,host2:2181,host3:2181";
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(user, new String[0]);

    // Call the real methods for these
    Mockito.doCallRealMethod().when(helper).updateOutputFormatConfWithAccumuloToken(jobConf, ugi, cnxnParams);
    Mockito.doCallRealMethod().when(helper).updateConfWithAccumuloToken(jobConf, ugi, cnxnParams, false);

    // Return our mocked objects
    Mockito.when(cnxnParams.getConnector()).thenReturn(connector);
    Mockito.when(helper.getDelegationToken(connector)).thenReturn(authToken);
    Mockito.when(helper.getHadoopToken(authToken)).thenReturn(hadoopToken);

    // Stub AccumuloConnectionParameters actions
    Mockito.when(cnxnParams.useSasl()).thenReturn(true);
    Mockito.when(cnxnParams.getAccumuloUserName()).thenReturn(user);
    Mockito.when(cnxnParams.getAccumuloInstanceName()).thenReturn(instanceName);
    Mockito.when(cnxnParams.getZooKeepers()).thenReturn(zookeepers);

    // We have kerberos credentials

    Mockito.when(helper.hasKerberosCredentials(ugi)).thenReturn(true);
    // Invoke the OutputFormat entrypoint
    helper.updateOutputFormatConfWithAccumuloToken(jobConf, ugi, cnxnParams);
    Mockito.verify(helper).setOutputFormatConnectorInfo(jobConf, user, authToken);
    Mockito.verify(helper).mergeTokenIntoJobConf(jobConf, hadoopToken);
    Mockito.verify(helper).addTokenFromUserToJobConf(ugi, jobConf);

    // Make sure the token made it into the UGI
    Collection<Token<? extends TokenIdentifier>> tokens = ugi.getTokens();
    Assert.assertEquals(1, tokens.size());
    Assert.assertEquals(hadoopToken, tokens.iterator().next());
  }
}
