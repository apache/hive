/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.tez;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Vertex;
import org.junit.Test;

import org.junit.Assert;

/**
 * 
 */
public class TestDagUtils {

  @Test
  public void testCredentialsNotOverwritten() throws Exception {
    final UserGroupInformation testUser = UserGroupInformation.createUserForTesting("test_user", new String[0]);
    final DagUtils dagUtils = DagUtils.getInstance();

    Credentials originalCredentials = new Credentials();
    final Text testTokenAlias = new Text("my_test_token");
    @SuppressWarnings("unchecked")
    Token<? extends TokenIdentifier> testToken = mock(Token.class);
    originalCredentials.addToken(testTokenAlias, testToken);
    Credentials testUserCredentials = new Credentials();

    testUser.addCredentials(testUserCredentials);

    final BaseWork work = mock(BaseWork.class);
    final DAG dag = DAG.create("test_credentials_dag");

    dag.setCredentials(originalCredentials);

    testUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        dagUtils.addCredentials(work, dag, null);
        return null;
      }
    });

    Token<? extends TokenIdentifier> actualToken = dag.getCredentials().getToken(testTokenAlias);
    assertEquals(testToken, actualToken);
  }

  @Test
  public void outputCommitterSetToDefaultIfNotPresent() throws IOException {
    DagUtils dagUtils = DagUtils.getInstance();
    HiveConf conf = new HiveConf();

    JobConf configuration = dagUtils.createConfiguration(conf);

    assertEquals(HiveFileFormatUtils.NullOutputCommitter.class.getName(),
        configuration.get("mapred.output.committer.class"));
  }

  @Test
  public void outputCommitterNotOverriddenIfPresent() throws IOException {
    DagUtils dagUtils = DagUtils.getInstance();
    HiveConf conf = new HiveConf();
    conf.set("mapred.output.committer.class", TestTezOutputCommitter.CountingOutputCommitter.class.getName());

    JobConf configuration = dagUtils.createConfiguration(conf);

    assertEquals(TestTezOutputCommitter.CountingOutputCommitter.class.getName(),
        configuration.get("mapred.output.committer.class"));
  }

  @Test
  public void testMapTezTaskEnvIsCopiedFromMrProperties() {
    final DagUtils dagUtils = DagUtils.getInstance();

    Vertex map = Vertex.create("mapWorkName", null);
    HiveConf conf = new HiveConf();
    Assert.assertNull(map.getTaskEnvironment().get("key"));

    conf.set(JobConf.MAPRED_MAP_TASK_ENV, "key=value");
    map.setTaskEnvironment(dagUtils.getContainerEnvironment(conf, true));

    Assert.assertEquals("value", map.getTaskEnvironment().get("key"));
  }

  @Test
  public void testReduceTezTaskEnvIsCopiedFromMrProperties() {
    final DagUtils dagUtils = DagUtils.getInstance();

    Vertex reduce = Vertex.create("reduceWorkName", null);
    HiveConf conf = new HiveConf();
    Assert.assertNull(reduce.getTaskEnvironment().get("key"));

    conf.set(JobConf.MAPRED_REDUCE_TASK_ENV, "key=value");
    reduce.setTaskEnvironment(dagUtils.getContainerEnvironment(conf, false));

    Assert.assertEquals("value", reduce.getTaskEnvironment().get("key"));
  }
}
