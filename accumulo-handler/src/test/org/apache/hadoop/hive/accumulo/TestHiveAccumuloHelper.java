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

import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestHiveAccumuloHelper {

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

    helper.mergeTokenIntoJobConf(jobConf, token);

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

    helper.addTokenFromUserToJobConf(ugi, jobConf);

    Collection<Token<?>> credTokens = jobConf.getCredentials().getAllTokens();
    assertEquals(1, credTokens.size());
    assertEquals(service, credTokens.iterator().next().getService());
  }
}
