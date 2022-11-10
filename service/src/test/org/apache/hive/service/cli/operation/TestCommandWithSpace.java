/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.cli.operation;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.TestQueryHooks;
import org.apache.hadoop.hive.ql.processors.DfsProcessor;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.HiveSession;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCommandWithSpace {
    
    @Test
    public void testCommandWithPrefixSpace() throws IllegalAccessException, ClassNotFoundException, InstantiationException, HiveSQLException {
        String query = " dfs -ls /";
        HiveConf conf = new HiveConf();
        conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
        conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
                "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");

        SessionState.start(conf);
        HiveSession mockHiveSession = mock(HiveSession.class);
        when(mockHiveSession.getHiveConf()).thenReturn(conf);
        when(mockHiveSession.getSessionState()).thenReturn(SessionState.get());
        DfsProcessor dfsProcessor = new DfsProcessor(new Configuration());
        HiveCommandOperation sqlOperation = new HiveCommandOperation(mockHiveSession, query, dfsProcessor, ImmutableMap.of());
        sqlOperation.run();
    }

}
