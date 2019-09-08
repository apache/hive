/*
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

package org.apache.hadoop.hive.ql.ddl.privilege;

import java.lang.reflect.Constructor;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.authorization.HiveAuthorizationTaskFactory;
import org.apache.hadoop.hive.ql.parse.authorization.HiveAuthorizationTaskFactoryImpl;

import com.google.common.annotations.VisibleForTesting;

/**
 * Abstract analyzer for all privilege related commands.
 */
public abstract class AbstractPrivilegeAnalyzer extends BaseSemanticAnalyzer {
  protected final HiveAuthorizationTaskFactory hiveAuthorizationTaskFactory;

  public AbstractPrivilegeAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
    hiveAuthorizationTaskFactory = createAuthorizationTaskFactory(conf, db);
  }

  @VisibleForTesting
  public AbstractPrivilegeAnalyzer(QueryState queryState, Hive db) throws SemanticException {
    super(queryState, db);
    hiveAuthorizationTaskFactory = createAuthorizationTaskFactory(conf, db);
  }

  private HiveAuthorizationTaskFactory createAuthorizationTaskFactory(HiveConf conf, Hive db) {
    Class<? extends HiveAuthorizationTaskFactory> authProviderClass = conf.getClass(
        HiveConf.ConfVars.HIVE_AUTHORIZATION_TASK_FACTORY.varname, HiveAuthorizationTaskFactoryImpl.class,
        HiveAuthorizationTaskFactory.class);

    try {
      Constructor<? extends HiveAuthorizationTaskFactory> constructor =
          authProviderClass.getConstructor(HiveConf.class, Hive.class);
      return constructor.newInstance(conf, db);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to create instance of " + authProviderClass.getName() + ": " + e.getMessage(), e);
    }
  }
}
