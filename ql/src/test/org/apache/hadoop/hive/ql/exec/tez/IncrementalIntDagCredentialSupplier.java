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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;

import java.util.Set;

/**
 * A credential supplier that creates and returns a new token based on an incrementing integer on every call.
 */
public class IncrementalIntDagCredentialSupplier implements DagCredentialSupplier {
  private static final Text ALIAS = new Text("test_token");
  private static final Text SERVICE = new Text("test_service");
  int tokenCalls = 0;

  @Override
  public Token<?> obtainToken(final BaseWork work, final Set<TableDesc> tables, final Configuration conf) {
    tokenCalls++;
    byte[] idpass = String.valueOf(tokenCalls).getBytes();
    return new Token<>(idpass, idpass, null, SERVICE);
  }

  @Override
  public Text getTokenAlias() {
    return ALIAS;
  }
}
