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
import org.apache.hadoop.hive.common.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;

import java.util.Set;

/**
 * A supplier of credentials for Tez dags.
 */
@Private
public interface DagCredentialSupplier {
  /**
   * Obtains a token for the specified unit of work, tables, and configuration.
   * @return a valid token or null if such token cannot be supplied.
   */
  Token<?> obtainToken(BaseWork work, Set<TableDesc> tables, Configuration conf);

  /**
   * Returns the alias for tokens obtained by this supplier.
   * <p>It returns always the same value and never null.</p>
   * @return the alias for tokens obtained by this supplier.
   */
  Text getTokenAlias();
}
