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
package org.apache.hadoop.hive.metastore.txn.retryhandling;

import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.function.Function;

/**
 * Basic implementation of the {@link ParameterizedCommand} interface.
 */
public class SimpleParameterizedCommand extends SimpleParameterizedQuery implements ParameterizedCommand {
  
  private final Function<Integer, Boolean> resultPolicy;
  
  @Override
  public Function<Integer, Boolean> resultPolicy() {
    return resultPolicy;
  }

  public SimpleParameterizedCommand(String query, SqlParameterSource params, Function<Integer, Boolean> resultPolicy) {
    super(query, params);
    this.resultPolicy = resultPolicy;
  }
}
