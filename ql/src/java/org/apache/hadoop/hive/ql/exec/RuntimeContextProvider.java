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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.exec.tez.TezRuntimeContext;

/**
 * A task that records runtime information about the query it runs.
 *
 * <p>Compilation collects that information through this interface, so it names no task type of an execution
 * engine and loads no engine class: a query that is compiled but never executed on Tez needs no Tez on the
 * classpath.
 */
public interface RuntimeContextProvider {

  TezRuntimeContext getRuntimeContext();
}
