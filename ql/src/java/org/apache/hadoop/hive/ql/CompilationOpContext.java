/**
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

package org.apache.hadoop.hive.ql;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A subset of compilation context that is passed to operators to get rid of some globals.
 * Perhaps this should be rolled into main Context; however, some code necessitates storing the
 * context in the operators for now, so this may not be advisable given how much stuff the main
 * Context class contains.
 * For now, only the operator sequence ID lives here.
 */
public class CompilationOpContext {
  private final AtomicInteger opSeqId = new AtomicInteger(0);

  public int nextOperatorId() {
    return opSeqId.getAndIncrement();
  }
}