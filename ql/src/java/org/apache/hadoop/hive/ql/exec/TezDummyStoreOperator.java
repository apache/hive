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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * A dummy store operator same as the dummy store operator but for tez. This is required so that we
 * don't check for tez everytime before forwarding a record. In tez records flow down from the dummy
 * store operator in processOp phase unlike in map reduce.
 *
 */
public class TezDummyStoreOperator extends DummyStoreOperator {
  /** Kryo ctor. */
  protected TezDummyStoreOperator() {
    super();
  }

  public TezDummyStoreOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  private boolean fetchDone = false;

  /**
   * Unlike the MR counterpoint, on Tez we want processOp to forward
   * the records.
   */
  @Override
  public void process(Object row, int tag) throws HiveException {
    super.process(row, tag);
    forward(result.o, outputObjInspector);
  }

  public boolean getFetchDone() {
    return fetchDone;
  }

  public void setFetchDone(boolean fetchDone) {
    this.fetchDone = fetchDone;
  }
}
