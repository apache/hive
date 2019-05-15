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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.HashTableSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;

import java.io.IOException;

public class TemporaryHashSinkOperator extends HashTableSinkOperator {
  public TemporaryHashSinkOperator(CompilationOpContext ctx, MapJoinDesc desc) {
    super(ctx);
    conf = new HashTableSinkDesc(desc);

    // Sanity check the config.
    assert conf.getHashtableMemoryUsage() != 0;
    if (conf.getHashtableMemoryUsage() == 0) {
      LOG.error("Hash table memory usage not set in map join operator; non-staged load may fail");
    }
  }

  @Override
  protected void flushToFile() throws IOException, HiveException {
    // do nothing
  }
}
