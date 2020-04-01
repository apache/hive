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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class BucketVersionPopulator extends Transform {

  protected static final Logger LOG = LoggerFactory.getLogger(BucketVersionPopulator.class);

  protected ParseContext pGraphContext;

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    pGraphContext = pctx;
    buildClusters();
    return pctx;
  }

  private void buildClusters() {
    Queue<Operator<?>> q = new LinkedList<>();
    Set<Operator<?>> visited = Sets.newIdentityHashSet();
    q.addAll(pGraphContext.getTopOps().values());

    while (!q.isEmpty()) {
      Operator<?> o = q.peek();
      if (visited.contains(o)) {
        continue;
      }
      q.addAll(o.getChildOperators());

    }
  }
}
