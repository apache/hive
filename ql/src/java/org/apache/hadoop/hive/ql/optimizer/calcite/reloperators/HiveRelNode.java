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
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

public interface HiveRelNode extends RelNode {
  void implement(Implementor implementor);

  /** Calling convention for relational operations that occur in Hive. */
  final Convention CONVENTION = new Convention.Impl("HIVE", HiveRelNode.class);

  class Implementor {

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((HiveRelNode) input).implement(this);
    }
  }
}
