/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.benchmark.vectorization;

import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.openjdk.jmh.infra.Blackhole;

public class BlackholeOperator extends Operator {
  private Blackhole bh;

  public BlackholeOperator(CompilationOpContext cContext,  Blackhole bh) {
    super(cContext);
    this.bh = bh;
  }

  @Override
  public void process(final Object row, final int tag) throws HiveException {
    bh.consume(row);
  }

  @Override
  public String getName() {
    return "Blackhole Operator";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.FILESINK;
  }
}
