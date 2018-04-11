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

package org.apache.hadoop.hive.ql.parse;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitioningSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowExpressionSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowFunctionSpec;

/*
 * breakup the original WindowingSpec into a set of WindowingSpecs.
 * Each WindowingSpec is executed in an instance of PTFOperator,
 * preceded by ReduceSink and Extract.
 * The logic to componentize is straightforward:
 * - distribute Window Fn. Specs from original Window Spec into a set of WindowSpecs,
 *   based on their Partitioning.
 * - A Group of WindowSpecs, is a subset of the Window Fn Invocations in the QueryBlock that
 *   have the same Partitioning(Partition + Order spec).
 * - Each Group is put in a new WindowingSpec and is evaluated in its own PTFOperator instance.
 * - the order of computation is then inferred based on the dependency between Groups.
 *   If 2 groups have the same dependency, then the Group with the function that is
 *   earliest in the SelectList is executed first.
 */
public class WindowingComponentizer {

  WindowingSpec originalSpec;
  LinkedHashMap<PartitioningSpec, WindowingSpec> groups;

  public WindowingComponentizer(WindowingSpec originalSpec) throws SemanticException {
    super();
    this.originalSpec = originalSpec;
    groups = new LinkedHashMap<PartitioningSpec, WindowingSpec>();
    groupFunctions();
  }

  private void groupFunctions() throws SemanticException {
    for (WindowExpressionSpec expr : originalSpec.getWindowExpressions()) {
      WindowFunctionSpec wFn = (WindowFunctionSpec) expr;
      PartitioningSpec wFnGrp = wFn.getWindowSpec().getPartitioning();
      WindowingSpec wSpec = groups.get(wFnGrp);
      if (wSpec == null) {
        wSpec = new WindowingSpec();
        groups.put(wFnGrp, wSpec);
      }
      wSpec.addWindowFunction(wFn);
    }
  }

  public boolean hasNext() {
    return !groups.isEmpty();
  }

  public WindowingSpec next(HiveConf hCfg,
      SemanticAnalyzer semAly,
      UnparseTranslator unparseT,
      RowResolver inputRR) throws SemanticException {

    SemanticException originalException = null;

    Iterator<Map.Entry<PartitioningSpec, WindowingSpec>> grpIt = groups.entrySet().iterator();
    while (grpIt.hasNext()) {
      Map.Entry<PartitioningSpec, WindowingSpec> entry = grpIt.next();
      WindowingSpec wSpec = entry.getValue();
      try {
        PTFTranslator t = new PTFTranslator();
        t.translate(wSpec, semAly, hCfg, inputRR, unparseT);
        groups.remove(entry.getKey());
        return wSpec;
      } catch (SemanticException se) {
        originalException = se;
      }
    }

    throw new SemanticException("Failed to breakup Windowing invocations into Groups. " +
        "At least 1 group must only depend on input columns. " +
        "Also check for circular dependencies.\n" +
        "Underlying error: " + originalException.getMessage());
  }

}
