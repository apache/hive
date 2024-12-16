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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.List;

/**
 * Suggester for printing common table expressions to the Session's console.
 */
@InterfaceAudience.Private
public class CommonTableExpressionPrintSuggester implements CommonTableExpressionSuggester {

  private final CommonTableExpressionSuggester internal = new CommonTableExpressionIdentitySuggester();

  @Override
  public List<RelNode> suggest(final RelNode input, final Configuration configuration) {
    List<RelNode> result = internal.suggest(input, configuration);
    // Ensure CTEs are printed deterministically to avoid test flakiness
    result.stream().map(RelOptUtil::toString).sorted()
        .forEach(cte -> SessionState.getConsole().printInfo("CTE Suggestion:\n" + cte, false));
    return result;
  }

}
