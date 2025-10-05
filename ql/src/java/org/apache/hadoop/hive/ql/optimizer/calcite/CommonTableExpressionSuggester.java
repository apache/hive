/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;

import java.util.List;

/**
 * Suggester for finding and returning interesting expressions that appear more than once in a query. The notion of
 * "interesting" is specific to the actual implementation of this interface.
 * <p>In some cases the interesting expressions may be readily available in the input query while in others they could be
 * revealed after applying various transformations and algebraic equivalences.</p>
 * <p>The final decision about using (or not) the suggestions provided by this class lies to the query optimizer. For
 * various reasons (e.g, incorrect or expensive expressions) the optimizer may choose to reject/ignore the results
 * provided by this class.</p>
 * <p>Implementations of this interface must have a public no argument constructor since they are instantiated via reflection.
 * The class is instantiated once for each query under compilation and then thrown away.</p>
 * <p>The interface is experimental and subject to change without notice.</p>
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface CommonTableExpressionSuggester {
  /**
   * Suggests interesting expressions for the specified query and configuration.
   * @param input a relational expression representing the query under compilation.
   * @param configuration the configuration to use for identifying and returning interesting expressions.
   * @return a list with interesting expressions for the specified query
   */
  List<RelNode> suggest(RelNode input, Configuration configuration);
}
