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

package org.apache.hadoop.hive.ql.metadata;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.JobConf;

import java.io.Serializable;

/**
 * HiveStoragePredicateHandler is an optional companion to {@link
 * HiveStorageHandler}; it should only be implemented by handlers which
 * support decomposition of predicates being pushed down into table scans.
 */
public interface HiveStoragePredicateHandler {

  /**
   * Gives the storage handler a chance to decompose a predicate.  The storage
   * handler should analyze the predicate and return the portion of it which
   * cannot be evaluated during table access.  For example, if the original
   * predicate is <code>x = 2 AND upper(y)='YUM'</code>, the storage handler
   * might be able to handle <code>x = 2</code> but leave the "residual"
   * <code>upper(y)='YUM'</code> for Hive to deal with.  The breakdown
   * need not be non-overlapping; for example, given the
   * predicate <code>x LIKE 'a%b'</code>, the storage handler might
   * be able to evaluate the prefix search <code>x LIKE 'a%'</code>, leaving
   * <code>x LIKE '%b'</code> as the residual.
   *
   * @param jobConf contains a job configuration matching the one that
   * will later be passed to getRecordReader and getSplits
   *
   * @param deserializer deserializer which will be used when
   * fetching rows
   *
   * @param predicate predicate to be decomposed
   *
   * @return decomposed form of predicate, or null if no pushdown is
   * possible at all
   */
  public DecomposedPredicate decomposePredicate(
    JobConf jobConf,
    Deserializer deserializer,
    ExprNodeDesc predicate);

  /**
   * Struct class for returning multiple values from decomposePredicate.
   */
  public static class DecomposedPredicate {
    /**
     * Portion of predicate to be evaluated by storage handler.  Hive
     * will pass this into the storage handler's input format.
     */
    public ExprNodeGenericFuncDesc pushedPredicate;

    /**
     * Serialized format for filter
     */
    public Serializable pushedPredicateObject;

    /**
     * Portion of predicate to be post-evaluated by Hive for any rows
     * which are returned by storage handler.
     */
    public ExprNodeGenericFuncDesc residualPredicate;
  }
}
