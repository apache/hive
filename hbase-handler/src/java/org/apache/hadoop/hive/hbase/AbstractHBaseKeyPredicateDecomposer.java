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
package org.apache.hadoop.hive.hbase;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

/**
 * Simple abstract class to help with creation of a {@link DecomposedPredicate}. In order to create
 * one, consumers should extend this class and override the "getScanRange" method to define the
 * start/stop keys and/or filters on their hbase scans
 * */
public abstract class AbstractHBaseKeyPredicateDecomposer {

  public static final Logger LOG = LoggerFactory.getLogger(AbstractHBaseKeyPredicateDecomposer.class);

  public DecomposedPredicate decomposePredicate(String keyColName, ExprNodeDesc predicate) {
    IndexPredicateAnalyzer analyzer = IndexPredicateAnalyzer.createAnalyzer(true);
    analyzer.allowColumnName(keyColName);
    analyzer.setAcceptsFields(true);
    analyzer.setFieldValidator(getFieldValidator());

    DecomposedPredicate decomposed = new DecomposedPredicate();

    List<IndexSearchCondition> conditions = new ArrayList<IndexSearchCondition>();
    decomposed.residualPredicate =
        (ExprNodeGenericFuncDesc) analyzer.analyzePredicate(predicate, conditions);
    if (!conditions.isEmpty()) {
      decomposed.pushedPredicate = analyzer.translateSearchConditions(conditions);
      try {
        decomposed.pushedPredicateObject = getScanRange(conditions);
      } catch (Exception e) {
        LOG.warn("Failed to decompose predicates", e);
        return null;
      }
    }

    return decomposed;
  }

  /**
   * Get the scan range that specifies the start/stop keys and/or filters to be applied onto the
   * hbase scan
   * */
  protected abstract HBaseScanRange getScanRange(List<IndexSearchCondition> searchConditions)
      throws Exception;

  /**
   * Get an optional {@link IndexPredicateAnalyzer.FieldValidator validator}. A validator can be
   * used to optinally filter out the predicates which need not be decomposed. By default this
   * method returns {@code null} which means that all predicates are pushed but consumers can choose
   * to override this to provide a custom validator as well.
   * */
  protected IndexPredicateAnalyzer.FieldValidator getFieldValidator() {
    return null;
  }
}
