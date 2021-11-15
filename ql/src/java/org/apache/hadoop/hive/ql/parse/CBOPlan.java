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

import org.apache.calcite.rel.RelNode;

/**
 * Wrapper of Calcite plan.
 */
public class CBOPlan {
  private final RelNode plan;
  private final String invalidAutomaticRewritingMaterializationReason;

  public CBOPlan(RelNode plan, String invalidAutomaticRewritingMaterializationReason) {
    this.plan = plan;
    this.invalidAutomaticRewritingMaterializationReason = invalidAutomaticRewritingMaterializationReason;
  }

  /**
   * Root node of plan.
   * @return Root {@link RelNode}
   */
  public RelNode getPlan() {
    return plan;
  }

  /**
   * Returns an error message if this plan can not be a definition of a Materialized view which is an input of
   * Calcite based materialized view query rewrite.
   * Null or empty string otherwise.
   * @return String contains error message or null.
   */
  public String getInvalidAutomaticRewritingMaterializationReason() {
    return invalidAutomaticRewritingMaterializationReason;
  }
}
