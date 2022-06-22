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

import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Listener for logging useful debugging information on certain rule events.
 */
public class RuleEventLogger implements RelOptListener {
  private static final Logger LOG = LoggerFactory.getLogger(RuleEventLogger.class.getName());
  private static final Marker FULL = MarkerFactory.getMarker("FULL_PLAN");

  public RuleEventLogger() {
    if (Bug.CALCITE_4704_FIXED) {
      throw new IllegalStateException("Class redundant after fix is merged into Calcite");
    }
  }

  @Override
  public void relEquivalenceFound(final RelEquivalenceEvent event) {

  }

  @Override
  public void ruleAttempted(final RuleAttemptedEvent event) {
    if (event.isBefore() && LOG.isDebugEnabled()) {
      RelOptRuleCall call = event.getRuleCall();
      String ruleArgs = Arrays.stream(call.rels).map(rel -> "rel#" + rel.getId() + ":" + rel.getRelTypeName())
          .collect(Collectors.joining(","));
      LOG.debug("call#{}: Apply rule [{}] to [{}]", call.id, call.getRule(), ruleArgs);
    }
  }

  @Override
  public void ruleProductionSucceeded(RuleProductionEvent event) {
    if (event.isBefore() && LOG.isDebugEnabled()) {
      RelOptRuleCall call = event.getRuleCall();

      Arrays.stream(call.rels).forEach(rel ->
          LOG.debug(FULL, "call#{}: Full plan for rule input [rel#{}:{}]: {}", call.id, rel.getId(),
              rel.getRelTypeName(), System.lineSeparator() + RelOptUtil.toString(rel)));

      RelNode newRel = event.getRel();
      String description = newRel == null ? "null" : "rel#" + newRel.getId() + ":" + newRel.getRelTypeName();
      LOG.debug("call#{}: Rule [{}] produced [{}]", call.id, call.getRule(), description);
      if (newRel != null) {
        LOG.debug(FULL, "call#{}: Full plan for [{}]:{}", call.id, description,
            System.lineSeparator() + RelOptUtil.toString(newRel));
      }
    }
  }

  @Override
  public void relDiscarded(final RelDiscardedEvent event) {

  }

  @Override
  public void relChosen(final RelChosenEvent event) {

  }
}
