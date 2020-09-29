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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.mutable.MutableLong;

/**
 * This listener gathers information about the rules being triggered
 * in the planning phase.
 */
public class RuleStatisticsListener implements RelOptListener {

  /**
   * Map that gathers the number of times that each rule was triggered
   * as well as the time spent in the transformation method (onMatch).
   */
  private final Map<String, Pair<MutableLong, MutableLong>> counterRulesTriggered =
      new HashMap<>();
  /**
   * Map that gathers the number of times that each rule produced a new
   * expression.
   */
  private final Map<String, MutableLong> counterTransformationsProduced =
      new HashMap<>();
  /**
   * This is a variable that is used to hold the current time before
   * a rule is triggered.
   */
  private long beforeTime;


  /**
   * Dump information that has been stored by the listener.
   */
  public String dump(String phase) {
    StringBuilder sb = new StringBuilder();
    sb.append("Dumping information for Calcite optimization : " + phase + " phase\n");
    LinkedHashMap<String, Pair<MutableLong, MutableLong>> sortedCounterRulesTriggered =
        new LinkedHashMap<>();
    counterRulesTriggered.entrySet()
        .stream()
        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
        .forEachOrdered(x -> {
          x.getValue().right.setValue(x.getValue().right.longValue() / 1_000); // To µs
          sortedCounterRulesTriggered.put(x.getKey(), x.getValue());
        });
    sb.append("Rules triggered (count, time in µs): " + sortedCounterRulesTriggered + "\n");
    LinkedHashMap<String, MutableLong> sortedCounterTransformationsProduced =
        new LinkedHashMap<>();
    counterTransformationsProduced.entrySet()
        .stream()
        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
        .forEachOrdered(x -> sortedCounterTransformationsProduced.put(x.getKey(), x.getValue()));
    sb.append("Transformations produced (count): " + sortedCounterTransformationsProduced);
    return sb.toString();
  }

  @Override
  public void relEquivalenceFound(RelEquivalenceEvent relEquivalenceEvent) {
    // Nothing to do
  }

  @Override
  public void ruleAttempted(RuleAttemptedEvent ruleAttemptedEvent) {
    if (ruleAttemptedEvent.isBefore()) {
      beforeTime = System.nanoTime();
    } else {
      // Only count after being triggered
      long elapsed = System.nanoTime() - beforeTime;
      String ruleName = ruleAttemptedEvent.getRuleCall().rule.toString();
      Pair<MutableLong, MutableLong> info = counterRulesTriggered.get(ruleName);
      if (info == null) {
        counterRulesTriggered.put(ruleName,
            Pair.of(new MutableLong(1L), new MutableLong(elapsed)));
      } else {
        info.left.increment();
        info.right.add(elapsed);
      }
    }
  }

  @Override
  public void ruleProductionSucceeded(RuleProductionEvent ruleProductionEvent) {
    // Only count after transform
    if (!ruleProductionEvent.isBefore()) {
      String ruleName = ruleProductionEvent.getRuleCall().rule.toString();
      MutableLong count = counterTransformationsProduced.get(ruleName);
      if (count == null) {
        counterTransformationsProduced.put(ruleName, new MutableLong(1));
      } else {
        count.increment();
      }
    }
  }

  @Override
  public void relDiscarded(RelDiscardedEvent relDiscardedEvent) {
    // Nothing to do
  }

  @Override
  public void relChosen(RelChosenEvent relChosenEvent) {
    // Nothing to do
  }
}
