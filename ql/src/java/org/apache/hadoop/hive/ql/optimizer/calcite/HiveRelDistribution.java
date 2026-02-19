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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelMultipleTrait;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mappings.TargetMapping;

import com.google.common.collect.Ordering;

public class HiveRelDistribution implements RelDistribution {

  private static final Ordering<Iterable<Integer>> ORDERING = Ordering.<Integer>natural().lexicographical();

  public static HiveRelDistribution from(
          List<RelFieldCollation> fieldCollations, RelDistribution.Type distributionType) {
    List<Integer> newDistributionKeys = new ArrayList<>(fieldCollations.size());
    for (RelFieldCollation fieldCollation : fieldCollations) {
      newDistributionKeys.add(fieldCollation.getFieldIndex());
    }
    return new HiveRelDistribution(distributionType, newDistributionKeys);
  }

  List<Integer> keys;
  RelDistribution.Type type;

  public HiveRelDistribution(Type type, List<Integer> keys) {
    this.type = type;
    this.keys = keys;
  }

  @Override
  public RelTraitDef<?> getTraitDef() {
    return RelDistributionTraitDef.INSTANCE;
  }

  @Override
  public void register(RelOptPlanner planner) {

  }

  @Override
  public boolean satisfies(RelTrait trait) {
    if (trait == this) {
      return true;
    }
    switch (((RelDistribution)trait).getType()) {
      case HASH_DISTRIBUTED :
        return this.getKeys().equals(((RelDistribution)trait).getKeys());
      default:
        throw new RuntimeException("Other distributions are not used yet.");
    }
  }

  @Override
  public RelDistribution apply(TargetMapping mapping) {
    if (keys.isEmpty()) {
      return this;
    }
    List<Integer> newKeys = new ArrayList<>(keys.size());

    if (Bug.CALCITE_4166_FIXED) {
      throw new AssertionError("Remove logic in HiveRelDistribution when [CALCITE-4166] "
          + "has been fixed and use newKeys.add(mapping.getTargetOpt(key)); instead.");
    }

    Map<Integer, Integer> tmp = new HashMap<>(mapping.getSourceCount());
    for (IntPair aMapping : mapping) {
      tmp.put(aMapping.source, aMapping.target);
    }

    for (Integer key : keys) {
      newKeys.add(tmp.get(key));
    }
    return new HiveRelDistribution(type, newKeys);
  }

  @Override
  public List<Integer> getKeys() {
    return keys;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public boolean isTop() {
    return type == Type.ANY;
  }

  @Override
  public int compareTo(RelMultipleTrait o) {
    final RelDistribution distribution = (RelDistribution) o;
    if (type == distribution.getType()
        && (type == Type.HASH_DISTRIBUTED
            || type == Type.RANGE_DISTRIBUTED)) {
      return ORDERING.compare(getKeys(), distribution.getKeys());
    }

    return type.compareTo(distribution.getType());
  }

  @Override
  public String toString() {
    if (keys.isEmpty()) {
      return type.shortName;
    } else {
      return type.shortName + keys;
    }
  }
}
