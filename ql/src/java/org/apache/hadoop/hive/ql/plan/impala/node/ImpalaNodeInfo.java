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
package org.apache.hadoop.hive.ql.plan.impala.node;

import com.google.common.base.Preconditions;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.TupleDescriptor;

import java.util.ArrayList;
import java.util.List;

/**
 * ImpalaNodeInfo is used to transfer node specific information from the ImpalaPlanRel
 * (which is part of the RelNode hierarchy) to the Impala's PlanNodes. This includes
 * statistics, assigned conjuncts, Impala's TupleDescriptor etc.
 */
public class ImpalaNodeInfo {
  // average row size in bytes of the output schema of this plan node (initialized with
  // a default for now .. this will eventually be populated from Hive stats)
  private float avgRowSize = 10.0f;
  // output cardinality of this plan node (initialized with a default for now .. this
  // will eventually be populated from Hive stats)
  private long cardinality = 100L;
  // list of assigned conjuncts that should be evaluated by this plan node
  private List<Expr> assignedConjuncts = new ArrayList<>();
  // tuple descriptor that describes the output tuple of this plan node
  private TupleDescriptor tupleDesc;

  public ImpalaNodeInfo(List<Expr> assignedConjuncts, TupleDescriptor tupleDesc) {
    this.assignedConjuncts = assignedConjuncts;
    this.tupleDesc = tupleDesc;
  }

  public ImpalaNodeInfo() {}

  public void setStats(float avgRowSize, long cardinality) {
    this.avgRowSize = avgRowSize;
    this.cardinality = cardinality;
  }

  public void setTupleDesc(TupleDescriptor desc) {
    tupleDesc = desc;
  }

  public TupleDescriptor getTupleDesc() {
    return tupleDesc;
  }

  public float getAvgRowSize() {
    return avgRowSize;
  }

  public long  getCardinality() {
    return cardinality;
  }

  public List<Expr> getAssignedConjuncts() {
    return assignedConjuncts;
  }

  public void setAssignedConjuncts(List<Expr> conjs) {
    Preconditions.checkState(assignedConjuncts.size() == 0);
    assignedConjuncts = conjs;
  }

}
