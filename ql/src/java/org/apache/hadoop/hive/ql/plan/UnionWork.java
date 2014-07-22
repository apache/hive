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

package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.mapred.JobConf;

/**
 * Simple wrapper for union all cases. All contributing work for a union all
 * is collected here. Downstream work will connect to the union not the individual
 * work.
 */
public class UnionWork extends BaseWork {
  
  private final Set<UnionOperator> unionOperators = new HashSet<UnionOperator>();

  public UnionWork() {
    super();
  }
  
  public UnionWork(String name) {
    super(name);
  }

  @Explain(displayName = "Vertex")
  @Override
  public String getName() {
    return super.getName();
  }

  @Override
  public void replaceRoots(Map<Operator<?>, Operator<?>> replacementMap) {
  }

  @Override
  public Set<Operator<?>> getAllRootOperators() {
    return new HashSet<Operator<?>>();
  }

  public void addUnionOperators(Collection<UnionOperator> unions) {
    unionOperators.addAll(unions);
  }

  public Set<UnionOperator> getUnionOperators() {
    return unionOperators;
  }

  public void configureJobConf(JobConf job) {
  }
}
