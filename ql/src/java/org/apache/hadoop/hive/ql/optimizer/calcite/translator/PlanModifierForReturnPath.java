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
package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

public class PlanModifierForReturnPath {


  public static RelNode convertOpTree(RelNode rel, List<FieldSchema> resultSchema, boolean isCTAS)
          throws CalciteSemanticException {
    if (isCTAS) {
      rel = introduceProjectIfNeeded(rel, resultSchema);
    }
    RelNode newTopNode = rel;

    Pair<RelNode, RelNode> topSelparentPair = HiveCalciteUtil.getTopLevelSelect(newTopNode);
    PlanModifierUtil.fixTopOBSchema(newTopNode, topSelparentPair, resultSchema, false);

    return newTopNode;
  }

  private static RelNode introduceProjectIfNeeded(RelNode optimizedOptiqPlan,
      List<FieldSchema> resultSchema) throws CalciteSemanticException {
    List<String> fieldNames = new ArrayList<>();
    for (FieldSchema fieldSchema : resultSchema) {
      fieldNames.add(fieldSchema.getName());
    }
    RelNode newRoot = null;
    List<RexNode> projectList = null;

    if (!(optimizedOptiqPlan instanceof Project)) {
      projectList = HiveCalciteUtil.getProjsFromBelowAsInputRef(optimizedOptiqPlan);
      newRoot = HiveProject.create(optimizedOptiqPlan, projectList, fieldNames);
    } else {
      HiveProject project = (HiveProject) optimizedOptiqPlan;
      newRoot = HiveProject.create(project.getInput(0), project.getChildExps(), fieldNames);
    }
    return newRoot;
  }

}
