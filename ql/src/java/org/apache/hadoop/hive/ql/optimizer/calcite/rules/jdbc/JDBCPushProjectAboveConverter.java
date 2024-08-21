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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.jdbc;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This rule adds a Project on top of the HiveJdbcConverter when a project is not present 
 * either above or below the Converter. The idea is to follow this rule with
 * HiveFieldTrimmerRule (to trim the projects of the new Project) and then 
 * JDBCProjectPushDownRule (to push the trimmed Project below the Converter)
 */
public class JDBCPushProjectAboveConverter extends RelOptRule {
  private static final Logger LOG = LoggerFactory.getLogger(JDBCPushProjectAboveConverter.class);
  
  public static final JDBCPushProjectAboveConverter INSTANCE = new JDBCPushProjectAboveConverter();

  public JDBCPushProjectAboveConverter() {
    super(
        operand(RelNode.class, 
            operand(HiveJdbcConverter.class, 
                operand(RelNode.class, any())))
    );
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final RelNode parent = call.rel(0);
    final HiveJdbcConverter conv = call.rel(1);
    final RelNode child = call.rel(2);
    
    return !(parent instanceof Project) && !(child instanceof Project);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LOG.debug("JDBCPushProjectAboveConverter has been called");
    
    final RelNode parent = call.rel(0);
    final HiveJdbcConverter conv = call.rel(1);
    List<RexNode> projectList = HiveCalciteUtil.getProjsFromBelowAsInputRef(conv);

    try {
      HiveProject newProject = HiveProject.create(
          conv,
          projectList,
          null
      );
      call.transformTo(parent.copy(parent.getTraitSet(), ImmutableList.of(newProject)));
    } catch (CalciteSemanticException e) {
      throw new RuntimeException(e);
    }
  }
}
