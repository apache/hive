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
package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHook;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHookContext;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.common.log.InPlaceUpdate.MIN_TERMINAL_WIDTH;

public class QueryCompilationSummaryHook implements QueryLifeTimeHook {
    private static final String OPERATION_SUMMARY = "%-84s %9s";
    private static final String QUERY_COMPILATION_SUMMARY = "Query Compilation Summary";
    private static final String LINE_SEPARATOR = "line.separator";
    private static final String TOTAL_COMPILATION_TIME = "Total Compilation Time";
    private static final List<CompileStep> compileSteps = new ArrayList<>();
    private static final String SEPARATOR = new String(new char[MIN_TERMINAL_WIDTH]).replace("\0", "-");


    private String format(String value, long number) {
        return String.format(OPERATION_SUMMARY, value, number + "ms");
    }
    @Override
    public void beforeCompile(QueryLifeTimeHookContext ctx) {

    }

    @Override
    public void afterCompile(QueryLifeTimeHookContext ctx, boolean hasError) {
      printCompileSummary();
    }

    @Override
    public void beforeExecution(QueryLifeTimeHookContext ctx) {

    }

    @Override
    public void afterExecution(QueryLifeTimeHookContext ctx, boolean hasError) {

    }

    private static final class CompileStep {
      final private String name;
      final private List<CompileStep> subSteps;

      public CompileStep(String name) {
        this.name = name;
        subSteps = new ArrayList<>();
      }
      public void addSubStep(CompileStep subStep) {
        subSteps.add(subStep);
      }
      public List<CompileStep> getSubSteps() {
          return subSteps;
      }
    }

    static {
      compileSteps.add(new CompileStep(PerfLogger.PARSE));
      compileSteps.add(new CompileStep(PerfLogger.GENERATE_RESOLVED_PARSETREE));
      CompileStep logicalPlanAndOpTree = new CompileStep(PerfLogger.LOGICALPLAN_AND_HIVE_OPERATOR_TREE);
      CompileStep logicalPlan = new CompileStep(PerfLogger.GENERATE_LOGICAL_PLAN);
      logicalPlanAndOpTree.addSubStep(logicalPlan);
      logicalPlan.addSubStep(new CompileStep(PerfLogger.PLAN_GENERATION));
      logicalPlan.addSubStep(new CompileStep(PerfLogger.MV_REWRITE_FIELD_TRIMMER));
      logicalPlan.addSubStep(new CompileStep(PerfLogger.REMOVING_SUBQUERY));
      logicalPlan.addSubStep(new CompileStep(PerfLogger.DECORRELATION));
      logicalPlan.addSubStep(new CompileStep(PerfLogger.VALIDATE_QUERY_MATERIALIZATION));
      logicalPlan.addSubStep(new CompileStep(PerfLogger.PREJOIN_ORDERING));
      logicalPlan.addSubStep(new CompileStep(PerfLogger.MV_REWRITING));
      logicalPlan.addSubStep(new CompileStep(PerfLogger.JOIN_REORDERING));
      logicalPlan.addSubStep(new CompileStep(PerfLogger.POSTJOIN_ORDERING));
      logicalPlan.addSubStep(new CompileStep(PerfLogger.HIVE_SORT_PREDICATES));
      logicalPlanAndOpTree.addSubStep(new CompileStep(PerfLogger.GENERATE_OPERATOR_TREE));
      compileSteps.add(logicalPlanAndOpTree);
      compileSteps.add(new CompileStep(PerfLogger.DEDUCE_RESULTSET_SCHEMA));
      compileSteps.add(new CompileStep(PerfLogger.PARSE_CONTEXT_GENERATION));
      compileSteps.add(new CompileStep(PerfLogger.SAVE_AND_VALIDATE_VIEW));
      compileSteps.add(new CompileStep(PerfLogger.LOGICAL_OPTIMIZATION));
      compileSteps.add(new CompileStep(PerfLogger.PHYSICAL_OPTIMIZATION));
      compileSteps.add(new CompileStep(PerfLogger.POST_PROCESSING));
    }

    public void printCompileSummary() {
      StringBuilder compileSummary = new StringBuilder();
      compileSummary.append(QUERY_COMPILATION_SUMMARY);
      compileSummary.append(System.getProperty(LINE_SEPARATOR));
      compileSummary.append(SEPARATOR);
      compileSummary.append(System.getProperty(LINE_SEPARATOR));
      PerfLogger perfLogger = SessionState.getPerfLogger();
      appendCompileSteps(compileSummary, perfLogger, "", compileSteps);
      compileSummary.append(format(TOTAL_COMPILATION_TIME, perfLogger.getDuration(PerfLogger.COMPILE)));
      compileSummary.append(System.getProperty(LINE_SEPARATOR));
      compileSummary.append(SEPARATOR);
      SessionState.getConsole().printInfo(compileSummary.toString(), false);
    }

    public void appendCompileSteps(StringBuilder compileSummary, PerfLogger perfLogger, String prefix,
                                   List<CompileStep> currentLevelCompileSteps) {
      int counter = 1;
      for (CompileStep compileStep : currentLevelCompileSteps) {
        compileSummary.append(format(PerfLogger.COMPILE_STEP + " - " +
                        prefix + counter + " " + compileStep.name,
                    perfLogger.getDuration(compileStep.name)));
        compileSummary.append(System.getProperty(LINE_SEPARATOR));
        appendCompileSteps(compileSummary, perfLogger, prefix + counter + ".", compileStep.getSubSteps());
        ++counter;
      }
    }
}
