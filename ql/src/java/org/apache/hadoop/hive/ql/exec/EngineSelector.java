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

package org.apache.hadoop.hive.ql.exec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.TypeCheckCtx;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;

public interface EngineSelector {

  enum Engine {TEZ, SPARK, MR, DEFAULT}

  /**
   * select engine to be used, can return null for default value
   *
   * @param conf
   * @param parseContext
   * @param param
   * @param available active engines for selection
   * @return
   */
  Engine select(HiveConf conf, ParseContext parseContext, String param, EnumSet<Engine> available);

  public static class SimpleSelector implements EngineSelector {

    static final Log LOG = LogFactory.getLog(EngineSelector.class.getName());

    @Override
    public Engine select(HiveConf conf, ParseContext pctx, String param, EnumSet<Engine> available) {
      Map<String, PrunedPartitionList> pruned = pctx.getPrunedPartitions();
      Map<String, Operator<?>> topOps = pctx.getTopOps();

      try {
        long total = 0;
        long biggest = 0;
        for (Map.Entry<String, Operator<?>> entry : topOps.entrySet()) {
          MapWork dummy = new MapWork();
          String alias = entry.getKey();
          Operator<?> operator = entry.getValue();
          GenMapRedUtils.setMapWork(dummy, pctx, new HashSet<ReadEntity>(),
              pruned.get(alias), operator, alias, conf, false);

          ContentSummary summary = Utilities.getInputSummary(pctx.getContext(), dummy, null);
          if (!summary.isValidSummary()) {
            LOG.info("Cannot estimate the size of " + alias);
            total = -1;
            break;
          }
          total += summary.getLength();
          biggest = Math.max(biggest, summary.getLength());
        }
        // spark = $total < 10gb && $num_aliases < 4, tez
        param = param.replaceAll("$num_aliases", String.valueOf(topOps.size()));
        param = param.replaceAll("$total", String.valueOf(total));
        param = param.replaceAll("$biggest", String.valueOf(biggest));
        param = param.replaceAll("(\\d)+( )?kb", "$1 * 1024L");
        param = param.replaceAll("(\\d)+( )?mb", "$1 * 1024L * 1024L");
        param = param.replaceAll("(\\d)+( )?gb", "$1 * 1024L * 1024L * 1024L");
        param = param.replaceAll("(\\d)+( )?tb", "$1 * 1024L * 1024L * 1024L * 1024L");

        Engine selected = evaluate(available, param.trim(), total < 0);
        LOG.info("Execution engine selected " + selected);
        return selected;
      } catch (Exception e) {
        LOG.warn("Failed to select engine by exception ", e);
      }
      return Engine.DEFAULT;
    }

    Engine evaluate(EnumSet<Engine> available, String expressions, boolean skipEval) throws Exception {
      ParseDriver driver = new ParseDriver();
      CalcitePlanner.ASTSearcher searcher = new CalcitePlanner.ASTSearcher();

      TypeCheckCtx ctx = new TypeCheckCtx(new RowResolver());
      for (String expression : expressions.split(",")) {
        expression = expression.trim();
        String[] split = expression.split("=");
        if (split.length == 1) {
          Engine engine = engineOfName(split[0]);
          if (available.contains(engine)) {
            return engine;
          }
          continue;
        }
        if (skipEval) {
          continue;
        }
        // hack
        ASTNode astNode = driver.parseSelect("select " + split[1], null);

        ASTNode target = (ASTNode) searcher.simpleBreadthFirstSearch(
            astNode, HiveParser.TOK_SELEXPR).getChild(0);
        ExprNodeDesc expr = TypeCheckProcFactory.genExprNode(target, ctx).get(target);
        ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory.get(expr);
        evaluator.initialize(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
        BooleanObjectInspector inspector = (BooleanObjectInspector) evaluator.getOutputOI();
        if (inspector.get(evaluator.evaluate(null))) {
          Engine engine = engineOfName(split[0]);
          if (available.contains(engine)) {
            return engine;
          }
        }
      }
      return null;
    }

    private Engine engineOfName(String engineName) {
      return Engine.valueOf(engineName.trim().toUpperCase());
    }
  }
}
