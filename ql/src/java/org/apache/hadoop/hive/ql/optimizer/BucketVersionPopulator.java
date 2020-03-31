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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * If two reducer sink operators share the same partition/sort columns and order,
 * they can be merged. This should happen after map join optimization because map
 * join optimization will remove reduce sink operators.
 *
 * This optimizer removes/replaces child-RS (not parent) which is safer way for DefaultGraphWalker.
 */
public class BucketVersionPopulator extends Transform {

  protected static final Logger LOG = LoggerFactory.getLogger(BucketVersionPopulator.class);

  protected ParseContext pGraphContext;

  static class BucketingVersionResult {
    Integer bucketingVersion;

    public BucketingVersionResult(Integer version) {
      bucketingVersion = version;
    }

    public BucketingVersionResult merge(BucketingVersionResult r) throws SemanticException {
      if (bucketingVersion == r.bucketingVersion || r.bucketingVersion == -1) {
        return new BucketingVersionResult(bucketingVersion);
      }
      throw new SemanticException("invalid state; can't set bucketingVersion correctly");
    }
  }

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    pGraphContext = pctx;

    NodeProcessorCtx ctx = new NodeProcessorCtx() {
    };

    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    //    opRules.put(new RuleRegExp("TS", TableScanOperator.getOperatorName() + "%"), new TSRule());
    opRules.put(new RuleRegExp("RS", ReduceSinkOperator.getOperatorName() + "%"), new RSRule());
    opRules.put(new RuleRegExp("FS", FileSinkOperator.getOperatorName() + "%"), new FSRule());

    SemanticDispatcher disp = new DefaultRuleDispatcher(new DeduceBucketingVersionRule(), opRules, ctx);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pGraphContext.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pGraphContext;
  }

  static class DeduceBucketingVersionRule implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
        throws SemanticException {
      Operator o = (Operator) nd;
      BucketingVersionResult res = new BucketingVersionResult(-1);
      try {
        for (Object object : nodeOutputs) {
          BucketingVersionResult r = res;
          res = res.merge(r);
        }
        o.getConf().setBucketingVersion(res.bucketingVersion);
        return res;
      } catch (Exception e) {
        throw new SemanticException("Error while processing: " + o, e);
      }
    }
  }

  static class FSRule implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
        throws SemanticException {
      FileSinkOperator fso = (FileSinkOperator) nd;
      Integer version = fso.getConf().getTableInfo().getBucketingVersion();
      fso.getConf().setBucketingVersion(version);
      return new BucketingVersionResult(version);
    }

  }

  static class TSRule implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
        throws SemanticException {
      return null;
    }

  }

  static class RSRule extends DeduceBucketingVersionRule {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
        throws SemanticException {
      Operator o = (Operator) nd;
      BucketingVersionResult pVersion = (BucketingVersionResult) super.process(nd, stack, procCtx, nodeOutputs);
      if (pVersion.bucketingVersion == -1) {
        // use version 2 if possible
        o.getConf().setBucketingVersion(2);
      }
      return new BucketingVersionResult(-1);
    }

  }

}
