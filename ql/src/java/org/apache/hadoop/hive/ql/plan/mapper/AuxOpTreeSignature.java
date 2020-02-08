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

package org.apache.hadoop.hive.ql.plan.mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.signature.OpTreeSignature;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * This signature is used to establish related parts to connect pre/post PPD optimizations.
 */
public final class AuxOpTreeSignature {
  private OpTreeSignature sig;

  public AuxOpTreeSignature(OpTreeSignature sig) {
    this.sig = sig;
  }

  @Override
  public int hashCode() {
    return sig.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return (obj.getClass() == AuxOpTreeSignature.class) && sig.equals(((AuxOpTreeSignature) obj).sig);
  }

  @Override
  public String toString() {
    return sig.toString();
  }

  static class AuxSignatureLinker implements SemanticNodeProcessor {

    private PlanMapper pm;

    public AuxSignatureLinker(PlanMapper pm) {
      this.pm = pm;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
        throws SemanticException {
      Operator<?> op = (Operator<?>) nd;
      AuxOpTreeSignature treeSig = pm.getAuxSignatureOf(op);
      pm.merge(op, treeSig);
      return nd;
    }

  }

  /**
   * Links a full tree
   */
  private static void linkAuxSignatures(ParseContext pctx, ArrayList<Node> topNodes) throws SemanticException {

    PlanMapper pm = pctx.getContext().getPlanMapper();
    pm.clearSignatureCache();
    SemanticDispatcher disp = new DefaultRuleDispatcher(new AuxSignatureLinker(pm), new HashMap(), null);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

    ogw.startWalking(topNodes, null);

  }

  public static void linkAuxSignatures(ParseContext parseContext) throws SemanticException {
    if (parseContext.getConf().getBoolVar(ConfVars.HIVE_QUERY_PLANMAPPER_LINK_RELNODES)) {
      linkAuxSignatures(parseContext, new ArrayList(parseContext.getTopOps().values()));
    }
  }
}