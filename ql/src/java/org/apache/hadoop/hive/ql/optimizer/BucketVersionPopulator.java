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
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.curator.shaded.com.google.common.collect.Sets;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BucketVersionPopulator extends Transform {

  protected static final Logger LOG = LoggerFactory.getLogger(BucketVersionPopulator.class);

  @Deprecated

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
      if (bucketingVersion == -1) {
        return new BucketingVersionResult(r.bucketingVersion);
      }
      throw new SemanticException("invalid state; can't set bucketingVersion correctly");
    }

    public BucketingVersionResult merge2(BucketingVersionResult r) {
      if (bucketingVersion == r.bucketingVersion || r.bucketingVersion == -1) {
        return new BucketingVersionResult(bucketingVersion);
      }
      return new BucketingVersionResult(2);
    }
  }

  @Deprecated
  Set<OpGroup> groups = new HashSet<BucketVersionPopulator.OpGroup>();

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    pGraphContext = pctx;
    findOpGroups();
    assignGroupVersions();
    return pctx;
  }

  private void assignGroupVersions() {
    Set<OpGroup> g = groups;
    for (OpGroup opGroup : g) {
      opGroup.analyzeBucketVersion();
      opGroup.setBucketVersion();
    }

  }

  private ParseContext findOpGroups() throws SemanticException {

    NodeProcessorCtx ctx = new NodeProcessorCtx() {
    };

    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();

    SemanticDispatcher disp = new DefaultRuleDispatcher(new SetPreferredBucketingVersionRule(), opRules, ctx);
    SemanticGraphWalker ogw = new PreOrderWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pGraphContext.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pGraphContext;
  }

  static class OpGroup {
    Set<Operator<?>> members = Sets.newIdentityHashSet();
    int version = -1;

    public void add(Operator o) {
      members.add(o);
    }

    public void setBucketVersion() {
      for (Operator<?> operator : members) {
        operator.getConf().setBucketingVersion(version);

      }
    }

    public void analyzeBucketVersion() {
      for (Operator<?> operator : members) {
        if (operator instanceof TableScanOperator) {
          TableScanOperator tso = (TableScanOperator) operator;
          setVersion(tso.getConf().getTableMetadata().getBucketingVersion());
        }
        if (operator instanceof FileSinkOperator) {
          FileSinkOperator fso = (FileSinkOperator) operator;
          setVersion(fso.getConf().getTableInfo().getBucketingVersion());
        }
      }

    }

    private void setVersion(int newVersion) {
      if (version == newVersion) {
        return;
      }
      if (version == -1) {
        version = newVersion;
        return;
      }
      throw new RuntimeException("Unable to set version");
    }

  }


  class SetPreferredBucketingVersionRule implements SemanticNodeProcessor {

    Map<Operator<?>, OpGroup> b = new IdentityHashMap<>();

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
        throws SemanticException {
      Operator o = (Operator) nd;
      OpGroup g;
      if (o.getNumParent() == 0 || o instanceof ReduceSinkOperator) {
        groups.add(g = new OpGroup());
      } else {
        if (o.getNumParent() != 1) {
          throw new RuntimeException("unexpected");
        }
        g = b.get(o.getParentOperators().get(0));
        if (g == null) {
          throw new RuntimeException("no group for parent operator?");
        }
      }
      g.add(o);
      b.put(o, g);
      return null;
    }

  }
}
