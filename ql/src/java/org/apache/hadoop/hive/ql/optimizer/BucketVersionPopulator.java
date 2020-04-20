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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

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

  Map<Operator<?>, OpGroup> b = new IdentityHashMap<>();

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
    //    SemanticGraphWalker ogw = new PreOrderWalker(disp);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pGraphContext.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pGraphContext;
  }

  class OpGroup {
    Set<Operator<?>> members = Sets.newIdentityHashSet();
    int version = -1;

    public OpGroup() {
      groups.add(this);
    }

    public void add(Operator o) {
      members.add(o);
      b.put(o, this);
    }

    public void setBucketVersion() {
      for (Operator<?> operator : members) {
        operator.getConf().setBucketingVersion(version);
      }
    }

    class OperatorBucketingVersionInfo {

      private Operator<?> op;
      private int bucketingVersion;

      public OperatorBucketingVersionInfo(Operator<?> op, int bucketingVersion) {
        this.op = op;
        this.bucketingVersion = bucketingVersion;
      }

      @Override
      public String toString() {
        return String.format("[op: %s, bucketingVersion=%d]", op, bucketingVersion);
      }
    }

    List<OperatorBucketingVersionInfo> getBucketingVersions() {
      List<OperatorBucketingVersionInfo> ret = new ArrayList<>();
      for (Operator<?> operator : members) {
        if (operator instanceof TableScanOperator) {
          TableScanOperator tso = (TableScanOperator) operator;
          int bucketingVersion = tso.getConf().getTableMetadata().getBucketingVersion();
          ret.add(new OperatorBucketingVersionInfo(operator, bucketingVersion));
        }
        if (operator instanceof FileSinkOperator) {
          FileSinkOperator fso = (FileSinkOperator) operator;
          int bucketingVersion = fso.getConf().getTableInfo().getBucketingVersion();
          ret.add(new OperatorBucketingVersionInfo(operator, bucketingVersion));
        }
      }
      return ret;
    }

    public void analyzeBucketVersion() {
      List<OperatorBucketingVersionInfo> bucketingVersions = getBucketingVersions();
      try {
        for (OperatorBucketingVersionInfo info : bucketingVersions) {
        setVersion(info.bucketingVersion);
      }
      } catch (Exception e) {
        throw new RuntimeException("Error setting bucketingVersion for group: " + bucketingVersions, e);
      }
      if (version == -1) {
        // use version 2 if possible
        version = 2;
      }
    }

    private void setVersion(int newVersion) {
      if (version == newVersion || newVersion == -1) {
        return;
      }
      if (version == -1) {
        version = newVersion;
        return;
      }
      throw new RuntimeException("Unable to set version");
    }

    public void merge(OpGroup opGroup) {
      for (Operator<?> operator : opGroup.members) {
        add(operator);
      }
      opGroup.members.clear();
    }

  }


  class SetPreferredBucketingVersionRule implements SemanticNodeProcessor {


    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
        throws SemanticException {
      Operator o = (Operator) nd;
      OpGroup g;
      if (nodeOutputs.length == 0) {
        g = new OpGroup();
      } else {
        g = (OpGroup) nodeOutputs[0];
      }
      for (int i = 1; i < nodeOutputs.length; i++) {
        g.merge((OpGroup) nodeOutputs[i]);
      }
      g.add(o);
      if (o instanceof ReduceSinkOperator) {
        // start a new group before the reduceSinkOperator
        return new OpGroup();
      } else {
        return g;
      }
    }

    private OpGroup getGroupFor(Operator o) {
      OpGroup g = b.get(o.getParentOperators().get(0));
      for (int i = 1; i < o.getNumParent(); i++) {
        g.merge(b.get(o.getParentOperators().get(i)));
      }
      return g;
    }

  }
}
