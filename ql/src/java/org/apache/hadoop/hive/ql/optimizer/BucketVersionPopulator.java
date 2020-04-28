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

/**
 * This class analyzes and sets the bucketing versions.
 *
 * A set of data values can be distributed into N buckets differently depending on the used hashing algorithm.
 * Hive right now supports multiple hashing algorithms - the actual algo is identified by "bucketingVersion".
 *
 * Bucketing version can be re-select after every Reduce Sink; because a full shuffle can re-distribute the data according to a new hash algo as well.
 *
 * Depending on the table Hive might need to write it's data in some specific bucketing version.
 *
 * In case a bucketed table is read from the table location; the data should be threated as described by the table's bucketing_version property.
 *
 */
public class BucketVersionPopulator extends Transform {

  protected static final Logger LOG = LoggerFactory.getLogger(BucketVersionPopulator.class);


  @Deprecated
  Set<OpGroup> groups = new HashSet<BucketVersionPopulator.OpGroup>();


  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    findOpGroups(pctx);
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

  private void findOpGroups(ParseContext pctx) throws SemanticException {

    NodeProcessorCtx ctx = new NodeProcessorCtx() {
    };

    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();

    SemanticDispatcher disp = new DefaultRuleDispatcher(new SetPreferredBucketingVersionRule(), opRules, ctx);
    //    SemanticGraphWalker ogw = new PreOrderWalker(disp);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
  }

  class OpGroup {
    Set<Operator<?>> members = Sets.newIdentityHashSet();
    int version = -1;

    public OpGroup() {
      groups.add(this);
    }

    public void add(Operator o) {
      members.add(o);
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
          int numBuckets = tso.getConf().getNumBuckets();
          if (numBuckets > 1) {
            ret.add(new OperatorBucketingVersionInfo(operator, bucketingVersion));
          } else {
            LOG.info("not considering bucketingVersion for: %s because it has %d<2 buckets ", tso, numBuckets);
          }
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

  }
}
