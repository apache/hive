/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

/**
 *this transformation does bucket map join optimization.
 */
public class BucketMapJoinOptimizer implements Transform {
  
  private static final Log LOG = LogFactory.getLog(GroupByOptimizer.class
      .getName());

  public BucketMapJoinOptimizer() {
  }

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    BucketMapjoinOptProcCtx bucketMapJoinOptimizeCtx = new BucketMapjoinOptProcCtx();

    // process map joins with no reducers pattern
    opRules.put(new RuleRegExp("R1", "MAPJOIN%"), getBucketMapjoinProc(pctx));
    opRules.put(new RuleRegExp("R2", "RS%.*MAPJOIN"), getBucketMapjoinRejectProc(pctx));
    opRules.put(new RuleRegExp(new String("R3"), "UNION%.*MAPJOIN%"),
        getBucketMapjoinRejectProc(pctx));
    opRules.put(new RuleRegExp(new String("R4"), "MAPJOIN%.*MAPJOIN%"),
        getBucketMapjoinRejectProc(pctx));

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules,
        bucketMapJoinOptimizeCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pctx;
  }

  private NodeProcessor getBucketMapjoinRejectProc(ParseContext pctx) {
    return new NodeProcessor () {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx procCtx, Object... nodeOutputs)
          throws SemanticException {
        MapJoinOperator mapJoinOp = (MapJoinOperator) nd;
        BucketMapjoinOptProcCtx context = (BucketMapjoinOptProcCtx) procCtx;
        context.listOfRejectedMapjoins.add(mapJoinOp);
        return null;
      }
    };
  }

  private NodeProcessor getBucketMapjoinProc(ParseContext pctx) {
    return new BucketMapjoinOptProc(pctx);
  }

  private NodeProcessor getDefaultProc() {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx procCtx, Object... nodeOutputs)
          throws SemanticException {
        return null;
      }
    };
  }
  
  class BucketMapjoinOptProc implements NodeProcessor {
    
    protected ParseContext pGraphContext;
    
    public BucketMapjoinOptProc(ParseContext pGraphContext) {
      super();
      this.pGraphContext = pGraphContext;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      MapJoinOperator mapJoinOp = (MapJoinOperator) nd;
      BucketMapjoinOptProcCtx context = (BucketMapjoinOptProcCtx) procCtx;

      if(context.getListOfRejectedMapjoins().contains(mapJoinOp))
        return null;
      
      QBJoinTree joinCxt = this.pGraphContext.getMapJoinContext().get(mapJoinOp);
      if(joinCxt == null)
        return null;
      
      List<String> joinAliases = new ArrayList<String>();
      String[] srcs = joinCxt.getBaseSrc();
      String[] left = joinCxt.getLeftAliases();
      List<String> mapAlias = joinCxt.getMapAliases();
      String baseBigAlias = null;
      for(String s : left) {
        if(s != null && !joinAliases.contains(s)) {
          joinAliases.add(s);
          if(!mapAlias.contains(s)) {
            baseBigAlias = s;
          }
        }
      }
      for(String s : srcs) {
        if(s != null && !joinAliases.contains(s)) {
          joinAliases.add(s);
          if(!mapAlias.contains(s)) {
            baseBigAlias = s;
          }
        }
      }
      
      MapJoinDesc mjDecs = mapJoinOp.getConf();
      LinkedHashMap<String, Integer> aliasToBucketNumber = new LinkedHashMap<String, Integer>();
      LinkedHashMap<String, List<String>> aliasToBucketFileNames = new LinkedHashMap<String, List<String>>();
      // right now this code does not work with "a join b on a.key = b.key and
      // a.ds = b.ds", where ds is a partition column. It only works with joins
      // with only one partition presents in each join source tables.
      Map<String, Operator<? extends Serializable>> topOps = this.pGraphContext.getTopOps();
      Map<TableScanOperator, Table> topToTable = this.pGraphContext.getTopToTable();
      
      List<Integer> bucketNumbers = new ArrayList<Integer>();
      for (int index = 0; index < joinAliases.size(); index++) {
        String alias = joinAliases.get(index);
        TableScanOperator tso = (TableScanOperator) topOps.get(alias);
        Table tbl = topToTable.get(tso);
        if(tbl.isPartitioned()) {
          PrunedPartitionList prunedParts = null;
          try {
            prunedParts = PartitionPruner.prune(tbl, pGraphContext.getOpToPartPruner().get(tso), pGraphContext.getConf(), alias,
                pGraphContext.getPrunedPartitions());
          } catch (HiveException e) {
            // Has to use full name to make sure it does not conflict with
            // org.apache.commons.lang.StringUtils
            LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
            throw new SemanticException(e.getMessage(), e);
          }
          int partNumber = prunedParts.getConfirmedPartns().size()
              + prunedParts.getUnknownPartns().size();
          if(partNumber > 1)
            return null;

          Partition part = null;
          Iterator<Partition> iter = prunedParts.getConfirmedPartns()
              .iterator();
          while (iter.hasNext())
            part = iter.next();
          if (part == null) {
            iter = prunedParts.getUnknownPartns().iterator();
            while (iter.hasNext())
              part = iter.next();
          }

          assert part != null;

          if (!checkBucketColumns(part.getBucketCols(), mjDecs, index))
            return null;

          Integer num = new Integer(part.getBucketCount());
          aliasToBucketNumber.put(alias, num);
          List<String> fileNames = new ArrayList<String>();
          try {
            FileSystem fs = FileSystem.get(this.pGraphContext.getConf());
            FileStatus[] files = fs.listStatus(new Path(part.getDataLocation()
                .toString()));
            if (files != null) {
              for (FileStatus file : files) {
                fileNames.add(file.getPath().toString());
              }
            }
          } catch (IOException e) {
            throw new SemanticException(e);
          }
          aliasToBucketFileNames.put(alias, fileNames);
        } else {
          if (!checkBucketColumns(tbl.getBucketCols(), mjDecs, index))
            return null;
          Integer num = new Integer(tbl.getNumBuckets());
          aliasToBucketNumber.put(alias, num);
          List<String> fileNames = new ArrayList<String>();
          try {
            FileSystem fs = FileSystem.get(this.pGraphContext.getConf());
            FileStatus[] files = fs.listStatus(new Path(tbl.getDataLocation().toString()));
            if(files != null) {
              for(FileStatus file : files) {
                fileNames.add(file.getPath().toString());
              }
            }
          } catch (IOException e) {
            throw new SemanticException(e);
          }
          aliasToBucketFileNames.put(alias, fileNames);
        }
      }
      
      // All tables or partitions are bucketed, and their bucket number is
      // stored in 'bucketNumbers', we need to check if the number of buckets in
      // the big table can be divided by no of buckets in small tables.
      int bucketNoInBigTbl = aliasToBucketNumber.get(baseBigAlias).intValue();
      Iterator<Integer> iter = aliasToBucketNumber.values().iterator();
      while(iter.hasNext()) {
        int nxt = iter.next().intValue();
        boolean ok = (nxt >= bucketNoInBigTbl) ? nxt % bucketNoInBigTbl == 0
            : bucketNoInBigTbl % nxt == 0;
        if(!ok)
          return null;
      }
      MapJoinDesc desc = mapJoinOp.getConf();
      
      LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>> aliasBucketFileNameMapping = 
        new LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>>();
      
      int bigTblBucketNum =  aliasToBucketNumber.get(baseBigAlias);
      Collections.sort(aliasToBucketFileNames.get(baseBigAlias));
      for (int j = 0; j < joinAliases.size(); j++) {
        String alias = joinAliases.get(j);
        if(alias.equals(baseBigAlias))
          continue;
        Collections.sort(aliasToBucketFileNames.get(alias));
        LinkedHashMap<String, ArrayList<String>> mapping = new LinkedHashMap<String, ArrayList<String>>();
        aliasBucketFileNameMapping.put(alias, mapping);
        for(String inputBigTBLBucket : aliasToBucketFileNames.get(baseBigAlias)) {
          int smallTblBucketNum = aliasToBucketNumber.get(alias);
          ArrayList<String> resultFileNames = new ArrayList<String>();
          int index = aliasToBucketFileNames.get(baseBigAlias).indexOf(inputBigTBLBucket);
          if (bigTblBucketNum >= smallTblBucketNum) {
            int temp = bigTblBucketNum / smallTblBucketNum;
            int toAddSmallIndex = index/temp;
            if(toAddSmallIndex < aliasToBucketFileNames.get(alias).size()) {
              resultFileNames.add(aliasToBucketFileNames.get(alias).get(toAddSmallIndex));
            }
          } else {
            int jump = smallTblBucketNum / bigTblBucketNum;
            for (int i = index; i < aliasToBucketFileNames.get(alias).size(); i = i + jump) {
              if(i <= aliasToBucketFileNames.get(alias).size()) {
                resultFileNames.add(aliasToBucketFileNames.get(alias).get(i));
              }
            }
          }
          mapping.put(inputBigTBLBucket, resultFileNames);
        }
      }
      desc.setAliasBucketFileNameMapping(aliasBucketFileNameMapping);
      desc.setBigTableAlias(baseBigAlias);
      return null;
    }
    
    private boolean checkBucketColumns(List<String> bucketColumns, MapJoinDesc mjDesc, int index) {
      List<ExprNodeDesc> keys = mjDesc.getKeys().get((byte)index);
      if (keys == null || bucketColumns == null || bucketColumns.size() == 0)
        return false;
      
      //get all join columns from join keys stored in MapJoinDesc
      List<String> joinCols = new ArrayList<String>();
      List<ExprNodeDesc> joinKeys = new ArrayList<ExprNodeDesc>();
      joinKeys.addAll(keys);
      while (joinKeys.size() > 0) {
        ExprNodeDesc node = joinKeys.remove(0);
        if (node instanceof ExprNodeColumnDesc) {
          joinCols.addAll(node.getCols());
        } else if (node instanceof ExprNodeGenericFuncDesc) {
          ExprNodeGenericFuncDesc udfNode = ((ExprNodeGenericFuncDesc) node);
          GenericUDF udf = udfNode.getGenericUDF();
          if (!FunctionRegistry.isDeterministic(udf)) {
            return false;
          }
          joinKeys.addAll(0, udfNode.getChildExprs());
        } else {
          return false;
        }
      }

      // to see if the join columns from a table is exactly this same as its
      // bucket columns 
      if (joinCols.size() == 0 || joinCols.size() != bucketColumns.size()) {
        return false;
      }
      
      for (String col : joinCols) {
        if (!bucketColumns.contains(col))
          return false;
      }
      
      return true;
    }
    
  }
  
  class BucketMapjoinOptProcCtx implements NodeProcessorCtx {
    // we only convert map joins that follows a root table scan in the same
    // mapper. That means there is no reducer between the root table scan and
    // mapjoin.
    Set<MapJoinOperator> listOfRejectedMapjoins = new HashSet<MapJoinOperator>();
    
    public Set<MapJoinOperator> getListOfRejectedMapjoins() {
      return listOfRejectedMapjoins;
    }
    
  }
}
