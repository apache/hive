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
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree.Operator;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.PrunerOperatorFactory.FilterPruner;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExcept;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.google.common.base.Preconditions;

/**
 * Fixed bucket pruning optimizer goes through all the table scans and annotates them
 * with a bucketing inclusion bit-set.
 */
public class FixedBucketPruningOptimizer extends Transform {

  private static final Log LOG = LogFactory
      .getLog(FixedBucketPruningOptimizer.class.getName());

  private final boolean compat;

  public FixedBucketPruningOptimizer(boolean compat) {
    this.compat = compat;
  }

  public class NoopWalker implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      // do nothing
      return null;
    }
  }

  public static class BucketBitsetGenerator extends FilterPruner {

    @Override
    protected void generatePredicate(NodeProcessorCtx procCtx,
        FilterOperator fop, TableScanOperator top) throws SemanticException{
      FixedBucketPruningOptimizerCtxt ctxt = ((FixedBucketPruningOptimizerCtxt) procCtx);
      Table tbl = top.getConf().getTableMetadata();
      int numBuckets = tbl.getNumBuckets();
      if (numBuckets <= 0 || tbl.getBucketCols().size() != 1) {
        // bucketing isn't consistent or there are >1 bucket columns
        // optimizer does not extract multiple column predicates for this
        return;
      }

      if (tbl.isPartitioned()) {
        // Make sure all the partitions have same bucket count.
        PrunedPartitionList prunedPartList =
            PartitionPruner.prune(top, ctxt.pctx, top.getConf().getAlias());
        if (prunedPartList != null) {
          for (Partition p : prunedPartList.getPartitions()) {
            if (numBuckets != p.getBucketCount()) {
              // disable feature
              return;
            }
          }
        }
      }
      
      ExprNodeGenericFuncDesc filter = top.getConf().getFilterExpr();
      if (filter == null) {
        return;
      }
      // the sargs are closely tied to hive.optimize.index.filter
      SearchArgument sarg = ConvertAstToSearchArg.create(ctxt.pctx.getConf(), filter);
      if (sarg == null) {
        return;
      }
      final String bucketCol = tbl.getBucketCols().get(0);
      StructField bucketField = null;
      for (StructField fs : tbl.getFields()) {
        if(fs.getFieldName().equals(bucketCol)) {
          bucketField = fs;
        }
      }
      Preconditions.checkArgument(bucketField != null);
      List<Object> literals = new ArrayList<Object>();
      List<PredicateLeaf> leaves = sarg.getLeaves();
      Set<PredicateLeaf> bucketLeaves = new HashSet<PredicateLeaf>();
      for (PredicateLeaf l : leaves) {
        if (bucketCol.equals(l.getColumnName())) {
          switch (l.getOperator()) {
          case EQUALS:
          case IN:
            // supported
            break;
          case IS_NULL:
            // TODO: (a = 1) and NOT (a is NULL) can be potentially folded earlier into a NO-OP
            // fall through
          case BETWEEN:
            // TODO: for ordinal types you can produce a range (BETWEEN 1444442100 1444442107)
            // fall through
          default:
            // cannot optimize any others
            return;
          }
          bucketLeaves.add(l);
        }
      }
      if (bucketLeaves.size() == 0) {
        return;
      }
      // TODO: Add support for AND clauses under OR clauses
      // first-cut takes a known minimal tree and no others.
      // $expr = (a=1)
      //         (a=1 or a=2)
      //         (a in (1,2))
      //         ($expr and *)
      //         (* and $expr)
      ExpressionTree expr = sarg.getExpression();
      if (expr.getOperator() == Operator.LEAF) {
        PredicateLeaf l = leaves.get(expr.getLeaf());
        if (!addLiteral(literals, l)) {
          return;
        }
      } else if (expr.getOperator() == Operator.AND) {
        boolean found = false;
        for (ExpressionTree subExpr : expr.getChildren()) {
          if (subExpr.getOperator() != Operator.LEAF) {
            return;
          }
          // one of the branches is definitely a bucket-leaf
          PredicateLeaf l = leaves.get(subExpr.getLeaf());
          if (bucketLeaves.contains(l)) {
            if (!addLiteral(literals, l)) {
              return;
            }
            found = true;
          }
        }
        if (!found) {
          return;
        }
      } else if (expr.getOperator() == Operator.OR) {
        for (ExpressionTree subExpr : expr.getChildren()) {
          if (subExpr.getOperator() != Operator.LEAF) {
            return;
          }
          PredicateLeaf l = leaves.get(subExpr.getLeaf());
          if (bucketLeaves.contains(l)) {
            if (!addLiteral(literals, l)) {
              return;
            }
          } else {
            // all of the OR branches need to be bucket-leaves
            return;
          }
        }
      }
      // invariant: bucket-col IN literals of type bucketField
      BitSet bs = new BitSet(numBuckets);
      bs.clear();
      PrimitiveObjectInspector bucketOI = (PrimitiveObjectInspector)bucketField.getFieldObjectInspector();
      PrimitiveObjectInspector constOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(bucketOI.getPrimitiveCategory());
      // Fetch the bucketing version from table scan operator
      int bucketingVersion = top.getConf().getTableMetadata().getBucketingVersion();

      for (Object literal: literals) {
        PrimitiveObjectInspector origOI = PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(literal.getClass());
        Converter conv = ObjectInspectorConverters.getConverter(origOI, constOI);
        // exact type conversion or get out
        if (conv == null) {
          return;
        }
        Object convCols[] = new Object[] {conv.convert(literal)};
        int n = bucketingVersion == 2 ?
            ObjectInspectorUtils.getBucketNumber(convCols, new ObjectInspector[]{constOI}, numBuckets) :
            ObjectInspectorUtils.getBucketNumberOld(convCols, new ObjectInspector[]{constOI}, numBuckets);
        bs.set(n);
        if (bucketingVersion == 1 && ctxt.isCompat()) {
          int h = ObjectInspectorUtils.getBucketHashCodeOld(convCols, new ObjectInspector[]{constOI});
          // -ve hashcodes had conversion to positive done in different ways in the past
          // abs() is now obsolete and all inserts now use & Integer.MAX_VALUE 
          // the compat mode assumes that old data could've been loaded using the other conversion
          n = ObjectInspectorUtils.getBucketNumber(Math.abs(h), numBuckets);
          bs.set(n);
        }
      }
      if (bs.cardinality() < numBuckets) {
        // there is a valid bucket pruning filter
        top.getConf().setIncludedBuckets(bs);
        top.getConf().setNumBuckets(numBuckets);
      }
    }

    private boolean addLiteral(List<Object> literals, PredicateLeaf leaf) {
      switch (leaf.getOperator()) {
      case EQUALS:
        return literals.add(
            convertLiteral(leaf.getLiteral()));
      case IN:
        return literals.addAll(
            leaf.getLiteralList().stream().map(l -> convertLiteral(l)).collect(Collectors.toList()));
      default:
        return false;
      }
    }

    private Object convertLiteral(Object o) {
      // This is a bit hackish to fix mismatch between SARG and Hive types
      // for Timestamp and Date. TODO: Move those types to storage-api.
      if (o instanceof java.sql.Date) {
        return Date.valueOf(o.toString());
      } else if (o instanceof java.sql.Timestamp) {
        return Timestamp.valueOf(o.toString());
      }
      return o;
    }
  }

  public final class FixedBucketPruningOptimizerCtxt implements
      NodeProcessorCtx {
    public final ParseContext pctx;
    private final boolean compat;
    private int numBuckets;
    private PrunedPartitionList partitions;
    private List<String> bucketCols;
    private List<StructField> schema;

    public FixedBucketPruningOptimizerCtxt(boolean compat, ParseContext pctx) {
      this.compat = compat;
      this.pctx = pctx;
    }

    public void setSchema(ArrayList<StructField> fields) {
      this.schema = fields;
    }

    public List<StructField> getSchema() {
      return this.schema;
    }

    public void setBucketCols(List<String> bucketCols) {
      this.bucketCols = bucketCols;
    }

    public List<String> getBucketCols() {
      return this.bucketCols;
    }

    public void setPartitions(PrunedPartitionList partitions) {
      this.partitions = partitions;
    }

    public PrunedPartitionList getPartitions() {
      return this.partitions;
    }

    public int getNumBuckets() {
      return numBuckets;
    }

    public void setNumBuckets(int numBuckets) {
      this.numBuckets = numBuckets;
    }

    // compatibility mode enabled
    public boolean isCompat() {
      return this.compat;
    }
  }

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    // create a the context for walking operators
    FixedBucketPruningOptimizerCtxt opPartWalkerCtx = new FixedBucketPruningOptimizerCtxt(compat,
        pctx);

    // walk operator tree to create expression tree for filter buckets
    PrunerUtils.walkOperatorTree(pctx, opPartWalkerCtx,
        new BucketBitsetGenerator(), new NoopWalker());

    return pctx;
  }
}
