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

package org.apache.hadoop.hive.accumulo.predicate;

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.serde.AccumuloIndexParameters;
import org.apache.hadoop.hive.accumulo.AccumuloIndexScanner;
import org.apache.hadoop.hive.accumulo.AccumuloIndexScannerException;
import org.apache.hadoop.hive.accumulo.AccumuloIndexLexicoder;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloRowIdColumnMapping;
import org.apache.hadoop.hive.accumulo.predicate.compare.CompareOp;
import org.apache.hadoop.hive.accumulo.predicate.compare.Equal;
import org.apache.hadoop.hive.accumulo.predicate.compare.GreaterThan;
import org.apache.hadoop.hive.accumulo.predicate.compare.GreaterThanOrEqual;
import org.apache.hadoop.hive.accumulo.predicate.compare.LessThan;
import org.apache.hadoop.hive.accumulo.predicate.compare.LessThanOrEqual;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 *
 */
public class AccumuloRangeGenerator implements NodeProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(AccumuloRangeGenerator.class);

  private final AccumuloPredicateHandler predicateHandler;
  private final HiveAccumuloRowIdColumnMapping rowIdMapping;
  private final String hiveRowIdColumnName;
  private AccumuloIndexScanner indexScanner;

  public AccumuloRangeGenerator(Configuration conf, AccumuloPredicateHandler predicateHandler,
      HiveAccumuloRowIdColumnMapping rowIdMapping, String hiveRowIdColumnName) {
    this.predicateHandler = predicateHandler;
    this.rowIdMapping = rowIdMapping;
    this.hiveRowIdColumnName = hiveRowIdColumnName;
    try {
      this.indexScanner = new AccumuloIndexParameters(conf).createScanner();
    } catch (AccumuloIndexScannerException e) {
      LOG.error(e.getLocalizedMessage(), e);
      this.indexScanner = null;
    }
  }

  public AccumuloIndexScanner getIndexScanner() {
    return indexScanner;
  }

  public void setIndexScanner(AccumuloIndexScanner indexScanner) {
    this.indexScanner = indexScanner;
  }

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
      throws SemanticException {
    // If it's not some operator, pass it back
    if (!(nd instanceof ExprNodeGenericFuncDesc)) {
      return nd;
    }

    ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) nd;

    // 'and' nodes need to be intersected
    if (FunctionRegistry.isOpAnd(func)) {
      return processAndOpNode(nd, nodeOutputs);
      // 'or' nodes need to be merged
    } else if (FunctionRegistry.isOpOr(func)) {
      return processOrOpNode(nd, nodeOutputs);
    } else if (FunctionRegistry.isOpNot(func)) {
      // TODO handle negations
      throw new IllegalArgumentException("Negations not yet implemented");
    } else {
      return processExpression(func, nodeOutputs);
    }
  }

  protected Object processAndOpNode(Node nd, Object[] nodeOutputs) {
    // We might have multiple ranges coming from children
    List<Range> andRanges = null;

    for (Object nodeOutput : nodeOutputs) {
      // null signifies nodes that are irrelevant to the generation
      // of Accumulo Ranges
      if (null == nodeOutput) {
        continue;
      }

      // When an AND has no children (some conjunction over a field that isn't the column
      // mapped to the Accumulo rowid) and when a conjunction generates Ranges which are empty
      // (the children of the conjunction are disjoint), these two cases need to be kept separate.
      //
      // A null `andRanges` implies that ranges couldn't be computed, while an empty List
      // of Ranges implies that there are no possible Ranges to lookup.
      if (null == andRanges) {
        andRanges = new ArrayList<Range>();
      }

      // The child is a single Range
      if (nodeOutput instanceof Range) {
        Range childRange = (Range) nodeOutput;

        // No existing ranges, just accept the current
        if (andRanges.isEmpty()) {
          andRanges.add(childRange);
        } else {
          // For each range we have, intersect them. If they don't overlap
          // the range can be discarded
          List<Range> newRanges = new ArrayList<Range>();
          for (Range andRange : andRanges) {
            Range intersectedRange = andRange.clip(childRange, true);
            if (null != intersectedRange) {
              newRanges.add(intersectedRange);
            }
          }

          // Set the newly-constructed ranges as the current state
          andRanges = newRanges;
        }
      } else if (nodeOutput instanceof List) {
        @SuppressWarnings("unchecked")
        List<Range> childRanges = (List<Range>) nodeOutput;

        // No ranges, use the ranges from the child
        if (andRanges.isEmpty()) {
          andRanges.addAll(childRanges);
        } else {
          List<Range> newRanges = new ArrayList<Range>();

          // Cartesian product of our ranges, to the child ranges
          for (Range andRange : andRanges) {
            for (Range childRange : childRanges) {
              Range intersectedRange = andRange.clip(childRange, true);

              // Retain only valid intersections (discard disjoint ranges)
              if (null != intersectedRange) {
                newRanges.add(intersectedRange);
              }
            }
          }

          // Set the newly-constructed ranges as the current state
          andRanges = newRanges;
        }
      } else {
        LOG.error("Expected Range from {} but got {}", nd, nodeOutput);
        throw new IllegalArgumentException("Expected Range but got "
            + nodeOutput.getClass().getName());
      }
    }

    return andRanges;
  }

  protected Object processOrOpNode(Node nd, Object[] nodeOutputs) {
    List<Range> orRanges = new ArrayList<Range>(nodeOutputs.length);
    for (Object nodeOutput : nodeOutputs) {
      if (nodeOutput instanceof Range) {
        orRanges.add((Range) nodeOutput);
      } else if (nodeOutput instanceof List) {
        @SuppressWarnings("unchecked")
        List<Range> childRanges = (List<Range>) nodeOutput;
        orRanges.addAll(childRanges);
      } else {
        LOG.error("Expected Range from {} but got {}", nd, nodeOutput);
        throw new IllegalArgumentException("Expected Range but got "
            + nodeOutput.getClass().getName());
      }
    }

    // Try to merge multiple ranges together
    if (orRanges.size() > 1) {
      return Range.mergeOverlapping(orRanges);
    } else if (1 == orRanges.size()) {
      // Return just the single Range
      return orRanges.get(0);
    } else {
      // No ranges, just return the empty list
      return orRanges;
    }
  }

  protected Object processExpression(ExprNodeGenericFuncDesc func, Object[] nodeOutputs)
      throws SemanticException {
    // a binary operator (gt, lt, ge, le, eq, ne)
    GenericUDF genericUdf = func.getGenericUDF();

    // Find the argument to the operator which is a constant
    ExprNodeConstantDesc constantDesc = null;
    ExprNodeColumnDesc columnDesc = null;
    ExprNodeDesc leftHandNode = null;
    for (Object nodeOutput : nodeOutputs) {
      if (nodeOutput instanceof ExprNodeConstantDesc) {
        // Ordering of constant and column in expression is important in correct range generation
        if (null == leftHandNode) {
          leftHandNode = (ExprNodeDesc) nodeOutput;
        }

        constantDesc = (ExprNodeConstantDesc) nodeOutput;
      } else if (nodeOutput instanceof ExprNodeColumnDesc) {
        // Ordering of constant and column in expression is important in correct range generation
        if (null == leftHandNode) {
          leftHandNode = (ExprNodeDesc) nodeOutput;
        }

        columnDesc = (ExprNodeColumnDesc) nodeOutput;
      }
    }

    // If it's constant = constant or column = column, we can't fetch any ranges
    // TODO We can try to be smarter and push up the value to some node which
    // we can generate ranges from e.g. rowid > (4 + 5)
    if (null == constantDesc || null == columnDesc) {
      return null;
    }

    ConstantObjectInspector objInspector = constantDesc.getWritableObjectInspector();

    // Reject any clauses that are against a column that isn't the rowId mapping or indexed
    if (!this.hiveRowIdColumnName.equals(columnDesc.getColumn())) {
      if (this.indexScanner != null && this.indexScanner.isIndexed(columnDesc.getColumn())) {
        return getIndexedRowIds(genericUdf, leftHandNode, columnDesc.getColumn(), objInspector);
      }
      return null;
    }

    Text constText = getConstantText(objInspector);

    return getRange(genericUdf, leftHandNode, constText);
  }

  private Range getRange(GenericUDF genericUdf, ExprNodeDesc leftHandNode, Text constText) {
    Class<? extends CompareOp> opClz;
    try {
      opClz = predicateHandler.getCompareOpClass(genericUdf.getUdfName());
    } catch (NoSuchCompareOpException e) {
      throw new IllegalArgumentException("Unhandled UDF class: " + genericUdf.getUdfName());
    }

    if (leftHandNode instanceof ExprNodeConstantDesc) {
      return getConstantOpColumnRange(opClz, constText);
    } else if (leftHandNode instanceof ExprNodeColumnDesc) {
      return getColumnOpConstantRange(opClz, constText);
    } else {
      throw new IllegalStateException("Expected column or constant on LHS of expression");
    }
  }

  private Text getConstantText(ConstantObjectInspector objInspector) throws SemanticException {
    Text constText;
    switch (rowIdMapping.getEncoding()) {
      case STRING:
        constText = getUtf8Value(objInspector);
        break;
      case BINARY:
        try {
          constText = getBinaryValue(objInspector);
        } catch (IOException e) {
          throw new SemanticException(e);
        }
        break;
      default:
        throw new SemanticException("Unable to parse unknown encoding: "
            + rowIdMapping.getEncoding());
    }
    return constText;
  }

  protected Range getConstantOpColumnRange(Class<? extends CompareOp> opClz, Text constText) {
    if (opClz.equals(Equal.class)) {
      // 100 == x
      return new Range(constText); // single row
    } else if (opClz.equals(GreaterThanOrEqual.class)) {
      // 100 >= x
      return new Range(null, constText); // neg-infinity to end inclusive
    } else if (opClz.equals(GreaterThan.class)) {
      // 100 > x
      return new Range(null, false, constText, false); // neg-infinity to end exclusive
    } else if (opClz.equals(LessThanOrEqual.class)) {
      // 100 <= x
      return new Range(constText, true, null, false); // start inclusive to infinity
    } else if (opClz.equals(LessThan.class)) {
      // 100 < x
      return new Range(constText, false, null, false); // start exclusive to infinity
    } else {
      throw new IllegalArgumentException("Could not process " + opClz);
    }
  }

  protected Range getColumnOpConstantRange(Class<? extends CompareOp> opClz, Text constText) {
    if (opClz.equals(Equal.class)) {
      return new Range(constText); // start inclusive to end inclusive
    } else if (opClz.equals(GreaterThanOrEqual.class)) {
      return new Range(constText, null); // start inclusive to infinity inclusive
    } else if (opClz.equals(GreaterThan.class)) {
      return new Range(constText, false, null, false); // start exclusive to infinity inclusive
    } else if (opClz.equals(LessThanOrEqual.class)) {
      return new Range(null, false, constText, true); // neg-infinity to start inclusive
    } else if (opClz.equals(LessThan.class)) {
      return new Range(null, false, constText, false); // neg-infinity to start exclusive
    } else {
      throw new IllegalArgumentException("Could not process " + opClz);
    }
  }


  protected Object getIndexedRowIds(GenericUDF genericUdf, ExprNodeDesc leftHandNode,
                                    String columnName, ConstantObjectInspector objInspector)
      throws SemanticException {
    Text constText = getConstantText(objInspector);
    byte[] value = constText.toString().getBytes(UTF_8);
    byte[] encoded = AccumuloIndexLexicoder.encodeValue(value, leftHandNode.getTypeString(), true);
    Range range = getRange(genericUdf, leftHandNode, new Text(encoded));
    if (indexScanner != null) {
      return indexScanner.getIndexRowRanges(columnName, range);
    }
    return null;
  }


  protected Text getUtf8Value(ConstantObjectInspector objInspector) {
    // TODO is there a more correct way to get the literal value for the Object?
    return new Text(objInspector.getWritableConstantValue().toString());
  }

  /**
   * Attempts to construct the binary value from the given inspector. Falls back to UTF8 encoding
   * when the value cannot be coerced into binary.
   *
   * @return Binary value when possible, utf8 otherwise
   * @throws IOException
   */
  protected Text getBinaryValue(ConstantObjectInspector objInspector) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    if (objInspector instanceof PrimitiveObjectInspector) {
      LazyUtils.writePrimitive(out, objInspector.getWritableConstantValue(),
          (PrimitiveObjectInspector) objInspector);
    } else {
      return getUtf8Value(objInspector);
    }

    out.close();
    return new Text(out.toByteArray());
  }
}
