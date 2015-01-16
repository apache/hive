/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hive.ql.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import com.google.common.collect.ImmutableSet;

/**
 * Factory for generating the different node processors used by ConstantPropagate.
 */
public final class ConstantPropagateProcFactory {
  protected static final Log LOG = LogFactory.getLog(ConstantPropagateProcFactory.class.getName());
  protected static Set<Class<?>> propagatableUdfs = new HashSet<Class<?>>();

  static {
    propagatableUdfs.add(GenericUDFOPAnd.class);
  };

  private ConstantPropagateProcFactory() {
    // prevent instantiation
  }

  /**
   * Get ColumnInfo from column expression.
   *
   * @param rr
   * @param desc
   * @return
   */
  public static ColumnInfo resolveColumn(RowResolver rr,
      ExprNodeColumnDesc desc) {
    try {
      ColumnInfo ci = rr.get(desc.getTabAlias(), desc.getColumn());
      if (ci == null) {
        String[] tmp = rr.reverseLookup(desc.getColumn());
        if (tmp == null) {
          return null;
        }
        ci = rr.get(tmp[0], tmp[1]);
        ci.setTabAlias(tmp[0]);
        ci.setAlias(tmp[1]);
      } else {
        String[] tmp = rr.reverseLookup(ci.getInternalName());
        if (tmp == null) {
          return null;
        }
        ci.setTabAlias(tmp[0]);
        ci.setAlias(tmp[1]);
      }
      return ci;
    } catch (SemanticException e) {
      throw new RuntimeException(e);
    }
  }

  private static final Set<PrimitiveCategory> unSupportedTypes = ImmutableSet
      .<PrimitiveCategory>builder()
      .add(PrimitiveCategory.DECIMAL)
      .add(PrimitiveCategory.VARCHAR)
      .add(PrimitiveCategory.CHAR).build();

  /**
   * Cast type from expression type to expected type ti.
   *
   * @param desc constant expression
   * @param ti expected type info
   * @return cast constant, or null if the type cast failed.
   */
  private static ExprNodeConstantDesc typeCast(ExprNodeDesc desc, TypeInfo ti) {
    if (desc instanceof ExprNodeNullDesc) {
      return null;
    }
    if (!(ti instanceof PrimitiveTypeInfo) || !(desc.getTypeInfo() instanceof PrimitiveTypeInfo)) {
      return null;
    }

    PrimitiveTypeInfo priti = (PrimitiveTypeInfo) ti;
    PrimitiveTypeInfo descti = (PrimitiveTypeInfo) desc.getTypeInfo();

    if (unSupportedTypes.contains(priti.getPrimitiveCategory())
        || unSupportedTypes.contains(descti.getPrimitiveCategory())) {
      // FIXME: support template types. It currently has conflict with
      // ExprNodeConstantDesc
      return null;
    }
    LOG.debug("Casting " + desc + " to type " + ti);
    ExprNodeConstantDesc c = (ExprNodeConstantDesc) desc;
    if (null != c.getFoldedFromVal() && priti.getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
      // avoid double casting to preserve original string representation of constant.
      return new ExprNodeConstantDesc(c.getFoldedFromVal());
    }
    ObjectInspector origOI =
        TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(desc.getTypeInfo());
    ObjectInspector oi =
        TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(ti);
    Converter converter = ObjectInspectorConverters.getConverter(origOI, oi);
    Object convObj = converter.convert(c.getValue());

    // Convert integer related types because converters are not sufficient
    if (convObj instanceof Integer) {
      switch (priti.getPrimitiveCategory()) {
        case BYTE:
          convObj = new Byte((byte) (((Integer) convObj).intValue()));
          break;
        case SHORT:
          convObj = new Short((short) ((Integer) convObj).intValue());
          break;
        case LONG:
          convObj = new Long(((Integer) convObj).intValue());
        default:
      }
    }
    return new ExprNodeConstantDesc(ti, convObj);
  }

  public static ExprNodeDesc foldExpr(ExprNodeGenericFuncDesc funcDesc) {

    GenericUDF udf = funcDesc.getGenericUDF();
    if (!isDeterministicUdf(udf)) {
      return funcDesc;
    }
    return evaluateFunction(funcDesc.getGenericUDF(),funcDesc.getChildren(), funcDesc.getChildren());
  }
  /**
   * Fold input expression desc.
   *
   * If desc is a UDF and all parameters are constants, evaluate it. If desc is a column expression,
   * find it from propagated constants, and if there is, replace it with constant.
   *
   * @param desc folding expression
   * @param constants current propagated constant map
   * @param cppCtx
   * @param op processing operator
   * @param propagate if true, assignment expressions will be added to constants.
   * @return fold expression
   */
  private static ExprNodeDesc foldExpr(ExprNodeDesc desc, Map<ColumnInfo, ExprNodeDesc> constants,
      ConstantPropagateProcCtx cppCtx, Operator<? extends Serializable> op, int tag,
      boolean propagate) {
    if (desc instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc) desc;

      // The function must be deterministic, or we can't fold it.
      GenericUDF udf = funcDesc.getGenericUDF();
      if (!isDeterministicUdf(udf)) {
        LOG.debug("Function " + udf.getClass() + " undeterministic, quit folding.");
        return desc;
      }

      boolean propagateNext = propagate && propagatableUdfs.contains(udf.getClass());
      List<ExprNodeDesc> newExprs = new ArrayList<ExprNodeDesc>();
      for (ExprNodeDesc childExpr : desc.getChildren()) {
        newExprs.add(foldExpr(childExpr, constants, cppCtx, op, tag, propagateNext));
      }

      // If all child expressions are constants, evaluate UDF immediately
      ExprNodeDesc constant = evaluateFunction(udf, newExprs, desc.getChildren());
      if (constant != null) {
        LOG.debug("Folding expression:" + desc + " -> " + constant);
        return constant;
      } else {

        // Check if the function can be short cut.
        ExprNodeDesc shortcut = shortcutFunction(udf, newExprs);
        if (shortcut != null) {
          LOG.debug("Folding expression:" + desc + " -> " + shortcut);
          return shortcut;
        }
        ((ExprNodeGenericFuncDesc) desc).setChildren(newExprs);
      }

      // If in some selected binary operators (=, is null, etc), one of the
      // expressions are
      // constant, add them to colToConstatns as half-deterministic columns.
      if (propagate) {
        propagate(udf, newExprs, cppCtx.getRowResolver(op), constants);
      }

      return desc;
    } else if (desc instanceof ExprNodeColumnDesc) {
      if (op.getParentOperators() == null || op.getParentOperators().isEmpty()) {
        return desc;
      }
      Operator<? extends Serializable> parent = op.getParentOperators().get(tag);
      ExprNodeDesc col = evaluateColumn((ExprNodeColumnDesc) desc, cppCtx, parent);
      if (col != null) {
        LOG.debug("Folding expression:" + desc + " -> " + col);
        return col;
      }
    }
    return desc;
  }

  private static boolean isDeterministicUdf(GenericUDF udf) {
    UDFType udfType = udf.getClass().getAnnotation(UDFType.class);
    if (udf instanceof GenericUDFBridge) {
      udfType = ((GenericUDFBridge) udf).getUdfClass().getAnnotation(UDFType.class);
    }
    if (udfType.deterministic() == false) {
      return false;
    }

    // If udf is requiring additional jars, we can't determine the result in
    // compile time.
    String[] files;
    String[] jars;
    if (udf instanceof GenericUDFBridge) {
      GenericUDFBridge bridge = (GenericUDFBridge) udf;
      String udfClassName = bridge.getUdfClassName();
      try {
        UDF udfInternal =
            (UDF) Class.forName(bridge.getUdfClassName(), true, Utilities.getSessionSpecifiedClassLoader())
                .newInstance();
        files = udfInternal.getRequiredFiles();
        jars = udfInternal.getRequiredJars();
      } catch (Exception e) {
        LOG.error("The UDF implementation class '" + udfClassName
            + "' is not present in the class path");
        return false;
      }
    } else {
      files = udf.getRequiredFiles();
      jars = udf.getRequiredJars();
    }
    if (files != null || jars != null) {
      return false;
    }
    return true;
  }

  /**
   * Propagate assignment expression, adding an entry into constant map constants.
   *
   * @param udf expression UDF, currently only 2 UDFs are supported: '=' and 'is null'.
   * @param newExprs child expressions (parameters).
   * @param cppCtx
   * @param op
   * @param constants
   */
  private static void propagate(GenericUDF udf, List<ExprNodeDesc> newExprs, RowResolver rr,
      Map<ColumnInfo, ExprNodeDesc> constants) {
    if (udf instanceof GenericUDFOPEqual) {
      ExprNodeDesc lOperand = newExprs.get(0);
      ExprNodeDesc rOperand = newExprs.get(1);
      ExprNodeConstantDesc v;
      if (lOperand instanceof ExprNodeConstantDesc) {
        v = (ExprNodeConstantDesc) lOperand;
      } else if (rOperand instanceof ExprNodeConstantDesc) {
        v = (ExprNodeConstantDesc) rOperand;
      } else {
        // we need a constant on one side.
        return;
      }
      // If both sides are constants, there is nothing to propagate
      ExprNodeColumnDesc c = getColumnExpr(lOperand);
      if (null == c) {
        c = getColumnExpr(rOperand);
      }
      if (null == c) {
        // we need a column expression on other side.
        return;
      }
      ColumnInfo ci = resolveColumn(rr, c);
      if (ci != null) {
        LOG.debug("Filter " + udf + " is identified as a value assignment, propagate it.");
        if (!v.getTypeInfo().equals(ci.getType())) {
          v = typeCast(v, ci.getType());
        }
        if (v != null) {
          constants.put(ci, v);
        }
      }
    } else if (udf instanceof GenericUDFOPNull) {
      ExprNodeDesc operand = newExprs.get(0);
      if (operand instanceof ExprNodeColumnDesc) {
        LOG.debug("Filter " + udf + " is identified as a value assignment, propagate it.");
        ExprNodeColumnDesc c = (ExprNodeColumnDesc) operand;
        ColumnInfo ci = resolveColumn(rr, c);
        if (ci != null) {
          constants.put(ci, new ExprNodeNullDesc());
        }
      }
    }
  }

  private static ExprNodeColumnDesc getColumnExpr(ExprNodeDesc expr) {
    while (FunctionRegistry.isOpCast(expr)) {
      expr = expr.getChildren().get(0);
    }
    return (expr instanceof ExprNodeColumnDesc) ? (ExprNodeColumnDesc)expr : null;
  }

  private static ExprNodeDesc shortcutFunction(GenericUDF udf, List<ExprNodeDesc> newExprs) {
    if (udf instanceof GenericUDFOPAnd) {
      for (int i = 0; i < 2; i++) {
        ExprNodeDesc childExpr = newExprs.get(i);
        ExprNodeDesc other = newExprs.get(Math.abs(i - 1));
        if (childExpr instanceof ExprNodeConstantDesc) {
          ExprNodeConstantDesc c = (ExprNodeConstantDesc) childExpr;
          if (Boolean.TRUE.equals(c.getValue())) {

            // if true, prune it
            return other;
          } else {

            // if false return false
            return childExpr;
          }
        } else // Try to fold (key = 86) and (key is not null) to (key = 86) 
        if (childExpr instanceof ExprNodeGenericFuncDesc &&
            ((ExprNodeGenericFuncDesc)childExpr).getGenericUDF() instanceof GenericUDFOPNotNull &&
            childExpr.getChildren().get(0) instanceof ExprNodeColumnDesc && other instanceof ExprNodeGenericFuncDesc
            && ((ExprNodeGenericFuncDesc)other).getGenericUDF() instanceof GenericUDFBaseCompare
            && other.getChildren().size() == 2) {
          ExprNodeColumnDesc colDesc = getColumnExpr(other.getChildren().get(0));
          if (null == colDesc) {
            colDesc = getColumnExpr(other.getChildren().get(1));
          }
          if (null != colDesc && colDesc.isSame(childExpr.getChildren().get(0))) {
            return other;
          }
        }
      }
    }

    if (udf instanceof GenericUDFOPOr) {
      for (int i = 0; i < 2; i++) {
        ExprNodeDesc childExpr = newExprs.get(i);
        if (childExpr instanceof ExprNodeConstantDesc) {
          ExprNodeConstantDesc c = (ExprNodeConstantDesc) childExpr;
          if (Boolean.FALSE.equals(c.getValue())) {

            // if false, prune it
            return newExprs.get(Math.abs(i - 1));
          } else {

            // if true return true
            return childExpr;
          }
        }
      }
    }

    return null;
  }

  /**
   * Evaluate column, replace the deterministic columns with constants if possible
   *
   * @param desc
   * @param ctx
   * @param op
   * @param colToConstants
   * @return
   */
  private static ExprNodeDesc evaluateColumn(ExprNodeColumnDesc desc,
      ConstantPropagateProcCtx cppCtx, Operator<? extends Serializable> parent) {
    try {
      ColumnInfo ci = null;
      RowResolver rr = cppCtx.getOpToParseCtxMap().get(parent).getRowResolver();
      String[] tmp = rr.reverseLookup(desc.getColumn());
      if (tmp == null) {
        LOG.error("Reverse look up of column " + desc + " error!");
        return null;
      }
      ci = rr.get(tmp[0], tmp[1]);
      if (ci != null) {
        ExprNodeDesc constant = null;
        // Additional work for union operator, see union27.q
        if (ci.getAlias() == null) {
          for (Entry<ColumnInfo, ExprNodeDesc> e : cppCtx.getOpToConstantExprs().get(parent).entrySet()) {
            if (e.getKey().getInternalName().equals(ci.getInternalName())) {
              constant = e.getValue();
              break;
            }
          }
        } else {
          constant = cppCtx.getOpToConstantExprs().get(parent).get(ci);
        }
        if (constant != null) {
          if (constant instanceof ExprNodeConstantDesc
              && !constant.getTypeInfo().equals(desc.getTypeInfo())) {
            return typeCast(constant, desc.getTypeInfo());
          }
          return constant;
        } else {
          return null;
        }
      }
      LOG.error("Can't resolve " + desc.getTabAlias() + "." + desc.getColumn());
      throw new RuntimeException("Can't resolve " + desc.getTabAlias() + "." + desc.getColumn());
    } catch (SemanticException e) {
      throw new RuntimeException(e);
    }

  }

  /**
   * Evaluate UDF
   *
   * @param udf UDF object
   * @param exprs
   * @param oldExprs
   * @return null if expression cannot be evaluated (not all parameters are constants). Or evaluated
   *         ExprNodeConstantDesc if possible.
   * @throws HiveException
   */
  private static ExprNodeDesc evaluateFunction(GenericUDF udf, List<ExprNodeDesc> exprs,
      List<ExprNodeDesc> oldExprs) {
    DeferredJavaObject[] arguments = new DeferredJavaObject[exprs.size()];
    ObjectInspector[] argois = new ObjectInspector[exprs.size()];
    for (int i = 0; i < exprs.size(); i++) {
      ExprNodeDesc desc = exprs.get(i);
      if (desc instanceof ExprNodeConstantDesc) {
        ExprNodeConstantDesc constant = (ExprNodeConstantDesc) exprs.get(i);
        if (!constant.getTypeInfo().equals(oldExprs.get(i).getTypeInfo())) {
          constant = typeCast(constant, oldExprs.get(i).getTypeInfo());
          if (constant == null) {
            return null;
          }
        }
        Object value = constant.getValue();
        PrimitiveTypeInfo pti = (PrimitiveTypeInfo) constant.getTypeInfo();
        Object writableValue =
            PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(pti)
                .getPrimitiveWritableObject(value);
        arguments[i] = new DeferredJavaObject(writableValue);
        argois[i] =
            ObjectInspectorUtils.getConstantObjectInspector(constant.getWritableObjectInspector(),
                writableValue);
      } else if (desc instanceof ExprNodeNullDesc) {

        // FIXME: add null support.
        return null;
      } else if (desc instanceof ExprNodeGenericFuncDesc) {
        ExprNodeDesc evaluatedFn = foldExpr((ExprNodeGenericFuncDesc)desc);
        if (null == evaluatedFn || !(evaluatedFn instanceof ExprNodeConstantDesc)) {
          return null;
        }
        ExprNodeConstantDesc constant = (ExprNodeConstantDesc) evaluatedFn;
        Object writableValue = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
          (PrimitiveTypeInfo) constant.getTypeInfo()).getPrimitiveWritableObject(constant.getValue());
        arguments[i] = new DeferredJavaObject(writableValue);
        argois[i] = ObjectInspectorUtils.getConstantObjectInspector(constant.getWritableObjectInspector(), writableValue);
      } else {
        return null;
      }
    }

    try {
      ObjectInspector oi = udf.initialize(argois);
      Object o = udf.evaluate(arguments);
      LOG.debug(udf.getClass().getName() + "(" + exprs + ")=" + o);
      if (o == null) {
        return new ExprNodeNullDesc();
      }
      Class<?> clz = o.getClass();
      if (PrimitiveObjectInspectorUtils.isPrimitiveWritableClass(clz)) {
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
        TypeInfo typeInfo = poi.getTypeInfo();

        // Handling parameterized types (varchar, decimal, etc).
        if (typeInfo.getTypeName().contains(serdeConstants.DECIMAL_TYPE_NAME)
            || typeInfo.getTypeName().contains(serdeConstants.VARCHAR_TYPE_NAME)
            || typeInfo.getTypeName().contains(serdeConstants.CHAR_TYPE_NAME)) {

          // Do not support parameterized types.
          return null;
        }
        o = poi.getPrimitiveJavaObject(o);
      } else if (PrimitiveObjectInspectorUtils.isPrimitiveJavaClass(clz)) {

      } else {
        LOG.error("Unable to evaluate " + udf + ". Return value unrecoginizable.");
        return null;
      }
      String constStr = null;
      if(arguments.length == 1 && FunctionRegistry.isOpCast(udf)) {
        // remember original string representation of constant.
        constStr = arguments[0].get().toString();
      }
      return new ExprNodeConstantDesc(o).setFoldedFromVal(constStr);
    } catch (HiveException e) {
      LOG.error("Evaluation function " + udf.getClass()
          + " failed in Constant Propagatation Optimizer.");
      throw new RuntimeException(e);
    }
  }

  /**
   * Change operator row schema, replace column with constant if it is.
   *
   * @param op
   * @param constants
   * @throws SemanticException
   */
  private static void foldOperator(Operator<? extends Serializable> op,
      ConstantPropagateProcCtx cppCtx) throws SemanticException {
    RowSchema schema = op.getSchema();
    Map<ColumnInfo, ExprNodeDesc> constants = cppCtx.getOpToConstantExprs().get(op);
    if (schema != null && schema.getSignature() != null) {
      for (ColumnInfo col : schema.getSignature()) {
        ExprNodeDesc constant = constants.get(col);
        if (constant != null) {
          LOG.debug("Replacing column " + col + " with constant " + constant + " in " + op);
          if (!col.getType().equals(constant.getTypeInfo())) {
            constant = typeCast(constant, col.getType());
          }
          if (constant != null) {
            col.setObjectinspector(constant.getWritableObjectInspector());
          }
        }
      }
    }

    Map<String, ExprNodeDesc> colExprMap = op.getColumnExprMap();
    if (colExprMap != null) {
      for (Entry<ColumnInfo, ExprNodeDesc> e : constants.entrySet()) {
        String internalName = e.getKey().getInternalName();
        if (colExprMap.containsKey(internalName)) {
          colExprMap.put(internalName, e.getValue());
        }
      }
    }
  }

  /**
   * Node Processor for Constant Propagation on Filter Operators. The processor is to fold
   * conditional expressions and extract assignment expressions and propagate them.
   */
  public static class ConstantPropagateFilterProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      FilterOperator op = (FilterOperator) nd;
      ConstantPropagateProcCtx cppCtx = (ConstantPropagateProcCtx) ctx;
      Map<ColumnInfo, ExprNodeDesc> constants = cppCtx.getPropagatedConstants(op);
      cppCtx.getOpToConstantExprs().put(op, constants);

      ExprNodeDesc condn = op.getConf().getPredicate();
      LOG.debug("Old filter FIL[" + op.getIdentifier() + "] conditions:" + condn.getExprString());
      ExprNodeDesc newCondn = foldExpr(condn, constants, cppCtx, op, 0, true);
      if (newCondn instanceof ExprNodeConstantDesc) {
        ExprNodeConstantDesc c = (ExprNodeConstantDesc) newCondn;
        if (Boolean.TRUE.equals(c.getValue())) {
          cppCtx.addOpToDelete(op);
          LOG.debug("Filter expression " + condn + " holds true. Will delete it.");
        } else if (Boolean.FALSE.equals(c.getValue())) {
          LOG.warn("Filter expression " + condn + " holds false!");
        }
      }
      LOG.debug("New filter FIL[" + op.getIdentifier() + "] conditions:" + newCondn.getExprString());

      // merge it with the downstream col list
      op.getConf().setPredicate(newCondn);
      foldOperator(op, cppCtx);
      return null;
    }

  }

  /**
   * Factory method to get the ConstantPropagateFilterProc class.
   *
   * @return ConstantPropagateFilterProc
   */
  public static ConstantPropagateFilterProc getFilterProc() {
    return new ConstantPropagateFilterProc();
  }

  /**
   * Node Processor for Constant Propagate for Group By Operators.
   */
  public static class ConstantPropagateGroupByProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      GroupByOperator op = (GroupByOperator) nd;
      ConstantPropagateProcCtx cppCtx = (ConstantPropagateProcCtx) ctx;
      Map<ColumnInfo, ExprNodeDesc> colToConstants = cppCtx.getPropagatedConstants(op);
      cppCtx.getOpToConstantExprs().put(op, colToConstants);

      if (colToConstants.isEmpty()) {
        return null;
      }

      GroupByDesc conf = op.getConf();
      ArrayList<ExprNodeDesc> keys = conf.getKeys();
      for (int i = 0; i < keys.size(); i++) {
        ExprNodeDesc key = keys.get(i);
        ExprNodeDesc newkey = foldExpr(key, colToConstants, cppCtx, op, 0, false);
        keys.set(i, newkey);
      }
      foldOperator(op, cppCtx);
      return null;
    }
  }

  /**
   * Factory method to get the ConstantPropagateGroupByProc class.
   *
   * @return ConstantPropagateGroupByProc
   */
  public static ConstantPropagateGroupByProc getGroupByProc() {
    return new ConstantPropagateGroupByProc();
  }

  /**
   * The Default Node Processor for Constant Propagation.
   */
  public static class ConstantPropagateDefaultProc implements NodeProcessor {
    @Override
    @SuppressWarnings("unchecked")
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      ConstantPropagateProcCtx cppCtx = (ConstantPropagateProcCtx) ctx;
      Operator<? extends Serializable> op = (Operator<? extends Serializable>) nd;
      Map<ColumnInfo, ExprNodeDesc> constants = cppCtx.getPropagatedConstants(op);
      cppCtx.getOpToConstantExprs().put(op, constants);
      if (constants.isEmpty()) {
        return null;
      }
      foldOperator(op, cppCtx);
      return null;
    }
  }

  /**
   * Factory method to get the ConstantPropagateDefaultProc class.
   *
   * @return ConstantPropagateDefaultProc
   */
  public static ConstantPropagateDefaultProc getDefaultProc() {
    return new ConstantPropagateDefaultProc();
  }

  /**
   * The Node Processor for Constant Propagation for Select Operators.
   */
  public static class ConstantPropagateSelectProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      SelectOperator op = (SelectOperator) nd;
      ConstantPropagateProcCtx cppCtx = (ConstantPropagateProcCtx) ctx;
      Map<ColumnInfo, ExprNodeDesc> constants = cppCtx.getPropagatedConstants(op);
      cppCtx.getOpToConstantExprs().put(op, constants);
      foldOperator(op, cppCtx);
      List<ExprNodeDesc> colList = op.getConf().getColList();
      List<String> columnNames = op.getConf().getOutputColumnNames();
      Map<String, ExprNodeDesc> columnExprMap = op.getColumnExprMap();
      if (colList != null) {
        for (int i = 0; i < colList.size(); i++) {
          ExprNodeDesc newCol = foldExpr(colList.get(i), constants, cppCtx, op, 0, false);
          if (!(colList.get(i) instanceof ExprNodeConstantDesc) && newCol instanceof ExprNodeConstantDesc) {
            // Lets try to store original column name, if this column got folded
            // This is useful for optimizations like GroupByOptimizer
            String colName = colList.get(i).getExprString();
            if (HiveConf.getPositionFromInternalName(colName) == -1) {
              // if its not an internal name, this is what we want.
              ((ExprNodeConstantDesc)newCol).setFoldedFromCol(colName);
            } else {
              // If it was internal column, lets try to get name from columnExprMap
              ExprNodeDesc desc = columnExprMap.get(colName);
              if (desc instanceof ExprNodeConstantDesc) {
                ((ExprNodeConstantDesc)newCol).setFoldedFromCol(((ExprNodeConstantDesc)desc).getFoldedFromCol());
              }
            }
          }
          colList.set(i, newCol);
          if (columnExprMap != null) {
            columnExprMap.put(columnNames.get(i), newCol);
          }
        }
        LOG.debug("New column list:(" + StringUtils.join(colList, " ") + ")");
      }
      return null;
    }
  }

  /**
   * The Factory method to get the ConstantPropagateSelectProc class.
   *
   * @return ConstantPropagateSelectProc
   */
  public static ConstantPropagateSelectProc getSelectProc() {
    return new ConstantPropagateSelectProc();
  }

  /**
   * The Node Processor for constant propagation for FileSink Operators. In addition to constant
   * propagation, this processor also prunes dynamic partitions to static partitions if possible.
   */
  public static class ConstantPropagateFileSinkProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      FileSinkOperator op = (FileSinkOperator) nd;
      ConstantPropagateProcCtx cppCtx = (ConstantPropagateProcCtx) ctx;
      Map<ColumnInfo, ExprNodeDesc> constants = cppCtx.getPropagatedConstants(op);
      cppCtx.getOpToConstantExprs().put(op, constants);
      if (constants.isEmpty()) {
        return null;
      }
      FileSinkDesc fsdesc = op.getConf();
      DynamicPartitionCtx dpCtx = fsdesc.getDynPartCtx();
      if (dpCtx != null) {

        // If all dynamic partitions are propagated as constant, remove DP.
        Set<String> inputs = dpCtx.getInputToDPCols().keySet();

        // Assume only 1 parent for FS operator
        Operator<? extends Serializable> parent = op.getParentOperators().get(0);
        Map<ColumnInfo, ExprNodeDesc> parentConstants = cppCtx.getPropagatedConstants(parent);
        RowResolver rr = cppCtx.getOpToParseCtxMap().get(parent).getRowResolver();
        boolean allConstant = true;
        for (String input : inputs) {
          String tmp[] = rr.reverseLookup(input);
          ColumnInfo ci = rr.get(tmp[0], tmp[1]);
          if (parentConstants.get(ci) == null) {
            allConstant = false;
            break;
          }
        }
        if (allConstant) {
          pruneDP(fsdesc);
        }
      }
      foldOperator(op, cppCtx);
      return null;
    }

    private void pruneDP(FileSinkDesc fsdesc) {
      // FIXME: Support pruning dynamic partitioning.
      LOG.info("DP can be rewritten to SP!");
    }
  }

  public static NodeProcessor getFileSinkProc() {
    return new ConstantPropagateFileSinkProc();
  }

  /**
   * The Node Processor for Constant Propagation for Operators which is designed to stop propagate.
   * Currently these kinds of Operators include UnionOperator and ScriptOperator.
   */
  public static class ConstantPropagateStopProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      Operator<?> op = (Operator<?>) nd;
      ConstantPropagateProcCtx cppCtx = (ConstantPropagateProcCtx) ctx;
      cppCtx.getOpToConstantExprs().put(op, new HashMap<ColumnInfo, ExprNodeDesc>());
      LOG.debug("Stop propagate constants on op " + op.getOperatorId());
      return null;
    }
  }

  public static NodeProcessor getStopProc() {
    return new ConstantPropagateStopProc();
  }

  /**
   * The Node Processor for Constant Propagation for ReduceSink Operators. If the RS Operator is for
   * a join, then only those constants from inner join tables, or from the 'inner side' of a outer
   * join (left table for left outer join and vice versa) can be propagated.
   */
  public static class ConstantPropagateReduceSinkProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      ReduceSinkOperator op = (ReduceSinkOperator) nd;
      ReduceSinkDesc rsDesc = op.getConf();
      ConstantPropagateProcCtx cppCtx = (ConstantPropagateProcCtx) ctx;
      Map<ColumnInfo, ExprNodeDesc> constants = cppCtx.getPropagatedConstants(op);

      cppCtx.getOpToConstantExprs().put(op, constants);
      if (constants.isEmpty()) {
        return null;
      }

      if (op.getChildOperators().size() == 1
          && op.getChildOperators().get(0) instanceof JoinOperator) {
        JoinOperator joinOp = (JoinOperator) op.getChildOperators().get(0);
        if (skipFolding(joinOp.getConf())) {
          LOG.debug("Skip folding in outer join " + op);
          cppCtx.getOpToConstantExprs().put(op, new HashMap<ColumnInfo, ExprNodeDesc>());
          return null;
        }
      }

      if (rsDesc.getDistinctColumnIndices() != null
          && !rsDesc.getDistinctColumnIndices().isEmpty()) {
        LOG.debug("Skip folding in distinct subqueries " + op);
        cppCtx.getOpToConstantExprs().put(op, new HashMap<ColumnInfo, ExprNodeDesc>());
        return null;
      }

      // key columns
      ArrayList<ExprNodeDesc> newKeyEpxrs = new ArrayList<ExprNodeDesc>();
      for (ExprNodeDesc desc : rsDesc.getKeyCols()) {
        ExprNodeDesc newDesc = foldExpr(desc, constants, cppCtx, op, 0, false);
        if (newDesc != desc && desc instanceof ExprNodeColumnDesc && newDesc instanceof ExprNodeConstantDesc) {
          ((ExprNodeConstantDesc)newDesc).setFoldedFromCol(((ExprNodeColumnDesc)desc).getColumn());
        }
        newKeyEpxrs.add(newDesc);
      }
      rsDesc.setKeyCols(newKeyEpxrs);

      // partition columns
      ArrayList<ExprNodeDesc> newPartExprs = new ArrayList<ExprNodeDesc>();
      for (ExprNodeDesc desc : rsDesc.getPartitionCols()) {
        ExprNodeDesc expr = foldExpr(desc, constants, cppCtx, op, 0, false);
        if (expr != desc && desc instanceof ExprNodeColumnDesc
            && expr instanceof ExprNodeConstantDesc) {
          ((ExprNodeConstantDesc) expr).setFoldedFromCol(((ExprNodeColumnDesc) desc).getColumn());
        }
        newPartExprs.add(expr);
      }
      rsDesc.setPartitionCols(newPartExprs);

      // value columns
      ArrayList<ExprNodeDesc> newValExprs = new ArrayList<ExprNodeDesc>();
      for (ExprNodeDesc desc : rsDesc.getValueCols()) {
        newValExprs.add(foldExpr(desc, constants, cppCtx, op, 0, false));
      }
      rsDesc.setValueCols(newValExprs);
      foldOperator(op, cppCtx);
      return null;
    }

    /**
     * Skip folding constants if there is outer join in join tree.
     * @param joinDesc
     * @return true if to skip.
     */
    private boolean skipFolding(JoinDesc joinDesc) {
      for (JoinCondDesc cond : joinDesc.getConds()) {
        if (cond.getType() == JoinDesc.INNER_JOIN || cond.getType() == JoinDesc.UNIQUE_JOIN) {
          continue;
        }
        return true;
      }
      return false;
    }

  }

  public static NodeProcessor getReduceSinkProc() {
    return new ConstantPropagateReduceSinkProc();
  }

  /**
   * The Node Processor for Constant Propagation for Join Operators.
   */
  public static class ConstantPropagateJoinProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      JoinOperator op = (JoinOperator) nd;
      JoinDesc conf = op.getConf();
      ConstantPropagateProcCtx cppCtx = (ConstantPropagateProcCtx) ctx;
      Map<ColumnInfo, ExprNodeDesc> constants = cppCtx.getPropagatedConstants(op);
      cppCtx.getOpToConstantExprs().put(op, constants);
      if (constants.isEmpty()) {
        return null;
      }

      // Note: the following code (removing folded constants in exprs) is deeply coupled with
      //    ColumnPruner optimizer.
      // Assuming ColumnPrunner will remove constant columns so we don't deal with output columns.
      //    Except one case that the join operator is followed by a redistribution (RS operator).
      if (op.getChildOperators().size() == 1
          && op.getChildOperators().get(0) instanceof ReduceSinkOperator) {
        LOG.debug("Skip JOIN-RS structure.");
        return null;
      }
      LOG.info("Old exprs " + conf.getExprs());
      Iterator<Entry<Byte, List<ExprNodeDesc>>> itr = conf.getExprs().entrySet().iterator();
      while (itr.hasNext()) {
        Entry<Byte, List<ExprNodeDesc>> e = itr.next();
        int tag = e.getKey();
        List<ExprNodeDesc> exprs = e.getValue();
        if (exprs == null) {
          continue;
        }
        List<ExprNodeDesc> newExprs = new ArrayList<ExprNodeDesc>();
        for (ExprNodeDesc expr : exprs) {
          ExprNodeDesc newExpr = foldExpr(expr, constants, cppCtx, op, tag, false);
          if (newExpr instanceof ExprNodeConstantDesc || newExpr instanceof ExprNodeNullDesc) {
            LOG.info("expr " + newExpr + " fold from " + expr + " is removed.");
            continue;
          }
          newExprs.add(newExpr);
        }
        e.setValue(newExprs);
      }
      LOG.info("New exprs " + conf.getExprs());

      for (List<ExprNodeDesc> v : conf.getFilters().values()) {
        for (int i = 0; i < v.size(); i++) {
          ExprNodeDesc expr = foldExpr(v.get(i), constants, cppCtx, op, 0, false);
          v.set(i, expr);
        }
      }
      foldOperator(op, cppCtx);
      return null;
    }

  }

  public static NodeProcessor getJoinProc() {
    return new ConstantPropagateJoinProc();
  }

  /**
   * The Node Processor for Constant Propagation for Table Scan Operators.
   */
  public static class ConstantPropagateTableScanProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      TableScanOperator op = (TableScanOperator) nd;
      TableScanDesc conf = op.getConf();
      ConstantPropagateProcCtx cppCtx = (ConstantPropagateProcCtx) ctx;
      Map<ColumnInfo, ExprNodeDesc> constants = cppCtx.getPropagatedConstants(op);
      cppCtx.getOpToConstantExprs().put(op, constants);
      ExprNodeGenericFuncDesc pred = conf.getFilterExpr();
      if (pred == null) {
        return null;
      }

      ExprNodeDesc constant = foldExpr(pred, constants, cppCtx, op, 0, false);
      if (constant instanceof ExprNodeGenericFuncDesc) {
        conf.setFilterExpr((ExprNodeGenericFuncDesc) constant);
      } else {
        conf.setFilterExpr(null);
      }
      return null;
    }
  }

  public static NodeProcessor getTableScanProc() {
    return new ConstantPropagateTableScanProc();
  }
}
