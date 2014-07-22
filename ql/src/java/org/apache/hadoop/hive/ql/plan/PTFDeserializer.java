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

package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.LeadLagInfo;
import org.apache.hadoop.hive.ql.parse.WindowingExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.plan.ptf.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFInputDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFQueryInputDef;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.plan.ptf.ShapeDetails;
import org.apache.hadoop.hive.ql.plan.ptf.ValueBoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFunctionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowTableFunctionDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLeadLag;
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionEvaluator;
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionResolver;
import org.apache.hadoop.hive.ql.udf.ptf.WindowingTableFunction.WindowingTableFunctionResolver;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.util.ReflectionUtils;

@SuppressWarnings("deprecation")
public class PTFDeserializer {

  PTFDesc ptfDesc;
  StructObjectInspector inputOI;
  Configuration hConf;
  LeadLagInfo llInfo;

  public PTFDeserializer(PTFDesc ptfDesc, StructObjectInspector inputOI, Configuration hConf) {
    super();
    this.ptfDesc = ptfDesc;
    ptfDesc.setCfg(hConf);
    this.inputOI = inputOI;
    this.hConf = hConf;
    llInfo = new LeadLagInfo();
    ptfDesc.setLlInfo(llInfo);
  }

  public void initializePTFChain(PartitionedTableFunctionDef tblFnDef) throws HiveException {
    Deque<PTFInputDef> ptfChain = new ArrayDeque<PTFInputDef>();
    PTFInputDef currentDef = tblFnDef;
    while (currentDef != null) {
      ptfChain.push(currentDef);
      currentDef = currentDef.getInput();
    }

    while (!ptfChain.isEmpty()) {
      currentDef = ptfChain.pop();
      if (currentDef instanceof PTFQueryInputDef) {
        initialize((PTFQueryInputDef) currentDef, inputOI);
      } else if (currentDef instanceof WindowTableFunctionDef) {
        initializeWindowing((WindowTableFunctionDef) currentDef);
      } else {
        initialize((PartitionedTableFunctionDef) currentDef);
      }
    }

    PTFDeserializer.alterOutputOIForStreaming(ptfDesc);
  }

  public void initializeWindowing(WindowTableFunctionDef def) throws HiveException {
    ShapeDetails inpShape = def.getInput().getOutputShape();

    /*
     * 1. setup resolve, make connections
     */
    TableFunctionEvaluator tEval = def.getTFunction();
    WindowingTableFunctionResolver tResolver =
        (WindowingTableFunctionResolver) constructResolver(def.getResolverClassName());
    tResolver.initialize(ptfDesc, def, tEval);


    /*
     * 2. initialize WFns.
     */
    for (WindowFunctionDef wFnDef : def.getWindowFunctions()) {
      if (wFnDef.getArgs() != null) {
        for (PTFExpressionDef arg : wFnDef.getArgs()) {
          initialize(arg, inpShape);
        }

      }
      if (wFnDef.getWindowFrame() != null) {
        WindowFrameDef wFrmDef = wFnDef.getWindowFrame();
        initialize(wFrmDef.getStart(), inpShape);
        initialize(wFrmDef.getEnd(), inpShape);
      }
      setupWdwFnEvaluator(wFnDef);
    }
    ArrayList<String> aliases = new ArrayList<String>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
    for (WindowFunctionDef wFnDef : def.getWindowFunctions()) {
      aliases.add(wFnDef.getAlias());
      if (wFnDef.isPivotResult()) {
        fieldOIs.add(((ListObjectInspector) wFnDef.getOI()).getListElementObjectInspector());
      } else {
        fieldOIs.add(wFnDef.getOI());
      }
    }

    PTFDeserializer.addInputColumnsToList(inpShape, aliases, fieldOIs);
    StructObjectInspector wdwOutOI = ObjectInspectorFactory.getStandardStructObjectInspector(
        aliases, fieldOIs);
    tResolver.setWdwProcessingOutputOI(wdwOutOI);
    initialize(def.getOutputShape(), wdwOutOI);
    tResolver.initializeOutputOI();
  }

  protected void initialize(PTFQueryInputDef def, StructObjectInspector OI) throws HiveException {
    ShapeDetails outShape = def.getOutputShape();
    initialize(outShape, OI);
  }

  protected void initialize(PartitionedTableFunctionDef def) throws HiveException {
    ShapeDetails inpShape = def.getInput().getOutputShape();

    /*
     * 1. initialize args
     */
    if (def.getArgs() != null) {
      for (PTFExpressionDef arg : def.getArgs()) {
        initialize(arg, inpShape);
      }
    }

    /*
     * 2. setup resolve, make connections
     */
    TableFunctionEvaluator tEval = def.getTFunction();
    // TableFunctionResolver tResolver = FunctionRegistry.getTableFunctionResolver(def.getName());
    TableFunctionResolver tResolver = constructResolver(def.getResolverClassName());
    tResolver.initialize(ptfDesc, def, tEval);

    /*
     * 3. give Evaluator chance to setup for RawInput execution; setup RawInput shape
     */
    if (tEval.isTransformsRawInput()) {
      tResolver.initializeRawInputOI();
      initialize(def.getRawInputShape(), tEval.getRawInputOI());
    } else {
      def.setRawInputShape(inpShape);
    }

    inpShape = def.getRawInputShape();

    /*
     * 4. give Evaluator chance to setup for Output execution; setup Output shape.
     */
    tResolver.initializeOutputOI();
    initialize(def.getOutputShape(), tEval.getOutputOI());
  }

  static void setupWdwFnEvaluator(WindowFunctionDef def) throws HiveException {
    List<PTFExpressionDef> args = def.getArgs();
    List<ObjectInspector> argOIs = new ArrayList<ObjectInspector>();
    ObjectInspector[] funcArgOIs = null;

    if (args != null) {
      for (PTFExpressionDef arg : args) {
        argOIs.add(arg.getOI());
      }
      funcArgOIs = new ObjectInspector[args.size()];
      funcArgOIs = argOIs.toArray(funcArgOIs);
    }

    GenericUDAFEvaluator wFnEval = def.getWFnEval();
    ObjectInspector OI = wFnEval.init(GenericUDAFEvaluator.Mode.COMPLETE, funcArgOIs);
    def.setWFnEval(wFnEval);
    def.setOI(OI);
  }

  protected void initialize(BoundaryDef def, ShapeDetails inpShape) throws HiveException {
    if (def instanceof ValueBoundaryDef) {
      ValueBoundaryDef vDef = (ValueBoundaryDef) def;
      initialize(vDef.getExpressionDef(), inpShape);
    }
  }

  protected void initialize(PTFExpressionDef eDef, ShapeDetails inpShape) throws HiveException {
    ExprNodeDesc exprNode = eDef.getExprNode();
    ExprNodeEvaluator exprEval = WindowingExprNodeEvaluatorFactory.get(llInfo, exprNode);
    ObjectInspector oi = initExprNodeEvaluator(exprEval, exprNode, inpShape);
    eDef.setExprEvaluator(exprEval);
    eDef.setOI(oi);
  }

  private ObjectInspector initExprNodeEvaluator(ExprNodeEvaluator exprEval,
      ExprNodeDesc exprNode,
      ShapeDetails inpShape)
      throws HiveException {
    ObjectInspector outOI;
    outOI = exprEval.initialize(inpShape.getOI());

    /*
     * if there are any LeadLag functions in this Expression Tree: - setup a
     * duplicate Evaluator for the 1st arg of the LLFuncDesc - initialize it
     * using the InputInfo provided for this Expr tree - set the duplicate
     * evaluator on the LLUDF instance.
     */
    List<ExprNodeGenericFuncDesc> llFuncExprs = llInfo.getLLFuncExprsInTopExpr(exprNode);
    if (llFuncExprs != null) {
      for (ExprNodeGenericFuncDesc llFuncExpr : llFuncExprs) {
        ExprNodeDesc firstArg = llFuncExpr.getChildren().get(0);
        ExprNodeEvaluator dupExprEval = WindowingExprNodeEvaluatorFactory.get(llInfo, firstArg);
        dupExprEval.initialize(inpShape.getOI());
        GenericUDFLeadLag llFn = (GenericUDFLeadLag) llFuncExpr.getGenericUDF();
        llFn.setExprEvaluator(dupExprEval);
      }
    }

    return outOI;
  }

  protected void initialize(ShapeDetails shp, StructObjectInspector OI) throws HiveException {
    String serdeClassName = shp.getSerdeClassName();
    Properties serDeProps = new Properties();
    Map<String, String> serdePropsMap = new LinkedHashMap<String, String>();
    addOIPropertiestoSerDePropsMap(OI, serdePropsMap);
    for (String serdeName : serdePropsMap.keySet()) {
      serDeProps.setProperty(serdeName, serdePropsMap.get(serdeName));
    }
    try {
      SerDe serDe =  ReflectionUtils.newInstance(hConf.getClassByName(serdeClassName).
          asSubclass(SerDe.class), hConf);
      SerDeUtils.initializeSerDe(serDe, hConf, serDeProps, null);
      shp.setSerde(serDe);
      StructObjectInspector outOI = PTFPartition.setupPartitionOutputOI(serDe, OI);
      shp.setOI(outOI);
    } catch (Exception se) {
      throw new HiveException(se);
    }
  }

  private static void addInputColumnsToList(ShapeDetails shape,
      ArrayList<String> fieldNames, ArrayList<ObjectInspector> fieldOIs)
  {
    StructObjectInspector OI = shape.getOI();
    for (StructField f : OI.getAllStructFieldRefs())
    {
      fieldNames.add(f.getFieldName());
      fieldOIs.add(f.getFieldObjectInspector());
    }
  }

  private TableFunctionResolver constructResolver(String className) throws HiveException {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends TableFunctionResolver> rCls = (Class<? extends TableFunctionResolver>)
          Class.forName(className);
      return ReflectionUtils.newInstance(rCls, null);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  @SuppressWarnings({"unchecked"})
  public static void addOIPropertiestoSerDePropsMap(StructObjectInspector OI,
      Map<String, String> serdePropsMap) {

    if (serdePropsMap == null) {
      return;
    }

    ArrayList<? extends Object>[] tInfo = getTypeMap(OI);

    ArrayList<String> columnNames = (ArrayList<String>) tInfo[0];
    ArrayList<TypeInfo> fields = (ArrayList<TypeInfo>) tInfo[1];
    StringBuilder cNames = new StringBuilder();
    StringBuilder cTypes = new StringBuilder();

    for (int i = 0; i < fields.size(); i++)
    {
      cNames.append(i > 0 ? "," : "");
      cTypes.append(i > 0 ? "," : "");
      cNames.append(columnNames.get(i));
      cTypes.append(fields.get(i).getTypeName());
    }

    serdePropsMap.put(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS,
        cNames.toString());
    serdePropsMap.put(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES,
        cTypes.toString());
  }

  private static ArrayList<? extends Object>[] getTypeMap(
      StructObjectInspector oi) {
    StructTypeInfo t = (StructTypeInfo) TypeInfoUtils
        .getTypeInfoFromObjectInspector(oi);
    ArrayList<String> fnames = t.getAllStructFieldNames();
    ArrayList<TypeInfo> fields = t.getAllStructFieldTypeInfos();
    return new ArrayList<?>[]
    {fnames, fields};
  }

  /*
   * If the final PTF in a PTFChain can stream its output, then set the OI of its OutputShape
   * to the OI returned by the TableFunctionEvaluator.
   */
  public static void alterOutputOIForStreaming(PTFDesc ptfDesc) {
    PartitionedTableFunctionDef tDef = ptfDesc.getFuncDef();
    TableFunctionEvaluator tEval = tDef.getTFunction();

    if ( tEval.canIterateOutput() ) {
      tDef.getOutputShape().setOI(tEval.getOutputOI());
    }
  }

}
