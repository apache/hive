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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * BaseReduceSinkOperator
 **/
public abstract class BaseReduceSinkOperator<T extends BaseReduceSinkDesc> extends
  TerminalOperator<T> implements Serializable {

  private static final long serialVersionUID = 1L;
  protected static final Log LOG = LogFactory.getLog(BaseReduceSinkOperator.class
      .getName());

  /**
   * The evaluators for the key columns. Key columns decide the sort order on
   * the reducer side. Key columns are passed to the reducer in the "key".
   */
  protected transient ExprNodeEvaluator[] keyEval;
  /**
   * The evaluators for the value columns. Value columns are passed to reducer
   * in the "value".
   */
  protected transient ExprNodeEvaluator[] valueEval;
  /**
   * The evaluators for the partition columns (CLUSTER BY or DISTRIBUTE BY in
   * Hive language). Partition columns decide the reducer that the current row
   * goes to. Partition columns are not passed to reducer.
   */
  protected transient ExprNodeEvaluator[] partitionEval;

  // TODO: we use MetadataTypedColumnsetSerDe for now, till DynamicSerDe is
  // ready
  protected transient Serializer keySerializer;
  protected transient boolean keyIsText;
  protected transient Serializer valueSerializer;
  protected transient int tag;
  protected transient byte[] tagByte = new byte[1];
  protected transient int numDistributionKeys;
  protected transient int numDistinctExprs;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    try {
      keyEval = new ExprNodeEvaluator[conf.getKeyCols().size()];
      int i = 0;
      for (ExprNodeDesc e : conf.getKeyCols()) {
        keyEval[i++] = ExprNodeEvaluatorFactory.get(e);
      }

      numDistributionKeys = conf.getNumDistributionKeys();
      distinctColIndices = conf.getDistinctColumnIndices();
      numDistinctExprs = distinctColIndices.size();

      valueEval = new ExprNodeEvaluator[conf.getValueCols().size()];
      i = 0;
      for (ExprNodeDesc e : conf.getValueCols()) {
        valueEval[i++] = ExprNodeEvaluatorFactory.get(e);
      }

      partitionEval = new ExprNodeEvaluator[conf.getPartitionCols().size()];
      i = 0;
      for (ExprNodeDesc e : conf.getPartitionCols()) {
        partitionEval[i++] = ExprNodeEvaluatorFactory.get(e);
      }

      tag = conf.getTag();
      tagByte[0] = (byte) tag;
      LOG.info("Using tag = " + tag);

      TableDesc keyTableDesc = conf.getKeySerializeInfo();
      keySerializer = (Serializer) keyTableDesc.getDeserializerClass()
          .newInstance();
      keySerializer.initialize(null, keyTableDesc.getProperties());
      keyIsText = keySerializer.getSerializedClass().equals(Text.class);

      TableDesc valueTableDesc = conf.getValueSerializeInfo();
      valueSerializer = (Serializer) valueTableDesc.getDeserializerClass()
          .newInstance();
      valueSerializer.initialize(null, valueTableDesc.getProperties());

      isFirstRow = true;
      initializeChildren(hconf);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  protected transient InspectableObject tempInspectableObject = new InspectableObject();
  protected transient HiveKey keyWritable = new HiveKey();
  protected transient Writable value;

  protected transient StructObjectInspector keyObjectInspector;
  protected transient StructObjectInspector valueObjectInspector;
  protected transient ObjectInspector[] partitionObjectInspectors;

  protected transient Object[][] cachedKeys;
  protected transient Object[] cachedValues;
  protected transient List<List<Integer>> distinctColIndices;

  protected boolean isFirstRow;

  protected transient Random random;

  /**
   * Initializes array of ExprNodeEvaluator. Adds Union field for distinct
   * column indices for group by.
   * Puts the return values into a StructObjectInspector with output column
   * names.
   *
   * If distinctColIndices is empty, the object inspector is same as
   * {@link Operator#initEvaluatorsAndReturnStruct(ExprNodeEvaluator[], List, ObjectInspector)}
   */
  protected static StructObjectInspector initEvaluatorsAndReturnStruct(
      ExprNodeEvaluator[] evals, List<List<Integer>> distinctColIndices,
      List<String> outputColNames,
      int length, ObjectInspector rowInspector)
      throws HiveException {
    int inspectorLen = evals.length > length ? length + 1 : evals.length;
    List<ObjectInspector> sois = new ArrayList<ObjectInspector>(inspectorLen);

    // keys
    ObjectInspector[] fieldObjectInspectors = initEvaluators(evals, 0, length, rowInspector);
    sois.addAll(Arrays.asList(fieldObjectInspectors));

    if (evals.length > length) {
      // union keys
      List<ObjectInspector> uois = new ArrayList<ObjectInspector>();
      for (List<Integer> distinctCols : distinctColIndices) {
        List<String> names = new ArrayList<String>();
        List<ObjectInspector> eois = new ArrayList<ObjectInspector>();
        int numExprs = 0;
        for (int i : distinctCols) {
          names.add(HiveConf.getColumnInternalName(numExprs));
          eois.add(evals[i].initialize(rowInspector));
          numExprs++;
        }
        uois.add(ObjectInspectorFactory.getStandardStructObjectInspector(names, eois));
      }
      UnionObjectInspector uoi =
          ObjectInspectorFactory.getStandardUnionObjectInspector(uois);
      sois.add(uoi);
    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(outputColNames, sois);
  }

  @Override
  public abstract void processOp(Object row, int tag) throws HiveException;

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return "BaseReduceSink";
  }
}
