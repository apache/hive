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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.PTFPartition.PTFPartitionIterator;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.IntWritable;

public abstract class GenericUDFLeadLag extends GenericUDF {
  transient ExprNodeEvaluator exprEvaluator;
  transient PTFPartitionIterator<Object> pItr;
  transient ObjectInspector firstArgOI;
  transient Converter defaultValueConverter;
  int amt;

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object defaultVal = null;
    if (arguments.length == 3) {
      defaultVal = ObjectInspectorUtils.copyToStandardObject(
              defaultValueConverter.convert(arguments[2].get()), firstArgOI);
    }

    int idx = pItr.getIndex() - 1;
    int start = 0;
    int end = pItr.getPartition().size();
    try {
      Object ret = null;
      int newIdx = getIndex(amt);

      if (newIdx >= end || newIdx < start) {
        ret = defaultVal;
      } else {
        Object row = getRow(amt);
        ret = exprEvaluator.evaluate(row);
        ret = ObjectInspectorUtils.copyToStandardObject(ret, firstArgOI,
                ObjectInspectorCopyOption.WRITABLE);
      }
      return ret;
    } finally {
      Object currRow = pItr.resetToIndex(idx);
      // reevaluate expression on current Row, to trigger the Lazy object
      // caches to be reset to the current row.
      exprEvaluator.evaluate(currRow);
    }

  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (!(arguments.length >= 1 && arguments.length <= 3)) {
      throw new UDFArgumentTypeException(arguments.length - 1, "Incorrect invocation of "
              + _getFnName() + ": _FUNC_(expr, amt, default)");
    }

    amt = 1;
    if (arguments.length > 1) {
      ObjectInspector amtOI = arguments[1];
      if (!ObjectInspectorUtils.isConstantObjectInspector(amtOI)
              || (amtOI.getCategory() != ObjectInspector.Category.PRIMITIVE)
              || ((PrimitiveObjectInspector) amtOI).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
        throw new UDFArgumentTypeException(1, _getFnName() + " amount must be a integer value "
                + amtOI.getTypeName() + " was passed as parameter 1.");
      }
      Object o = ((ConstantObjectInspector) amtOI).getWritableConstantValue();
      amt = ((IntWritable) o).get();
      if (amt < 0) {
        throw new UDFArgumentTypeException(1,  " amount can not be nagative. Specified: " + amt);
      }
    }

    if (arguments.length == 3) {
      defaultValueConverter = ObjectInspectorConverters.getConverter(arguments[2], arguments[0]);
    }

    firstArgOI = arguments[0];
    return ObjectInspectorUtils.getStandardObjectInspector(firstArgOI,
            ObjectInspectorCopyOption.WRITABLE);
  }

  public ExprNodeEvaluator getExprEvaluator() {
    return exprEvaluator;
  }

  public void setExprEvaluator(ExprNodeEvaluator exprEvaluator) {
    this.exprEvaluator = exprEvaluator;
  }

  public PTFPartitionIterator<Object> getpItr() {
    return pItr;
  }

  public void setpItr(PTFPartitionIterator<Object> pItr) {
    this.pItr = pItr;
  }

  public ObjectInspector getFirstArgOI() {
    return firstArgOI;
  }

  public void setFirstArgOI(ObjectInspector firstArgOI) {
    this.firstArgOI = firstArgOI;
  }

  public Converter getDefaultValueConverter() {
    return defaultValueConverter;
  }

  public void setDefaultValueConverter(Converter defaultValueConverter) {
    this.defaultValueConverter = defaultValueConverter;
  }

  public int getAmt() {
    return amt;
  }

  public void setAmt(int amt) {
    this.amt = amt;
  }

  @Override
  public String getDisplayString(String[] children) {
    if (children.length != 2) {
      return _getFnName() + "(...)";
    }
    return getStandardDisplayString(_getFnName(), children);
  }

  protected abstract String _getFnName();

  protected abstract Object getRow(int amt) throws HiveException;

  protected abstract int getIndex(int amt);

}
