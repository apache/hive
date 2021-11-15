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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.mapred.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * GenericUDTFGetSplits2 -  Memory efficient version of GenericUDTFGetSplits.
 * It separates out information like schema and planBytes[] which is common to all the splits.
 * This produces output in following format.
 * <p>
 * type                           split
 * ----------------------------------------------------
 * schema-split             LlapInputSplit -- contains only schema
 * plan-split               LlapInputSplit -- contains only planBytes[]
 * 0                        LlapInputSplit -- actual split 1
 * 1                        LlapInputSplit -- actual split 2
 * ...                         ...
 */
@Description(name = "get_llap_splits", value = "_FUNC_(string,int) - "
    + "Returns an array of length int serialized splits for the referenced tables string."
    + " Passing length 0 returns only schema data for the compiled query. "
    + "The order of splits is: schema-split, plan-split, 0, 1, 2...where 0, 1, 2...are the actual splits "
    + "This UDTF is for internal use by LlapBaseInputFormat and not to be invoked explicitly")
@UDFType(deterministic = false)
public class GenericUDTFGetSplits2 extends GenericUDTFGetSplits {
  private static final Logger LOG = LoggerFactory.getLogger(GenericUDTFGetSplits2.class);

  @Override public StructObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    LOG.debug("initializing GenericUDFGetSplits2");
    validateInput(arguments);

    List<String> names = Arrays.asList("type", "split");
    List<ObjectInspector> fieldOIs = Arrays.asList(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector);
    StructObjectInspector outputOI = ObjectInspectorFactory.getStandardStructObjectInspector(names, fieldOIs);

    LOG.debug("done initializing GenericUDFGetSplits2");
    return outputOI;
  }

  @Override public void process(Object[] arguments) throws HiveException {
    try {
      initArgs(arguments);
      SplitResult splitResult = getSplitResult(true);
      forwardOutput(splitResult);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private void forwardOutput(SplitResult splitResult) throws IOException, HiveException {
    for (Map.Entry<String, InputSplit> entry : transformSplitResult(splitResult).entrySet()) {
      Object[] os = new Object[2];
      os[0] = entry.getKey();
      InputSplit split = entry.getValue();
      bos.reset();
      split.write(dos);
      os[1] = bos.toByteArray();
      forward(os);
    }
  }

  private Map<String, InputSplit> transformSplitResult(SplitResult splitResult) {
    Map<String, InputSplit> splitMap = new LinkedHashMap<>();
    splitMap.put("schema-split", splitResult.schemaSplit);
    if (splitResult.actualSplits != null && splitResult.actualSplits.length > 0) {
      Preconditions.checkNotNull(splitResult.planSplit);
      splitMap.put("plan-split", splitResult.planSplit);
      for (int i = 0; i < splitResult.actualSplits.length; i++) {
        splitMap.put("" + i, splitResult.actualSplits[i]);
      }
    }
    return splitMap;
  }
}
