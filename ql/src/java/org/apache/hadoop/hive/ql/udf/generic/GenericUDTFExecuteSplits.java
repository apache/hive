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

package org.apache.hadoop.hive.ql.udf.generic;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFGetSplits.PlanFragment;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GenericUDTFExecuteSplits.
 *
 */
@Description(name = "execute_splits", value = "_FUNC_(string,int) - "
    + "Returns an array of length int serialized splits for the referenced tables string.")
@UDFType(deterministic = false)
public class GenericUDTFExecuteSplits extends GenericUDTFGetSplits {

  private static final Logger LOG = LoggerFactory.getLogger(GenericUDTFExecuteSplits.class);

  @Override
  public StructObjectInspector initialize(ObjectInspector[] arguments)
    throws UDFArgumentException {

    LOG.debug("initializing ExecuteSplits");

    if (SessionState.get() == null || SessionState.get().getConf() == null) {
      throw new IllegalStateException("Cannot run execute splits outside HS2");
    }

    if (arguments.length != 2) {
      throw new UDFArgumentLengthException("The function execute_splits accepts 2 arguments.");
    } else if (!(arguments[0] instanceof StringObjectInspector)) {
      LOG.error("Got "+arguments[0].getTypeName()+" instead of string.");
      throw new UDFArgumentTypeException(0, "\""
          + "string\" is expected at function execute_splits, " + "but \""
          + arguments[0].getTypeName() + "\" is found");
    } else if (!(arguments[1] instanceof IntObjectInspector)) {
      LOG.error("Got "+arguments[1].getTypeName()+" instead of int.");
      throw new UDFArgumentTypeException(1, "\""
          + "int\" is expected at function execute_splits, " + "but \""
          + arguments[1].getTypeName() + "\" is found");
    }

    stringOI = (StringObjectInspector) arguments[0];
    intOI = (IntObjectInspector) arguments[1];

    List<String> names = Arrays.asList("split_num","value");
    List<ObjectInspector> fieldOIs = Arrays.<ObjectInspector>asList(
      PrimitiveObjectInspectorFactory.javaIntObjectInspector,
      PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    StructObjectInspector outputOI = ObjectInspectorFactory.getStandardStructObjectInspector(names, fieldOIs);

    LOG.debug("done initializing GenericUDTFExecuteSplits");
    return outputOI;
  }

  @Override
  public void process(Object[] arguments) throws HiveException {

    String query = stringOI.getPrimitiveJavaObject(arguments[0]);
    int num = intOI.get(arguments[1]);

    PlanFragment fragment = createPlanFragment(query, num);
    try {
      InputFormat<NullWritable, Text> format = (InputFormat<NullWritable,Text>)(Class.forName("org.apache.hadoop.hive.llap.LlapInputFormat").newInstance());
      int index = 0;
      for (InputSplit s: getSplits(jc, num, fragment.work, fragment.schema)) {
        RecordReader<NullWritable, Text> reader = format.getRecordReader(s,fragment.jc,null);
        Text value = reader.createValue();
        NullWritable key = reader.createKey();
        index++;
        while(reader.next(key,value)) {
          Object[] os = new Object[2];
          os[0] = index;
          os[1] = value.toString();
          forward(os);
        }
      }
    } catch(Exception e) {
      throw new HiveException(e);
    }
  }

  @Override
  public void close() throws HiveException {
  }
}
