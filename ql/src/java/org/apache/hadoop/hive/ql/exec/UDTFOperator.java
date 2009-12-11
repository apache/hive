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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.udtfDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.generic.UDTFCollector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class UDTFOperator extends Operator<udtfDesc> implements Serializable {
  private static final long serialVersionUID = 1L;

  protected final Log LOG = LogFactory.getLog(this.getClass().getName());

  ObjectInspector [] udtfInputOIs = null;
  Object [] objToSendToUDTF = null;
  Object [] forwardObj = new Object[1];

  /**
   * sends periodic reports back to the tracker.
   */
  transient AutoProgressor autoProgressor;
  transient boolean closeCalled = false;
  
  protected void initializeOp(Configuration hconf) throws HiveException {
    conf.getGenericUDTF().setCollector(new UDTFCollector(this));

    // Make an object inspector [] of the arguments to the UDTF
    List<? extends StructField> inputFields =
      ((StandardStructObjectInspector)inputObjInspectors[0]).getAllStructFieldRefs();

    udtfInputOIs = new ObjectInspector[inputFields.size()];
    for (int i=0; i<inputFields.size(); i++) {
      udtfInputOIs[i] = inputFields.get(i).getFieldObjectInspector();
    }
    objToSendToUDTF = new Object[inputFields.size()];
    StructObjectInspector udtfOutputOI = conf.getGenericUDTF().initialize(udtfInputOIs);

    // Since we're passing the object output by the UDTF directly to the next
    // operator, we can use the same OI.
    outputObjInspector = udtfOutputOI;

    // Set up periodic progress reporting in case the UDTF doesn't output rows
    // for a while
    if (HiveConf.getBoolVar(hconf, HiveConf.ConfVars.HIVEUDTFAUTOPROGRESS)) {
      autoProgressor = new AutoProgressor(this.getClass().getName(), reporter,
          Utilities.getDefaultNotificationInterval(hconf));
      autoProgressor.go();
    }

    // Initialize the rest of the operator DAG
    super.initializeOp(hconf);
  }

  public void processOp(Object row, int tag) throws HiveException {
    // The UDTF expects arguments in an object[]
    StandardStructObjectInspector soi =
      (StandardStructObjectInspector) inputObjInspectors[tag];
    List<? extends StructField> fields = soi.getAllStructFieldRefs();

    for (int i=0; i<fields.size(); i++) {
      objToSendToUDTF[i] = soi.getStructFieldData(row, fields.get(i));
    }

    conf.getGenericUDTF().process(objToSendToUDTF);

  }
  /**
   * forwardUDTFOutput is typically called indirectly by the GenericUDTF when
   * the GenericUDTF has generated output rows that should be passed on to the
   * next operator(s) in the DAG.
   *
   * @param o
   * @throws HiveException
   */
  public void forwardUDTFOutput(Object o) throws HiveException {
    if (closeCalled) {
      throw new HiveException("UDTF's should not output rows on close");
    }
    // Since the output of the UDTF is a struct, we can just forward that
    forward(o, outputObjInspector);
  }

  public String getName() {
    return "UDTF";
  }

  public int getType() {
    return OperatorType.UDTF;
  }

  protected void closeOp(boolean abort) throws HiveException {
    closeCalled = true;
    conf.getGenericUDTF().close();
  }
}
