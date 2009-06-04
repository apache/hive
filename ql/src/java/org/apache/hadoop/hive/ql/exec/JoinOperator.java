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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.joinDesc;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;

/**
 * Join operator implementation.
 */
public class JoinOperator extends CommonJoinOperator<joinDesc> implements Serializable {
  private static final long serialVersionUID = 1L;
  
  @Override
  public void initializeOp(Configuration hconf, Reporter reporter, ObjectInspector[] inputObjInspector) throws HiveException {
    super.initializeOp(hconf, reporter, inputObjInspector);

    ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>(totalSz);

    for (Byte alias : order) {
      int sz = conf.getExprs().get(alias).size();
      StructObjectInspector fldObjIns = (StructObjectInspector)((StructObjectInspector)inputObjInspector[alias.intValue()]).getStructFieldRef("VALUE").getFieldObjectInspector();
      for (int i = 0; i < sz; i++) {
        structFieldObjectInspectors.add(
            ObjectInspectorUtils.getStandardObjectInspector(
                fldObjIns.getAllStructFieldRefs().get(i).getFieldObjectInspector(),
                ObjectInspectorCopyOption.KEEP));
      }
    }

    joinOutputObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(ObjectInspectorUtils
            .getIntegerArray(totalSz), structFieldObjectInspectors);
    LOG.info("JOIN " + ((StructObjectInspector)joinOutputObjectInspector).getTypeName() + " totalsz = " + totalSz);

    initializeChildren(hconf, reporter, new ObjectInspector[]{joinOutputObjectInspector});
  }
}

