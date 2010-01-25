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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.LateralViewJoinDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * The lateral view join operator is used to implement the lateral view
 * functionality. This operator was implemented with the following operator DAG
 * in mind. For a query such as
 * 
 * SELECT pageid, adid.* FROM example_table LATERAL VIEW explode(adid_list) AS
 * adid
 * 
 * The top of the operator tree will look similar to
 * 
 * [Table Scan] / \ [Select](*) [Select](adid_list) | | | [UDTF] (explode) \ /
 * [Lateral View Join] | | [Select] (pageid, adid.*) | ....
 * 
 * Rows from the table scan operator are first sent to two select operators. The
 * select operator on the left picks all the columns while the select operator
 * on the right picks only the columns needed by the UDTF.
 * 
 * The output of select in the left branch and output of the UDTF in the right
 * branch are then sent to the lateral view join (LVJ). In most cases, the UDTF
 * will generate > 1 row for every row received from the TS, while the left
 * select operator will generate only one. For each row output from the TS, the
 * LVJ outputs all possible rows that can be created by joining the row from the
 * left select and one of the rows output from the UDTF.
 * 
 * Additional lateral views can be supported by adding a similar DAG after the
 * previous LVJ operator.
 */

public class LateralViewJoinOperator extends Operator<LateralViewJoinDesc> {

  private static final long serialVersionUID = 1L;

  // The expected tags from the parent operators. See processOp() before
  // changing the tags.
  static final int SELECT_TAG = 0;
  static final int UDTF_TAG = 1;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    ArrayList<ObjectInspector> ois = new ArrayList<ObjectInspector>();
    ArrayList<String> fieldNames = conf.getOutputInternalColNames();

    // The output of the lateral view join will be the columns from the select
    // parent, followed by the column from the UDTF parent
    StructObjectInspector soi = (StructObjectInspector) inputObjInspectors[SELECT_TAG];
    List<? extends StructField> sfs = soi.getAllStructFieldRefs();
    for (StructField sf : sfs) {
      ois.add(sf.getFieldObjectInspector());
    }

    soi = (StructObjectInspector) inputObjInspectors[UDTF_TAG];
    sfs = soi.getAllStructFieldRefs();
    for (StructField sf : sfs) {
      ois.add(sf.getFieldObjectInspector());
    }

    outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(fieldNames, ois);

    // Initialize the rest of the operator DAG
    super.initializeOp(hconf);
  }

  // acc is short for accumulator. It's used to build the row before forwarding
  ArrayList<Object> acc = new ArrayList<Object>();
  // selectObjs hold the row from the select op, until receiving a row from
  // the udtf op
  ArrayList<Object> selectObjs = new ArrayList<Object>();

  /**
   * An important assumption for processOp() is that for a given row from the
   * TS, the LVJ will first get the row from the left select operator, followed
   * by all the corresponding rows from the UDTF operator. And so on.
   */
  @Override
  public void processOp(Object row, int tag) throws HiveException {
    StructObjectInspector soi = (StructObjectInspector) inputObjInspectors[tag];
    if (tag == SELECT_TAG) {
      selectObjs.clear();
      selectObjs.addAll(soi.getStructFieldsDataAsList(row));
    } else if (tag == UDTF_TAG) {
      acc.clear();
      acc.addAll(selectObjs);
      acc.addAll(soi.getStructFieldsDataAsList(row));
      forward(acc, outputObjInspector);
    } else {
      throw new HiveException("Invalid tag");
    }

  }

}
