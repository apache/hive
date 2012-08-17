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
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ListSinkDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.DelimitedJSONSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * For fetch task with operator tree, row read from FetchOperator is processed via operator tree
 * and finally arrives to this operator.
 */
public class ListSinkOperator extends Operator<ListSinkDesc> {

  private transient SerDe mSerde;

  private transient ArrayList<String> res;
  private transient int numRows;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    try {
      mSerde = initializeSerde(hconf);
      initializeChildren(hconf);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private SerDe initializeSerde(Configuration conf) throws Exception {
    String serdeName = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEFETCHOUTPUTSERDE);
    Class<? extends SerDe> serdeClass = Class.forName(serdeName, true,
        JavaUtils.getClassLoader()).asSubclass(SerDe.class);
    // cast only needed for Hadoop 0.17 compatibility
    SerDe serde = ReflectionUtils.newInstance(serdeClass, null);

    Properties serdeProp = new Properties();

    // this is the default serialization format
    if (serde instanceof DelimitedJSONSerDe) {
      serdeProp.put(Constants.SERIALIZATION_FORMAT, "" + Utilities.tabCode);
      serdeProp.put(Constants.SERIALIZATION_NULL_FORMAT, getConf().getSerializationNullFormat());
    }
    serde.initialize(conf, serdeProp);
    return serde;
  }

  public ListSinkOperator initialize(SerDe mSerde) {
    this.mSerde = mSerde;
    return this;
  }

  public void reset(ArrayList<String> res) {
    this.res = res;
    this.numRows = 0;
  }

  public int getNumRows() {
    return numRows;
  }

  public void processOp(Object row, int tag) throws HiveException {
    try {
      res.add(mSerde.serialize(row, outputObjInspector).toString());
      numRows++;
    } catch (SerDeException e) {
      throw new HiveException(e);
    }
  }

  public OperatorType getType() {
    return OperatorType.FORWARD;
  }
}
