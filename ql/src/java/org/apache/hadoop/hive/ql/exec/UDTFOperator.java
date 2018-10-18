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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.udf.generic.UDTFCollector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UDTFOperator.
 *
 */
public class UDTFOperator extends Operator<UDTFDesc> implements Serializable {
  private static final long serialVersionUID = 1L;

  protected static final Logger LOG = LoggerFactory.getLogger(UDTFOperator.class.getName());

  StructObjectInspector udtfInputOI = null;
  Object[] objToSendToUDTF = null;

  GenericUDTF genericUDTF;
  UDTFCollector collector;
  List outerObj;

  /**
   * sends periodic reports back to the tracker.
   */
  transient AutoProgressor autoProgressor;

  /** Kryo ctor. */
  protected UDTFOperator() {
    super();
  }

  public UDTFOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    genericUDTF = conf.getGenericUDTF();
    collector = new UDTFCollector(this);

    genericUDTF.setCollector(collector);

    udtfInputOI = (StructObjectInspector) inputObjInspectors[0];

    objToSendToUDTF = new Object[udtfInputOI.getAllStructFieldRefs().size()];

    MapredContext context = MapredContext.get();
    if (context != null) {
      context.setup(genericUDTF);
    }
    StructObjectInspector udtfOutputOI = genericUDTF.initialize(udtfInputOI);

    if (conf.isOuterLV()) {
      outerObj = Arrays.asList(new Object[udtfOutputOI.getAllStructFieldRefs().size()]);
    }

    // Since we're passing the object output by the UDTF directly to the next
    // operator, we can use the same OI.
    outputObjInspector = udtfOutputOI;

    // Set up periodic progress reporting in case the UDTF doesn't output rows
    // for a while
    if (HiveConf.getBoolVar(hconf, HiveConf.ConfVars.HIVEUDTFAUTOPROGRESS)) {
      autoProgressor = new AutoProgressor(this.getClass().getName(), reporter,
          Utilities.getDefaultNotificationInterval(hconf),
          HiveConf.getTimeVar(
              hconf, HiveConf.ConfVars.HIVES_AUTO_PROGRESS_TIMEOUT, TimeUnit.MILLISECONDS));
      autoProgressor.go();
    }
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    // The UDTF expects arguments in an object[]
    StructObjectInspector soi = (StructObjectInspector) inputObjInspectors[tag];
    List<? extends StructField> fields = soi.getAllStructFieldRefs();

    for (int i = 0; i < fields.size(); i++) {
      objToSendToUDTF[i] = soi.getStructFieldData(row, fields.get(i));
    }

    genericUDTF.process(objToSendToUDTF);
    if (conf.isOuterLV() && collector.getCounter() == 0) {
      collector.collect(outerObj);
    }
    collector.reset();
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
    // Since the output of the UDTF is a struct, we can just forward that
    forward(o, outputObjInspector);
  }

  @Override
  public String getName() {
    return UDTFOperator.getOperatorName();
  }

  static public String getOperatorName() {
    return "UDTF";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.UDTF;
  }

  @Override
  protected void closeOp(boolean abort) throws HiveException {
    conf.getGenericUDTF().close();
    super.closeOp(abort);
  }
}
