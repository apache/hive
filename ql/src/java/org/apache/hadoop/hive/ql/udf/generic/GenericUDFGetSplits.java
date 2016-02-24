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
import java.util.ArrayList;
import java.util.UUID;
import java.io.Serializable;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.DataOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.llap.LlapInputFormat;
import org.apache.hadoop.hive.llap.LlapOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.metastore.api.Schema;


/**
 * GenericUDFGetSplits.
 *
 */
@Description(name = "get_splits", value = "_FUNC_(string,int) - "
    + "Returns an array of length int serialized splits for the referenced tables string.")
@UDFType(deterministic = false)
public class GenericUDFGetSplits extends GenericUDF {

  private static final Logger LOG = LoggerFactory.getLogger(GenericUDFGetSplits.class);

  private transient StringObjectInspector stringOI;
  private transient IntObjectInspector intOI;
  private final ArrayList<Object> retArray = new ArrayList<Object>();
  private transient JobConf jc;
  private transient Hive db;
  private ByteArrayOutputStream bos;
  private DataOutput dos;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
    throws UDFArgumentException {

    LOG.debug("initializing GenericUDFGetSplits");

    try {
      if (SessionState.get() != null && SessionState.get().getConf() != null) {
        HiveConf conf = SessionState.get().getConf();
        jc = new JobConf(conf);
        db = Hive.get(conf);
      } else {
        jc = MapredContext.get().getJobConf();
        db = Hive.get();
      }
    } catch(HiveException e) {
      LOG.error("Failed to initialize: ",e);
      throw new UDFArgumentException(e);
    }

    LOG.debug("Initialized conf, jc and metastore connection");

    if (arguments.length != 2) {
      throw new UDFArgumentLengthException("The function GET_SPLITS accepts 2 arguments.");
    } else if (!(arguments[0] instanceof StringObjectInspector)) {
      LOG.error("Got "+arguments[0].getTypeName()+" instead of string.");
      throw new UDFArgumentTypeException(0, "\""
	  + "string\" is expected at function GET_SPLITS, " + "but \""
	  + arguments[0].getTypeName() + "\" is found");
    } else if (!(arguments[1] instanceof IntObjectInspector)) {
      LOG.error("Got "+arguments[1].getTypeName()+" instead of int.");
      throw new UDFArgumentTypeException(1, "\""
	  + "int\" is expected at function GET_SPLITS, " + "but \""
	  + arguments[1].getTypeName() + "\" is found");
    }

    stringOI = (StringObjectInspector) arguments[0];
    intOI = (IntObjectInspector) arguments[1];

    List<String> names = Arrays.asList("if_class","split_class","split");
    List<ObjectInspector> fieldOIs = Arrays.<ObjectInspector>asList(
								    PrimitiveObjectInspectorFactory.javaStringObjectInspector,
								    PrimitiveObjectInspectorFactory.javaStringObjectInspector,
								    PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector);
    ObjectInspector outputOI = ObjectInspectorFactory.getStandardStructObjectInspector(names, fieldOIs);
    ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(outputOI);
    bos = new ByteArrayOutputStream(1024);
    dos = new DataOutputStream(bos);

    LOG.debug("done initializing GenericUDFGetSplits");
    return listOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    retArray.clear();

    String query = stringOI.getPrimitiveJavaObject(arguments[0].get());

    int num = intOI.get(arguments[1].get());

    Driver driver = new Driver();
    CommandProcessorResponse cpr;

    HiveConf conf = SessionState.get().getConf();

    if (conf == null) {
      throw new HiveException("Need configuration");
    }

    LOG.info("setting fetch.task.conversion to none and query file format to \""+LlapOutputFormat.class.toString()+"\"");
    HiveConf.setVar(conf, ConfVars.HIVEFETCHTASKCONVERSION, "none");
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVEQUERYRESULTFILEFORMAT, LlapOutputFormat.class.toString());

    cpr = driver.compileAndRespond(query);
    if(cpr.getResponseCode() != 0) {
      throw new HiveException("Failed to compile query: "+cpr.getException());
    }

    QueryPlan plan = driver.getPlan();
    List<Task<?>> roots = plan.getRootTasks();
    Schema schema = plan.getResultSchema();

    if (roots == null || roots.size() != 1 || !(roots.get(0) instanceof TezTask)) {
      throw new HiveException("Was expecting a single TezTask.");
    }

    Path data = null;
    InputFormat inp = null;
    String ifc = null;

    TezWork tezWork = ((TezTask)roots.get(0)).getWork();

    if (tezWork.getAllWork().size() != 1) {

      String tableName = "table_"+UUID.randomUUID().toString().replaceAll("[^A-Za-z0-9 ]", "");

      String ctas = "create temporary table "+tableName+" as "+query;
      LOG.info("CTAS: "+ctas);

      try {
        cpr = driver.run(ctas, false);
      } catch(CommandNeedRetryException e) {
        throw new HiveException(e);
      }

      if(cpr.getResponseCode() != 0) {
        throw new HiveException("Failed to create temp table: " + cpr.getException());
      }

      query = "select * from " + tableName;
      cpr = driver.compileAndRespond(query);
      if(cpr.getResponseCode() != 0) {
        throw new HiveException("Failed to create temp table: "+cpr.getException());
      }

      plan = driver.getPlan();
      roots = plan.getRootTasks();
      schema = plan.getResultSchema();

      if (roots == null || roots.size() != 1 || !(roots.get(0) instanceof TezTask)) {
        throw new HiveException("Was expecting a single TezTask.");
      }

      tezWork = ((TezTask)roots.get(0)).getWork();

      // Table table = db.getTable(tableName);
      // if (table.isPartitioned()) {
      //   throw new UDFArgumentException("Table " + tableName + " is partitioned.");
      // }
      // data = table.getDataLocation();
      // LOG.info("looking at: "+data);

      // ifc = table.getInputFormatClass().toString();

      // inp = ReflectionUtils.newInstance(table.getInputFormatClass(), jc);
    }

    MapWork w = (MapWork)tezWork.getAllWork().get(0);
    inp = new LlapInputFormat(tezWork, schema);
    ifc = LlapInputFormat.class.toString();

    try {
      if (inp instanceof JobConfigurable) {
        ((JobConfigurable) inp).configure(jc);
      }

      if (inp instanceof FileInputFormat) {
        ((FileInputFormat) inp).addInputPath(jc, data);
      }

      for (InputSplit s: inp.getSplits(jc, num)) {
        Object[] os = new Object[3];
        os[0] = ifc;
        os[1] = s.getClass().toString();
        bos.reset();
        s.write(dos);
        byte[] frozen = bos.toByteArray();
        os[2] = frozen;
        retArray.add(os);
      }
    } catch(Exception e) {
      throw new HiveException(e);
    }

    return retArray;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert children.length == 2;
    return getStandardDisplayString("get_splits", children);
  }
}
