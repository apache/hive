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

import javax.security.auth.login.LoginException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Preconditions;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.hive.llap.LlapOutputFormat;
import org.apache.hadoop.hive.llap.SubmitWorkInfo;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.tez.DagUtils;
import org.apache.hadoop.hive.ql.exec.tez.HiveSplitGenerator;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.TaskSpecBuilder;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.InputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GenericUDFGetSplits.
 *
 */
@Description(name = "get_splits", value = "_FUNC_(string,int) - "
    + "Returns an array of length int serialized splits for the referenced tables string.")
@UDFType(deterministic = false)
public class GenericUDFGetSplits extends GenericUDF {

  private static final Logger LOG = LoggerFactory.getLogger(GenericUDFGetSplits.class);

  private static final String LLAP_INTERNAL_INPUT_FORMAT_NAME = "org.apache.hadoop.hive.llap.LlapInputFormat";

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

    String fetchTaskConversion = HiveConf.getVar(conf, ConfVars.HIVEFETCHTASKCONVERSION);
    String queryResultFormat = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYRESULTFILEFORMAT);

    try {
      LOG.info("setting fetch.task.conversion to none and query file format to \""+LlapOutputFormat.class.getName()+"\"");
      HiveConf.setVar(conf, ConfVars.HIVEFETCHTASKCONVERSION, "none");
      HiveConf.setVar(conf, HiveConf.ConfVars.HIVEQUERYRESULTFILEFORMAT, LlapOutputFormat.class.getName());

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
      }

      MapWork w = (MapWork)tezWork.getAllWork().get(0);

      try {
        for (InputSplit s: getSplits(jc, num, tezWork, schema)) {
          Object[] os = new Object[3];
          os[0] = LLAP_INTERNAL_INPUT_FORMAT_NAME;
          os[1] = s.getClass().getName();
          bos.reset();
          s.write(dos);
          byte[] frozen = bos.toByteArray();
          os[2] = frozen;
          retArray.add(os);
        }
      } catch(Exception e) {
        throw new HiveException(e);
      }
    } finally {
      HiveConf.setVar(conf, ConfVars.HIVEFETCHTASKCONVERSION, fetchTaskConversion);
      HiveConf.setVar(conf, HiveConf.ConfVars.HIVEQUERYRESULTFILEFORMAT, queryResultFormat);
    }
    return retArray;
  }

  public InputSplit[] getSplits(JobConf job, int numSplits, TezWork work, Schema schema) throws IOException {
    DAG dag = DAG.create(work.getName());
    dag.setCredentials(job.getCredentials());
    // TODO: set access control? TezTask.setAccessControlsForCurrentUser(dag);

    DagUtils utils = DagUtils.getInstance();
    Context ctx = new Context(job);
    MapWork mapWork = (MapWork) work.getAllWork().get(0);
    // bunch of things get setup in the context based on conf but we need only the MR tmp directory
    // for the following method.
    JobConf wxConf = utils.initializeVertexConf(job, ctx, mapWork);
    Path scratchDir = utils.createTezDir(ctx.getMRScratchDir(), job);
    FileSystem fs = scratchDir.getFileSystem(job);
    try {
      LocalResource appJarLr = createJarLocalResource(utils.getExecJarPathLocal(), utils, job);
      Vertex wx = utils.createVertex(wxConf, mapWork, scratchDir, appJarLr,
          new ArrayList<LocalResource>(), fs, ctx, false, work,
          work.getVertexType(mapWork));
      String vertexName = wx.getName();
      dag.addVertex(wx);
      utils.addCredentials(mapWork, dag);


      // we have the dag now proceed to get the splits:
      HiveSplitGenerator splitGenerator = new HiveSplitGenerator(null);
      Preconditions.checkState(HiveConf.getBoolVar(wxConf,
          HiveConf.ConfVars.HIVE_TEZ_GENERATE_CONSISTENT_SPLITS));
      Preconditions.checkState(HiveConf.getBoolVar(wxConf,
          HiveConf.ConfVars.LLAP_CLIENT_CONSISTENT_SPLITS));
      splitGenerator.initializeSplitGenerator(wxConf, mapWork);
      List<Event> eventList = splitGenerator.initialize();

      // hack - just serializing with kryo for now. This needs to be done properly
      InputSplit[] result = new InputSplit[eventList.size() - 1];
      DataOutputBuffer dob = new DataOutputBuffer();

      InputConfigureVertexTasksEvent configureEvent = (InputConfigureVertexTasksEvent) eventList.get(0);

      List<TaskLocationHint> hints = configureEvent.getLocationHint().getTaskLocationHints();

      Preconditions.checkState(hints.size() == eventList.size() -1);

      LOG.error("DBG: NumEvents=" + eventList.size());
      LOG.error("DBG: NumSplits=" + result.length);

      ApplicationId fakeApplicationId = ApplicationId.newInstance(Math.abs(new Random().nextInt()), 0);
      TaskSpec taskSpec =
          new TaskSpecBuilder().constructTaskSpec(dag, vertexName, eventList.size() - 1, fakeApplicationId);

      SubmitWorkInfo submitWorkInfo =
          new SubmitWorkInfo(taskSpec, fakeApplicationId, System.currentTimeMillis());
      EventMetaData sourceMetaData =
          new EventMetaData(EventMetaData.EventProducerConsumerType.INPUT, vertexName,
              "NULL_VERTEX", null);
      EventMetaData destinationMetaInfo = new TaskSpecBuilder().getDestingationMetaData(wx);

      LOG.info("DBG: Number of splits: " + (eventList.size() - 1));
      for (int i = 0; i < eventList.size() - 1; i++) {
        // Creating the TezEvent here itself, since it's easy to serialize.
        Event event = eventList.get(i + 1);
        TaskLocationHint hint = hints.get(i);
        Set<String> hosts = hint.getHosts();
        LOG.info("DBG: Using locations: " + hosts.toString());
        if (hosts.size() != 1) {
          LOG.warn("DBG: Bad # of locations: " + hosts.size());
        }
        SplitLocationInfo[] locations = new SplitLocationInfo[hosts.size()];

        int j = 0;
        for (String host : hosts) {
          locations[j++] = new SplitLocationInfo(host, false);
        }
        TezEvent tezEvent = new TezEvent(event, sourceMetaData, System.currentTimeMillis());
        tezEvent.setDestinationInfo(destinationMetaInfo);

        bos.reset();
        dob.reset();
        tezEvent.write(dob);

        byte[] submitWorkBytes = SubmitWorkInfo.toBytes(submitWorkInfo);

        result[i] =
            new LlapInputSplit(i, submitWorkBytes, dob.getData(), locations, schema);
      }
      return result;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

    /**
   * Returns a local resource representing a jar. This resource will be used to execute the plan on
   * the cluster.
   *
   * @param localJarPath
   *          Local path to the jar to be localized.
   * @return LocalResource corresponding to the localized hive exec resource.
   * @throws IOException
   *           when any file system related call fails.
   * @throws LoginException
   *           when we are unable to determine the user.
   * @throws URISyntaxException
   *           when current jar location cannot be determined.
   */
  private LocalResource createJarLocalResource(String localJarPath, DagUtils utils,
      Configuration conf)
    throws IOException, LoginException, IllegalArgumentException, FileNotFoundException {
    FileStatus destDirStatus = utils.getHiveJarDirectory(conf);
    assert destDirStatus != null;
    Path destDirPath = destDirStatus.getPath();

    Path localFile = new Path(localJarPath);
    String sha = getSha(localFile, conf);

    String destFileName = localFile.getName();

    // Now, try to find the file based on SHA and name. Currently we require exact name match.
    // We could also allow cutting off versions and other stuff provided that SHA matches...
    destFileName = FilenameUtils.removeExtension(destFileName) + "-" + sha
      + FilenameUtils.EXTENSION_SEPARATOR + FilenameUtils.getExtension(destFileName);

    // TODO: if this method is ever called on more than one jar, getting the dir and the
    // list need to be refactored out to be done only once.
    Path destFile = new Path(destDirPath.toString() + "/" + destFileName);
    return utils.localizeResource(localFile, destFile, LocalResourceType.FILE, conf);
  }

  private String getSha(Path localFile, Configuration conf)
    throws IOException, IllegalArgumentException {
    InputStream is = null;
    try {
      FileSystem localFs = FileSystem.getLocal(conf);
      is = localFs.open(localFile);
      return DigestUtils.sha256Hex(is);
    } finally {
      if (is != null) {
        is.close();
      }
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    assert children.length == 2;
    return getStandardDisplayString("get_splits", children);
  }
}
