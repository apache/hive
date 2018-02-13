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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.security.auth.login.LoginException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.FieldDesc;
import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.hive.llap.NotTezEventHelper;
import org.apache.hadoop.hive.llap.Schema;
import org.apache.hadoop.hive.llap.SubmitWorkInfo;
import org.apache.hadoop.hive.llap.coordinator.LlapCoordinator;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryIdentifierProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SignableVertexSpec;
import org.apache.hadoop.hive.llap.security.LlapSigner;
import org.apache.hadoop.hive.llap.security.LlapSigner.Signable;
import org.apache.hadoop.hive.llap.security.LlapSigner.SignedMessage;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;
import org.apache.hadoop.hive.llap.security.LlapTokenLocalClient;
import org.apache.hadoop.hive.llap.tez.Converters;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.tez.DagUtils;
import org.apache.hadoop.hive.ql.exec.tez.HiveSplitGenerator;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.TxnManagerFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.RootInputLeafOutput;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.TaskSpecBuilder;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.mapreduce.grouper.TezSplitGrouper;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.InputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * GenericUDTFGetSplits.
 *
 */
@Description(name = "get_splits", value = "_FUNC_(string,int) - "
    + "Returns an array of length int serialized splits for the referenced tables string.")
@UDFType(deterministic = false)
public class GenericUDTFGetSplits extends GenericUDTF {
  private static final Logger LOG = LoggerFactory.getLogger(GenericUDTFGetSplits.class);

  protected transient StringObjectInspector stringOI;
  protected transient IntObjectInspector intOI;
  protected transient JobConf jc;
  private ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
  private DataOutput dos = new DataOutputStream(bos);

  @Override
  public StructObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {

    LOG.debug("initializing GenericUDFGetSplits");

    if (SessionState.get() == null || SessionState.get().getConf() == null) {
      throw new IllegalStateException("Cannot run get splits outside HS2");
    }

    LOG.debug("Initialized conf, jc and metastore connection");

    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
          "The function GET_SPLITS accepts 2 arguments.");
    } else if (!(arguments[0] instanceof StringObjectInspector)) {
      LOG.error("Got " + arguments[0].getTypeName() + " instead of string.");
      throw new UDFArgumentTypeException(0, "\""
          + "string\" is expected at function GET_SPLITS, " + "but \""
          + arguments[0].getTypeName() + "\" is found");
    } else if (!(arguments[1] instanceof IntObjectInspector)) {
      LOG.error("Got " + arguments[1].getTypeName() + " instead of int.");
      throw new UDFArgumentTypeException(1, "\""
          + "int\" is expected at function GET_SPLITS, " + "but \""
          + arguments[1].getTypeName() + "\" is found");
    }

    stringOI = (StringObjectInspector) arguments[0];
    intOI = (IntObjectInspector) arguments[1];

    List<String> names = Arrays.asList("split");
    List<ObjectInspector> fieldOIs = Arrays
        .<ObjectInspector> asList(PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector);
    StructObjectInspector outputOI = ObjectInspectorFactory
        .getStandardStructObjectInspector(names, fieldOIs);

    LOG.debug("done initializing GenericUDFGetSplits");
    return outputOI;
  }

  public static class PlanFragment {
    public JobConf jc;
    public TezWork work;
    public Schema schema;

    public PlanFragment(TezWork work, Schema schema, JobConf jc) {
      this.work = work;
      this.schema = schema;
      this.jc = jc;
    }
  }

  @Override
  public void process(Object[] arguments) throws HiveException {

    String query = stringOI.getPrimitiveJavaObject(arguments[0]);
    int num = intOI.get(arguments[1]);

    // Generate applicationId for the LLAP splits
    LlapCoordinator coordinator = LlapCoordinator.getInstance();
    if (coordinator == null) {
      throw new HiveException("LLAP coordinator is not initialized; must be running in HS2 with "
          + ConfVars.LLAP_HS2_ENABLE_COORDINATOR.varname + " enabled");
    }
    ApplicationId applicationId = coordinator.createExtClientAppId();
    LOG.info("Generated appID {} for LLAP splits", applicationId.toString());

    PlanFragment fragment = createPlanFragment(query, num, applicationId);
    TezWork tezWork = fragment.work;
    Schema schema = fragment.schema;

    try {
      for (InputSplit s : getSplits(jc, num, tezWork, schema, applicationId)) {
        Object[] os = new Object[1];
        bos.reset();
        s.write(dos);
        byte[] frozen = bos.toByteArray();
        os[0] = frozen;
        forward(os);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public PlanFragment createPlanFragment(String query, int num, ApplicationId splitsAppId)
      throws HiveException {

    HiveConf conf = new HiveConf(SessionState.get().getConf());
    HiveConf.setVar(conf, ConfVars.HIVEFETCHTASKCONVERSION, "none");
    HiveConf.setVar(conf, ConfVars.HIVEQUERYRESULTFILEFORMAT, PlanUtils.LLAP_OUTPUT_FORMAT_KEY);

    String originalMode = HiveConf.getVar(conf,
        ConfVars.HIVE_EXECUTION_MODE);
    HiveConf.setVar(conf, ConfVars.HIVE_EXECUTION_MODE, "llap");
    HiveConf.setBoolVar(conf, ConfVars.HIVE_TEZ_GENERATE_CONSISTENT_SPLITS, true);
    HiveConf.setBoolVar(conf, ConfVars.LLAP_CLIENT_CONSISTENT_SPLITS, true);
    conf.setBoolean(TezSplitGrouper.TEZ_GROUPING_NODE_LOCAL_ONLY, true);
    // Tez/LLAP requires RPC query plan
    HiveConf.setBoolVar(conf, ConfVars.HIVE_RPC_QUERY_PLAN, true);

    try {
      jc = DagUtils.getInstance().createConfiguration(conf);
    } catch (IOException e) {
      throw new HiveException(e);
    }

    // Instantiate Driver to compile the query passed in.
    // This UDF is running as part of an existing query, which may already be using the
    // SessionState TxnManager. If this new Driver also tries to use the same TxnManager
    // then this may mess up the existing state of the TxnManager.
    // So initialize the new Driver with a new TxnManager so that it does not use the
    // Session TxnManager that is already in use.
    HiveTxnManager txnManager = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    Driver driver = new Driver(new QueryState.Builder().withHiveConf(conf).nonIsolated().build(), null, null, txnManager);
    DriverCleanup driverCleanup = new DriverCleanup(driver, txnManager, splitsAppId.toString());
    boolean needsCleanup = true;
    try {
      CommandProcessorResponse cpr = driver.compileAndRespond(query);
      if (cpr.getResponseCode() != 0) {
        throw new HiveException("Failed to compile query: " + cpr.getException());
      }

      QueryPlan plan = driver.getPlan();
      List<Task<?>> roots = plan.getRootTasks();
      Schema schema = convertSchema(plan.getResultSchema());

      if (roots == null || roots.size() != 1 || !(roots.get(0) instanceof TezTask)) {
        throw new HiveException("Was expecting a single TezTask.");
      }

      TezWork tezWork = ((TezTask)roots.get(0)).getWork();

      if (tezWork.getAllWork().size() != 1) {

        String tableName = "table_"+UUID.randomUUID().toString().replaceAll("[^A-Za-z0-9 ]", "");

        String ctas = "create temporary table " + tableName + " as " + query;
        LOG.info("Materializing the query for LLAPIF; CTAS: " + ctas);
        driver.releaseResources();
        HiveConf.setVar(conf, ConfVars.HIVE_EXECUTION_MODE, originalMode);
        cpr = driver.run(ctas, false);

        if(cpr.getResponseCode() != 0) {
          throw new HiveException("Failed to create temp table: " + cpr.getException());
        }

        HiveConf.setVar(conf, ConfVars.HIVE_EXECUTION_MODE, "llap");
        query = "select * from " + tableName;
        cpr = driver.compileAndRespond(query);
        if(cpr.getResponseCode() != 0) {
          throw new HiveException("Failed to create temp table: "+cpr.getException());
        }

        plan = driver.getPlan();
        roots = plan.getRootTasks();
        schema = convertSchema(plan.getResultSchema());

        if (roots == null || roots.size() != 1 || !(roots.get(0) instanceof TezTask)) {
          throw new HiveException("Was expecting a single TezTask.");
        }

        tezWork = ((TezTask)roots.get(0)).getWork();
      } else {
        // Table will be queried directly by LLAP
        // Acquire locks if necessary - they will be released during session cleanup.
        // The read will have READ_COMMITTED level semantics.
        try {
          driver.lockAndRespond();
        } catch (CommandProcessorResponse cpr1) {
          throw new HiveException("Failed to acquire locks", cpr1);
        }

        // Attach the resources to the session cleanup.
        SessionState.get().addCleanupItem(driverCleanup);
        needsCleanup = false;
      }

      return new PlanFragment(tezWork, schema, jc);
    } finally {
      if (needsCleanup) {
        if (driverCleanup != null) {
          try {
            driverCleanup.close();
          } catch (IOException err) {
            throw new HiveException(err);
          }
        } else if (driver != null) {
          driver.close();
          driver.destroy();
        }
      }
    }
  }

  public InputSplit[] getSplits(JobConf job, int numSplits, TezWork work, Schema schema, ApplicationId applicationId)
    throws IOException {

    DAG dag = DAG.create(work.getName());
    dag.setCredentials(job.getCredentials());

    DagUtils utils = DagUtils.getInstance();
    Context ctx = new Context(job);
    MapWork mapWork = (MapWork) work.getAllWork().get(0);
    // bunch of things get setup in the context based on conf but we need only the MR tmp directory
    // for the following method.
    JobConf wxConf = utils.initializeVertexConf(job, ctx, mapWork);
    // TODO: should we also whitelist input formats here? from mapred.input.format.class
    Path scratchDir = utils.createTezDir(ctx.getMRScratchDir(), job);
    FileSystem fs = scratchDir.getFileSystem(job);
    try {
      LocalResource appJarLr = createJarLocalResource(utils.getExecJarPathLocal(ctx.getConf()), utils, job);

      LlapCoordinator coordinator = LlapCoordinator.getInstance();
      if (coordinator == null) {
        throw new IOException("LLAP coordinator is not initialized; must be running in HS2 with "
            + ConfVars.LLAP_HS2_ENABLE_COORDINATOR.varname + " enabled");
      }

      // Update the queryId to use the generated applicationId. See comment below about
      // why this is done.
      HiveConf.setVar(wxConf, HiveConf.ConfVars.HIVEQUERYID, applicationId.toString());
      Vertex wx = utils.createVertex(wxConf, mapWork, scratchDir, fs, ctx, false, work,
          work.getVertexType(mapWork), DagUtils.createTezLrMap(appJarLr, null));
      String vertexName = wx.getName();
      dag.addVertex(wx);
      utils.addCredentials(mapWork, dag);


      // we have the dag now proceed to get the splits:
      Preconditions.checkState(HiveConf.getBoolVar(wxConf,
              ConfVars.HIVE_TEZ_GENERATE_CONSISTENT_SPLITS));
      Preconditions.checkState(HiveConf.getBoolVar(wxConf,
              ConfVars.LLAP_CLIENT_CONSISTENT_SPLITS));


      HiveSplitGenerator splitGenerator = new HiveSplitGenerator(wxConf, mapWork);
      List<Event> eventList = splitGenerator.initialize();

      InputSplit[] result = new InputSplit[eventList.size() - 1];

      InputConfigureVertexTasksEvent configureEvent
        = (InputConfigureVertexTasksEvent) eventList.get(0);

      List<TaskLocationHint> hints = configureEvent.getLocationHint().getTaskLocationHints();

      Preconditions.checkState(hints.size() == eventList.size() - 1);

      if (LOG.isDebugEnabled()) {
        LOG.debug("NumEvents=" + eventList.size() + ", NumSplits=" + result.length);
      }

      // This assumes LLAP cluster owner is always the HS2 user.
      String llapUser = UserGroupInformation.getLoginUser().getShortUserName();

      String queryUser = null;
      byte[] tokenBytes = null;
      LlapSigner signer = null;
      if (UserGroupInformation.isSecurityEnabled()) {
        signer = coordinator.getLlapSigner(job);

        // 1. Generate the token for query user (applies to all splits).
        queryUser = SessionState.getUserFromAuthenticator();
        if (queryUser == null) {
          queryUser = UserGroupInformation.getCurrentUser().getUserName();
          LOG.warn("Cannot determine the session user; using " + queryUser + " instead");
        }
        LlapTokenLocalClient tokenClient = coordinator.getLocalTokenClient(job, llapUser);
        // We put the query user, not LLAP user, into the message and token.
        Token<LlapTokenIdentifier> token = tokenClient.createToken(
            applicationId.toString(), queryUser, true);
        LOG.info("Created the token for remote user: {}", token);
        bos.reset();
        token.write(dos);
        tokenBytes = bos.toByteArray();
      } else {
        queryUser = UserGroupInformation.getCurrentUser().getUserName();
      }

      // Generate umbilical token (applies to all splits)
      Token<JobTokenIdentifier> umbilicalToken = JobTokenCreator.createJobToken(applicationId);

      LOG.info("Number of splits: " + (eventList.size() - 1));
      SignedMessage signedSvs = null;
      for (int i = 0; i < eventList.size() - 1; i++) {
        TaskSpec taskSpec = new TaskSpecBuilder().constructTaskSpec(dag, vertexName,
              eventList.size() - 1, applicationId, i);

        // 2. Generate the vertex/submit information for all events.
        if (i == 0) {
          // The queryId could either be picked up from the current request being processed, or
          // generated. The current request isn't exactly correct since the query is 'done' once we
          // return the results. Generating a new one has the added benefit of working once this
          // is moved out of a UDTF into a proper API.
          // Setting this to the generated AppId which is unique.
          // Despite the differences in TaskSpec, the vertex spec should be the same.
          signedSvs = createSignedVertexSpec(signer, taskSpec, applicationId, queryUser,
              applicationId.toString());
        }

        SubmitWorkInfo submitWorkInfo = new SubmitWorkInfo(applicationId,
            System.currentTimeMillis(), taskSpec.getVertexParallelism(), signedSvs.message,
            signedSvs.signature, umbilicalToken);
        byte[] submitWorkBytes = SubmitWorkInfo.toBytes(submitWorkInfo);

        // 3. Generate input event.
        SignedMessage eventBytes = makeEventBytes(wx, vertexName, eventList.get(i + 1), signer);

        // 4. Make location hints.
        SplitLocationInfo[] locations = makeLocationHints(hints.get(i));

        result[i] = new LlapInputSplit(i, submitWorkBytes, eventBytes.message,
            eventBytes.signature, locations, schema, llapUser, tokenBytes);
       }
      return result;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private static class DriverCleanup implements Closeable {
    private final Driver driver;
    private final HiveTxnManager txnManager;
    private final String applicationId;

    public DriverCleanup(Driver driver, HiveTxnManager txnManager, String applicationId) {
      this.driver = driver;
      this.txnManager = txnManager;
      this.applicationId = applicationId;
    }

    @Override
    public void close() throws IOException {
      try {
        LOG.info("DriverCleanup for LLAP splits: {}", applicationId);
        driver.releaseLocksAndCommitOrRollback(true);
        driver.close();
        driver.destroy();
        txnManager.closeTxnManager();
      } catch (Exception err) {
        LOG.error("Error closing driver resources", err);
        throw new IOException(err);
      }
    }

    @Override
    public String toString() {
      return "DriverCleanup for LLAP splits: " + applicationId;
    }
  }

  private static class JobTokenCreator {
    private static Token<JobTokenIdentifier> createJobToken(ApplicationId applicationId) {
      String tokenIdentifier = applicationId.toString();
      JobTokenIdentifier identifier = new JobTokenIdentifier(new Text(
          tokenIdentifier));
      Token<JobTokenIdentifier> sessionToken = new Token<JobTokenIdentifier>(identifier,
          new JobTokenSecretManager());
      sessionToken.setService(identifier.getJobId());
      return sessionToken;
    }
  }

  private SplitLocationInfo[] makeLocationHints(TaskLocationHint hint) {
    Set<String> hosts = hint.getHosts();
    if (hosts.size() != 1) {
      LOG.warn("Bad # of locations: " + hosts.size());
    }
    SplitLocationInfo[] locations = new SplitLocationInfo[hosts.size()];
    int j = 0;
    for (String host : hosts) {
      locations[j++] = new SplitLocationInfo(host, false);
    }
    return locations;
  }

  private SignedMessage makeEventBytes(Vertex wx, String vertexName,
      Event event, LlapSigner signer) throws IOException {
    assert event instanceof InputDataInformationEvent;
    List<RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> inputs =
        TaskSpecBuilder.getVertexInputs(wx);
    Preconditions.checkState(inputs.size() == 1);

    Signable signableNte = NotTezEventHelper.createSignableNotTezEvent(
        (InputDataInformationEvent)event, vertexName, inputs.get(0).getName());
    if (signer != null) {
      return signer.serializeAndSign(signableNte);
    } else {
      SignedMessage sm = new SignedMessage();
      sm.message = signableNte.serialize();
      return sm;
    }
  }

  private SignedMessage createSignedVertexSpec(LlapSigner signer, TaskSpec taskSpec,
      ApplicationId applicationId, String queryUser, String queryIdString) throws IOException {
    QueryIdentifierProto queryIdentifierProto =
        QueryIdentifierProto.newBuilder().setApplicationIdString(applicationId.toString())
            .setDagIndex(taskSpec.getDagIdentifier()).setAppAttemptNumber(0).build();
    final SignableVertexSpec.Builder svsb = Converters.constructSignableVertexSpec(
        taskSpec, queryIdentifierProto, applicationId.toString(), queryUser, queryIdString);
    svsb.setIsExternalSubmission(true);
    if (signer == null) {
      SignedMessage result = new SignedMessage();
      result.message = serializeVertexSpec(svsb);
      return result;
    }
    return signer.serializeAndSign(new Signable() {
      @Override
      public void setSignInfo(int masterKeyId) {
        svsb.setSignatureKeyId(masterKeyId);
      }

      @Override
      public byte[] serialize() throws IOException {
        return serializeVertexSpec(svsb);
      }
    });
  }

  private static byte[] serializeVertexSpec(SignableVertexSpec.Builder svsb) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    svsb.build().writeTo(os);
    return os.toByteArray();
  }

  /**
   * Returns a local resource representing a jar. This resource will be used to
   * execute the plan on the cluster.
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
  private LocalResource createJarLocalResource(String localJarPath,
      DagUtils utils, Configuration conf) throws IOException, LoginException,
      IllegalArgumentException, FileNotFoundException {
    FileStatus destDirStatus = utils.getHiveJarDirectory(conf);
    assert destDirStatus != null;
    Path destDirPath = destDirStatus.getPath();

    Path localFile = new Path(localJarPath);
    String sha = getSha(localFile, conf);

    String destFileName = localFile.getName();

    // Now, try to find the file based on SHA and name. Currently we require
    // exact name match.
    // We could also allow cutting off versions and other stuff provided that
    // SHA matches...
    destFileName = FilenameUtils.removeExtension(destFileName) + "-" + sha
        + FilenameUtils.EXTENSION_SEPARATOR
        + FilenameUtils.getExtension(destFileName);

    // TODO: if this method is ever called on more than one jar, getting the dir
    // and the
    // list need to be refactored out to be done only once.
    Path destFile = new Path(destDirPath.toString() + "/" + destFileName);
    return utils.localizeResource(localFile, destFile, LocalResourceType.FILE,
        conf);
  }

  private String getSha(Path localFile, Configuration conf) throws IOException,
      IllegalArgumentException {
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

  private Schema convertSchema(Object obj) throws HiveException {
    org.apache.hadoop.hive.metastore.api.Schema schema = (org.apache.hadoop.hive.metastore.api.Schema) obj;
    List<FieldDesc> colDescs = new ArrayList<FieldDesc>();
    for (FieldSchema fs : schema.getFieldSchemas()) {
      String colName = fs.getName();
      String typeString = fs.getType();
      colDescs.add(new FieldDesc(colName, TypeInfoUtils.getTypeInfoFromTypeString(typeString)));
    }
    Schema Schema = new Schema(colDescs);
    return Schema;
  }

  @Override
  public void close() throws HiveException {
  }
}
