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

package org.apache.hadoop.hive.ql.exec.tez;

import java.io.Serializable;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.UnionWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.tez.client.CallerContext;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.common.security.DAGAccessControls;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.GroupInputEdge;
import org.apache.tez.dag.api.SessionNotRunning;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexGroup;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.json.JSONObject;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.TezJobMonitor;

/**
 *
 * TezTask handles the execution of TezWork. Currently it executes a graph of map and reduce work
 * using the Tez APIs directly.
 *
 */
@SuppressWarnings({"serial"})
public class TezTask extends Task<TezWork> {

  private static final String CLASS_NAME = TezTask.class.getName();
  private final PerfLogger perfLogger = SessionState.getPerfLogger();
  private static final String TEZ_MEMORY_RESERVE_FRACTION = "tez.task.scale.memory.reserve-fraction";

  private TezCounters counters;

  private final DagUtils utils;

  private DAGClient dagClient = null;

  Map<BaseWork, Vertex> workToVertex = new HashMap<BaseWork, Vertex>();
  Map<BaseWork, JobConf> workToConf = new HashMap<BaseWork, JobConf>();

  public TezTask() {
    this(DagUtils.getInstance());
  }

  public TezTask(DagUtils utils) {
    super();
    this.utils = utils;
  }

  public TezCounters getTezCounters() {
    return counters;
  }

  @Override
  public int execute(DriverContext driverContext) {
    int rc = 1;
    boolean cleanContext = false;
    Context ctx = null;
    TezSessionState session = null;

    try {
      // Get or create Context object. If we create it we have to clean it later as well.
      ctx = driverContext.getCtx();
      if (ctx == null) {
        ctx = new Context(conf);
        cleanContext = true;
      }

      // Need to remove this static hack. But this is the way currently to get a session.
      SessionState ss = SessionState.get();
      session = ss.getTezSession();
      if (session != null && !session.isOpen()) {
        LOG.warn("The session: " + session + " has not been opened");
      }
      session = TezSessionPoolManager.getInstance().getSession(
          session, conf, false, getWork().getLlapMode());
      ss.setTezSession(session);
      try {
        // jobConf will hold all the configuration for hadoop, tez, and hive
        JobConf jobConf = utils.createConfiguration(conf);

        // Get all user jars from work (e.g. input format stuff).
        String[] inputOutputJars = work.configureJobConfAndExtractJars(jobConf);

        // we will localize all the files (jars, plans, hashtables) to the
        // scratch dir. let's create this and tmp first.
        Path scratchDir = ctx.getMRScratchDir();

        // create the tez tmp dir
        scratchDir = utils.createTezDir(scratchDir, conf);

        Map<String,LocalResource> inputOutputLocalResources =
            getExtraLocalResources(jobConf, scratchDir, inputOutputJars);

        // Ensure the session is open and has the necessary local resources
        updateSession(session, jobConf, scratchDir, inputOutputJars, inputOutputLocalResources);

        List<LocalResource> additionalLr = session.getLocalizedResources();
        logResources(additionalLr);

        // unless already installed on all the cluster nodes, we'll have to
        // localize hive-exec.jar as well.
        LocalResource appJarLr = session.getAppJarLr();

        // next we translate the TezWork to a Tez DAG
        DAG dag = build(jobConf, work, scratchDir, appJarLr, additionalLr, ctx);
        CallerContext callerContext = CallerContext.create(
            "HIVE", queryPlan.getQueryId(),
            "HIVE_QUERY_ID", queryPlan.getQueryStr());
        dag.setCallerContext(callerContext);

        // Add the extra resources to the dag
        addExtraResourcesToDag(session, dag, inputOutputJars, inputOutputLocalResources);

        // submit will send the job to the cluster and start executing
        dagClient = submit(jobConf, dag, scratchDir, appJarLr, session,
            additionalLr, inputOutputJars, inputOutputLocalResources);

        // finally monitor will print progress until the job is done
        TezJobMonitor monitor = new TezJobMonitor(work.getWorkMap(),dagClient, conf, dag, ctx);
        rc = monitor.monitorExecution();

        if (rc != 0) {
          this.setException(new HiveException(monitor.getDiagnostics()));
        }

        // fetch the counters
        try {
          Set<StatusGetOpts> statusGetOpts = EnumSet.of(StatusGetOpts.GET_COUNTERS);
          counters = dagClient.getDAGStatus(statusGetOpts).getDAGCounters();
        } catch (Exception err) {
          // Don't fail execution due to counters - just don't print summary info
          LOG.warn("Failed to get counters. Ignoring, summary info will be incomplete. " + err, err);
          counters = null;
        }
      } finally {
        // We return this to the pool even if it's unusable; reopen is supposed to handle this.
        try {
          TezSessionPoolManager.getInstance()
              .returnSession(session, getWork().getLlapMode());
        } catch (Exception e) {
          LOG.error("Failed to return session: {} to pool", session, e);
          throw e;
        }
      }

      if (LOG.isInfoEnabled() && counters != null
          && (HiveConf.getBoolVar(conf, HiveConf.ConfVars.TEZ_EXEC_SUMMARY) ||
          Utilities.isPerfOrAboveLogging(conf))) {
        for (CounterGroup group: counters) {
          LOG.info(group.getDisplayName() +":");
          for (TezCounter counter: group) {
            LOG.info("   "+counter.getDisplayName()+": "+counter.getValue());
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to execute tez graph.", e);
      // rc will be 1 at this point indicating failure.
    } finally {
      Utilities.clearWork(conf);

      // Clear gWorkMap
      for (BaseWork w : work.getAllWork()) {
        JobConf workCfg = workToConf.get(w);
        if (workCfg != null) {
          Utilities.clearWorkMapForConf(workCfg);
        }
      }

      if (cleanContext) {
        try {
          ctx.clear();
        } catch (Exception e) {
          /*best effort*/
          LOG.warn("Failed to clean up after tez job", e);
        }
      }
      // need to either move tmp files or remove them
      if (dagClient != null) {
        // rc will only be overwritten if close errors out
        rc = close(work, rc);
      }
    }
    return rc;
  }

  private void logResources(List<LocalResource> additionalLr) {
    // log which resources we're adding (apart from the hive exec)
    if (!LOG.isDebugEnabled()) return;
    if (additionalLr == null || additionalLr.size() == 0) {
      LOG.debug("No local resources to process (other than hive-exec)");
    } else {
      for (LocalResource lr: additionalLr) {
        LOG.debug("Adding local resource: " + lr.getResource());
      }
    }
  }

  /**
   * Converted the list of jars into local resources
   */
  Map<String,LocalResource> getExtraLocalResources(JobConf jobConf, Path scratchDir,
      String[] inputOutputJars) throws Exception {
    final Map<String,LocalResource> resources = new HashMap<String,LocalResource>();
    final List<LocalResource> localResources = utils.localizeTempFiles(
        scratchDir.toString(), jobConf, inputOutputJars);
    if (null != localResources) {
      for (LocalResource lr : localResources) {
        resources.put(utils.getBaseName(lr), lr);
      }
    }
    return resources;
  }

  /**
   * Ensures that the Tez Session is open and the AM has all necessary jars configured.
   */
  void updateSession(TezSessionState session,
      JobConf jobConf, Path scratchDir, String[] inputOutputJars,
      Map<String,LocalResource> extraResources) throws Exception {
    final boolean missingLocalResources = !session
        .hasResources(inputOutputJars);

    TezClient client = session.getSession();
    // TODO null can also mean that this operation was interrupted. Should we really try to re-create the session in that case ?
    if (client == null) {
      // can happen if the user sets the tez flag after the session was
      // established
      LOG.info("Tez session hasn't been created yet. Opening session");
      session.open(conf, inputOutputJars);
    } else {
      LOG.info("Session is already open");

      // Ensure the open session has the necessary resources (StorageHandler)
      if (missingLocalResources) {
        LOG.info("Tez session missing resources," +
            " adding additional necessary resources");
        client.addAppMasterLocalFiles(extraResources);
      }

      session.refreshLocalResourcesFromConf(conf);
    }
  }

  /**
   * Adds any necessary resources that must be localized in each vertex to the DAG.
   */
  void addExtraResourcesToDag(TezSessionState session, DAG dag,
      String[] inputOutputJars,
      Map<String,LocalResource> inputOutputLocalResources) throws Exception {
    if (!session.hasResources(inputOutputJars)) {
      if (null != inputOutputLocalResources) {
        dag.addTaskLocalFiles(inputOutputLocalResources);
      }
    }
  }

  void checkOutputSpec(BaseWork work, JobConf jc) throws IOException {
    for (Operator<?> op : work.getAllOperators()) {
      if (op instanceof FileSinkOperator) {
        ((FileSinkOperator) op).checkOutputSpecs(null, jc);
      }
    }
  }

  DAG build(JobConf conf, TezWork work, Path scratchDir,
      LocalResource appJarLr, List<LocalResource> additionalLr, Context ctx)
      throws Exception {

    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_BUILD_DAG);

    // getAllWork returns a topologically sorted list, which we use to make
    // sure that vertices are created before they are used in edges.
    List<BaseWork> ws = work.getAllWork();
    Collections.reverse(ws);

    FileSystem fs = scratchDir.getFileSystem(conf);

    // the name of the dag is what is displayed in the AM/Job UI
    String dagName = utils.createDagName(conf, queryPlan);

    LOG.info("Dag name: " + dagName);
    DAG dag = DAG.create(dagName);

    // set some info for the query
    JSONObject json = new JSONObject(new LinkedHashMap()).put("context", "Hive")
        .put("description", ctx.getCmd());
    String dagInfo = json.toString();

    if (LOG.isDebugEnabled()) {
      LOG.debug("DagInfo: " + dagInfo);
    }
    dag.setDAGInfo(dagInfo);

    dag.setCredentials(conf.getCredentials());
    setAccessControlsForCurrentUser(dag, queryPlan.getQueryId(), conf);

    for (BaseWork w: ws) {
      boolean isFinal = work.getLeaves().contains(w);

      // translate work to vertex
      perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_CREATE_VERTEX + w.getName());

      if (w instanceof UnionWork) {
        // Special case for unions. These items translate to VertexGroups

        List<BaseWork> unionWorkItems = new LinkedList<BaseWork>();
        List<BaseWork> children = new LinkedList<BaseWork>();

        // split the children into vertices that make up the union and vertices that are
        // proper children of the union
        for (BaseWork v: work.getChildren(w)) {
          EdgeType type = work.getEdgeProperty(w, v).getEdgeType();
          if (type == EdgeType.CONTAINS) {
            unionWorkItems.add(v);
          } else {
            children.add(v);
          }
        }
        JobConf parentConf = workToConf.get(unionWorkItems.get(0));
        checkOutputSpec(w, parentConf);

        // create VertexGroup
        Vertex[] vertexArray = new Vertex[unionWorkItems.size()];

        int i = 0;
        for (BaseWork v: unionWorkItems) {
          vertexArray[i++] = workToVertex.get(v);
        }
        VertexGroup group = dag.createVertexGroup(w.getName(), vertexArray);

        // For a vertex group, all Outputs use the same Key-class, Val-class and partitioner.
        // Pick any one source vertex to figure out the Edge configuration.
       

        // now hook up the children
        for (BaseWork v: children) {
          // finally we can create the grouped edge
          GroupInputEdge e = utils.createEdge(group, parentConf,
               workToVertex.get(v), work.getEdgeProperty(w, v), work.getVertexType(v));

          dag.addEdge(e);
        }
      } else {
        // Regular vertices
        JobConf wxConf = utils.initializeVertexConf(conf, ctx, w);
        checkOutputSpec(w, wxConf);
        Vertex wx =
            utils.createVertex(wxConf, w, scratchDir, appJarLr, additionalLr, fs, ctx, !isFinal,
                work, work.getVertexType(w));
        if (w.getReservedMemoryMB() > 0) {
          // If reversedMemoryMB is set, make memory allocation fraction adjustment as needed
          double frac = DagUtils.adjustMemoryReserveFraction(w.getReservedMemoryMB(), super.conf);
          LOG.info("Setting " + TEZ_MEMORY_RESERVE_FRACTION + " to " + frac);
          wx.setConf(TEZ_MEMORY_RESERVE_FRACTION, Double.toString(frac));
        } // Otherwise just leave it up to Tez to decide how much memory to allocate
        dag.addVertex(wx);
        utils.addCredentials(w, dag);
        perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_CREATE_VERTEX + w.getName());
        workToVertex.put(w, wx);
        workToConf.put(w, wxConf);

        // add all dependencies (i.e.: edges) to the graph
        for (BaseWork v: work.getChildren(w)) {
          assert workToVertex.containsKey(v);
          Edge e = null;

          TezEdgeProperty edgeProp = work.getEdgeProperty(w, v);

          e = utils.createEdge(wxConf, wx, workToVertex.get(v), edgeProp, work.getVertexType(v));
          dag.addEdge(e);
        }
      }
    }
    // Clear the work map after build. TODO: remove caching instead?
    Utilities.clearWorkMap(conf);
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_BUILD_DAG);
    return dag;
  }

  private static void setAccessControlsForCurrentUser(DAG dag, String queryId,
                                                     Configuration conf) throws
      IOException {
    String user = SessionState.getUserFromAuthenticator();
    UserGroupInformation loginUserUgi = UserGroupInformation.getLoginUser();
    String loginUser =
        loginUserUgi == null ? null : loginUserUgi.getShortUserName();
    boolean addHs2User =
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVETEZHS2USERACCESS);

    // Temporarily re-using the TEZ AM View ACLs property for individual dag access control.
    // Hive may want to setup it's own parameters if it wants to control per dag access.
    // Setting the tez-property per dag should work for now.

    String viewStr = Utilities.getAclStringWithHiveModification(conf,
            TezConfiguration.TEZ_AM_VIEW_ACLS, addHs2User, user, loginUser);
    String modifyStr = Utilities.getAclStringWithHiveModification(conf,
            TezConfiguration.TEZ_AM_MODIFY_ACLS, addHs2User, user, loginUser);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Setting Tez DAG access for queryId={} with viewAclString={}, modifyStr={}",
          queryId, viewStr, modifyStr);
    }
    // set permissions for current user on DAG
    DAGAccessControls ac = new DAGAccessControls(viewStr, modifyStr);
    dag.setAccessControls(ac);
  }

  DAGClient submit(JobConf conf, DAG dag, Path scratchDir,
      LocalResource appJarLr, TezSessionState sessionState,
      List<LocalResource> additionalLr, String[] inputOutputJars,
      Map<String,LocalResource> inputOutputLocalResources)
      throws Exception {

    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_SUBMIT_DAG);
    DAGClient dagClient = null;

    Map<String, LocalResource> resourceMap = new HashMap<String, LocalResource>();
    if (additionalLr != null) {
      for (LocalResource lr: additionalLr) {
        if (lr.getType() == LocalResourceType.FILE) {
          // TEZ AM will only localize FILE (no script operators in the AM)
          resourceMap.put(utils.getBaseName(lr), lr);
        }
      }
    }

    try {
      try {
        // ready to start execution on the cluster
        sessionState.getSession().addAppMasterLocalFiles(resourceMap);
        dagClient = sessionState.getSession().submitDAG(dag);
      } catch (SessionNotRunning nr) {
        console.printInfo("Tez session was closed. Reopening...");

        // close the old one, but keep the tmp files around
        // TODO Why is the session being create using a conf instance belonging to TezTask
        //      - instead of the session conf instance.
        TezSessionPoolManager.getInstance().reopenSession(
            sessionState, this.conf, inputOutputJars, true);
        console.printInfo("Session re-established.");

        dagClient = sessionState.getSession().submitDAG(dag);
      }
    } catch (Exception e) {
      // In case of any other exception, retry. If this also fails, report original error and exit.
      try {
        console.printInfo("Dag submit failed due to " + e.getMessage() + " stack trace: "
            + Arrays.toString(e.getStackTrace()) + " retrying...");
        TezSessionPoolManager.getInstance().reopenSession(sessionState, this.conf, inputOutputJars,
            true);
        dagClient = sessionState.getSession().submitDAG(dag);
      } catch (Exception retryException) {
        // we failed to submit after retrying. Destroy session and bail.
        TezSessionPoolManager.getInstance().destroySession(sessionState);
        throw retryException;
      }
    }

    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_SUBMIT_DAG);
    return new SyncDagClient(dagClient);
  }

  /*
   * close will move the temp files into the right place for the fetch
   * task. If the job has failed it will clean up the files.
   */
  int close(TezWork work, int rc) {
    try {
      List<BaseWork> ws = work.getAllWork();
      for (BaseWork w: ws) {
        if (w instanceof MergeJoinWork) {
          w = ((MergeJoinWork) w).getMainWork();
        }
        for (Operator<?> op: w.getAllOperators()) {
          op.jobClose(conf, rc == 0);
        }
      }
    } catch (Exception e) {
      // jobClose needs to execute successfully otherwise fail task
      if (rc == 0) {
        rc = 3;
        String mesg = "Job Commit failed with exception '"
          + Utilities.getNameMessage(e) + "'";
        console.printError(mesg, "\n" + StringUtils.stringifyException(e));
      }
    }
    closeDagClientWithoutEx();
    return rc;
  }

  /**
   * Close DagClient, log warning if it throws any exception.
   * We don't want to fail query if that function fails.
   */
  private void closeDagClientWithoutEx(){
    try {
      dagClient.close();
      dagClient = null;
    } catch (Exception e) {
      LOG.warn("Failed to close DagClient", e);
    }
  }

  @Override
  public void updateTaskMetrics(Metrics metrics) {
    metrics.incrementCounter(MetricsConstant.HIVE_TEZ_TASKS);
  }

  @Override
  public boolean isMapRedTask() {
    return true;
  }

  @Override
  public StageType getType() {
    return StageType.MAPRED;
  }

  @Override
  public String getName() {
    return "TEZ";
  }

  @Override
  public Collection<MapWork> getMapWork() {
    List<MapWork> result = new LinkedList<MapWork>();
    TezWork work = getWork();

    // framework expects MapWork instances that have no physical parents (i.e.: union parent is
    // fine, broadcast parent isn't)
    for (BaseWork w: work.getAllWorkUnsorted()) {
      if (w instanceof MapWork) {
        List<BaseWork> parents = work.getParents(w);
        boolean candidate = true;
        for (BaseWork parent: parents) {
          if (!(parent instanceof UnionWork)) {
            candidate = false;
          }
        }
        if (candidate) {
          result.add((MapWork)w);
        }
      }
    }
    return result;
  }

  @Override
  public Operator<? extends OperatorDesc> getReducer(MapWork mapWork) {
    List<BaseWork> children = getWork().getChildren(mapWork);
    if (children.size() != 1) {
      return null;
    }

    if (!(children.get(0) instanceof ReduceWork)) {
      return null;
    }

    return ((ReduceWork)children.get(0)).getReducer();
  }

  @Override
  public void shutdown() {
    super.shutdown();
    if (dagClient != null) {
      LOG.info("Shutting down Tez task " + this);
      try {
        dagClient.tryKillDAG();
        LOG.info("Waiting for Tez task to shut down: " + this);
        dagClient.waitForCompletion();
      } catch (Exception ex) {
        LOG.warn("Failed to shut down TezTask" + this, ex);
      }
      closeDagClientWithoutEx();
    }
  }

  /** DAG client that does dumb global sync on all the method calls;
   * Tez DAG client is not thread safe and getting the 2nd one is not recommended. */
  public class SyncDagClient extends DAGClient {
    private final DAGClient dagClient;

    public SyncDagClient(DAGClient dagClient) {
      super();
      this.dagClient = dagClient;
    }

    @Override
    public void close() throws IOException {
      dagClient.close(); // Don't sync.
    }

    public String getDagIdentifierString() {
      // TODO: Implement this when tez is upgraded. TEZ-3550
      return null;
    }

    public String getSessionIdentifierString() {
      // TODO: Implement this when tez is upgraded. TEZ-3550
      return null;
    }


    @Override
    public String getExecutionContext() {
      return dagClient.getExecutionContext(); // Don't sync.
    }

    @Override
    @Private
    protected ApplicationReport getApplicationReportInternal() {
      throw new UnsupportedOperationException(); // The method is not exposed, and we don't use it.
    }

    @Override
    public DAGStatus getDAGStatus(@Nullable Set<StatusGetOpts> statusOptions)
        throws IOException, TezException {
      synchronized (dagClient) {
        return dagClient.getDAGStatus(statusOptions);
      }
    }

    @Override
    public DAGStatus getDAGStatus(@Nullable Set<StatusGetOpts> statusOptions,
        long timeout) throws IOException, TezException {
      synchronized (dagClient) {
        return dagClient.getDAGStatus(statusOptions, timeout);
      }
    }

    @Override
    public VertexStatus getVertexStatus(String vertexName,
        Set<StatusGetOpts> statusOptions) throws IOException, TezException {
      synchronized (dagClient) {
        return dagClient.getVertexStatus(vertexName, statusOptions);
      }
    }

    @Override
    public void tryKillDAG() throws IOException, TezException {
      synchronized (dagClient) {
        dagClient.tryKillDAG();
      }
    }

    @Override
    public DAGStatus waitForCompletion() throws IOException, TezException, InterruptedException {
      synchronized (dagClient) {
        return dagClient.waitForCompletion();
      }
    }

    @Override
    public DAGStatus waitForCompletionWithStatusUpdates(@Nullable Set<StatusGetOpts> statusGetOpts)
        throws IOException, TezException, InterruptedException {
      synchronized (dagClient) {
        return dagClient.waitForCompletionWithStatusUpdates(statusGetOpts);
      }
    }
  }
}
