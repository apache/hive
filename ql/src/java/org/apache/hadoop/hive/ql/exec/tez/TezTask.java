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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.UnionWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezSession;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.GroupInputEdge;
import org.apache.tez.dag.api.SessionNotRunning;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexGroup;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.StatusGetOpts;

/**
 *
 * TezTask handles the execution of TezWork. Currently it executes a graph of map and reduce work
 * using the Tez APIs directly.
 *
 */
@SuppressWarnings({"serial", "deprecation"})
public class TezTask extends Task<TezWork> {

  private static final String CLASS_NAME = TezTask.class.getName();
  private final PerfLogger perfLogger = PerfLogger.getPerfLogger();

  private TezCounters counters;

  private DagUtils utils;

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
    DAGClient client = null;
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
      session = TezSessionPoolManager.getInstance().getSession(session, conf, false);
      ss.setTezSession(session);

      // jobConf will hold all the configuration for hadoop, tez, and hive
      JobConf jobConf = utils.createConfiguration(conf);

      // Get all user jars from work (e.g. input format stuff).
      String[] inputOutputJars = work.configureJobConfAndExtractJars(jobConf);

      // we will localize all the files (jars, plans, hashtables) to the
      // scratch dir. let's create this and tmp first.
      Path scratchDir = ctx.getMRScratchDir();

      // create the tez tmp dir
      scratchDir = utils.createTezDir(scratchDir, conf);

      // If we have any jars from input format, we need to restart the session because
      // AM will need them; so, AM has to be restarted. What a mess...
      if (!session.hasResources(inputOutputJars) && session.isOpen()) {
        LOG.info("Tez session being reopened to pass custom jars to AM");
        session.close(false);
        session = TezSessionPoolManager.getInstance().getSession(null, conf, false);
        ss.setTezSession(session);
      }

      if (!session.isOpen()) {
        // can happen if the user sets the tez flag after the session was
        // established
        LOG.info("Tez session hasn't been created yet. Opening session");
        session.open(conf, inputOutputJars);
      }
      List<LocalResource> additionalLr = session.getLocalizedResources();

      // unless already installed on all the cluster nodes, we'll have to
      // localize hive-exec.jar as well.
      LocalResource appJarLr = session.getAppJarLr();

      // next we translate the TezWork to a Tez DAG
      DAG dag = build(jobConf, work, scratchDir, appJarLr, additionalLr, ctx);

      // submit will send the job to the cluster and start executing
      client = submit(jobConf, dag, scratchDir, appJarLr, session);

      // finally monitor will print progress until the job is done
      TezJobMonitor monitor = new TezJobMonitor();
      rc = monitor.monitorExecution(client, ctx.getHiveTxnManager(), conf);

      // fetch the counters
      Set<StatusGetOpts> statusGetOpts = EnumSet.of(StatusGetOpts.GET_COUNTERS);
      counters = client.getDAGStatus(statusGetOpts).getDAGCounters();
      TezSessionPoolManager.getInstance().returnSession(session);

      if (LOG.isInfoEnabled()) {
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
      if (cleanContext) {
        try {
          ctx.clear();
        } catch (Exception e) {
          /*best effort*/
          LOG.warn("Failed to clean up after tez job");
        }
      }
      // need to either move tmp files or remove them
      if (client != null) {
        // rc will only be overwritten if close errors out
        rc = close(work, rc);
      }
    }
    return rc;
  }

  DAG build(JobConf conf, TezWork work, Path scratchDir,
      LocalResource appJarLr, List<LocalResource> additionalLr, Context ctx)
      throws Exception {

    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_BUILD_DAG);
    Map<BaseWork, Vertex> workToVertex = new HashMap<BaseWork, Vertex>();
    Map<BaseWork, JobConf> workToConf = new HashMap<BaseWork, JobConf>();

    // getAllWork returns a topologically sorted list, which we use to make
    // sure that vertices are created before they are used in edges.
    List<BaseWork> ws = work.getAllWork();
    Collections.reverse(ws);

    FileSystem fs = scratchDir.getFileSystem(conf);

    // the name of the dag is what is displayed in the AM/Job UI
    DAG dag = new DAG(work.getName());

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

        // create VertexGroup
        Vertex[] vertexArray = new Vertex[unionWorkItems.size()];

        int i = 0;
        for (BaseWork v: unionWorkItems) {
          vertexArray[i++] = workToVertex.get(v);
        }
        VertexGroup group = dag.createVertexGroup(w.getName(), vertexArray);
        
        // now hook up the children
        for (BaseWork v: children) {
          // need to pairwise patch up the configuration of the vertices
          for (BaseWork part: unionWorkItems) {
            utils.updateConfigurationForEdge(workToConf.get(part), workToVertex.get(part), 
                 workToConf.get(v), workToVertex.get(v));
          }
          
          // finally we can create the grouped edge
          GroupInputEdge e = utils.createEdge(group, workToConf.get(v),
               workToVertex.get(v), work.getEdgeProperty(w, v));

          dag.addEdge(e);
        }
      } else {
        // Regular vertices
        JobConf wxConf = utils.initializeVertexConf(conf, w);
        Vertex wx = utils.createVertex(wxConf, w, scratchDir, appJarLr, 
          additionalLr, fs, ctx, !isFinal, work);
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

          e = utils.createEdge(wxConf, wx, workToConf.get(v), workToVertex.get(v), edgeProp);
          dag.addEdge(e);
        }
      }
    }
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_BUILD_DAG);
    return dag;
  }

  DAGClient submit(JobConf conf, DAG dag, Path scratchDir,
      LocalResource appJarLr, TezSessionState sessionState)
      throws IOException, TezException, InterruptedException,
      LoginException, URISyntaxException, HiveException {

    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_SUBMIT_DAG);
    DAGClient dagClient = null;

    try {
      // ready to start execution on the cluster
      dagClient = sessionState.getSession().submitDAG(dag);
    } catch (SessionNotRunning nr) {
      console.printInfo("Tez session was closed. Reopening...");

      // close the old one, but keep the tmp files around
      sessionState.close(true);

      // (re)open the session
      sessionState.open(this.conf);

      console.printInfo("Session re-established.");

      dagClient = sessionState.getSession().submitDAG(dag);
    }

    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_SUBMIT_DAG);
    return dagClient;
  }

  /*
   * close will move the temp files into the right place for the fetch
   * task. If the job has failed it will clean up the files.
   */
  int close(TezWork work, int rc) {
    try {
      List<BaseWork> ws = work.getAllWork();
      for (BaseWork w: ws) {
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
    return rc;
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
}
