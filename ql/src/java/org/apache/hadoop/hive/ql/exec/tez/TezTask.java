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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.mapreduce.hadoop.MRHelpers;

/**
 *
 * TezTask handles the execution of TezWork. Currently it executes a graph of map and reduce work
 * using the Tez APIs directly.
 *
 */
@SuppressWarnings({"serial", "deprecation"})
public class TezTask extends Task<TezWork> {

  public TezTask() {
    super();
  }

  @Override
  public int execute(DriverContext driverContext) {
    int rc = 1;

    Context ctx = driverContext.getCtx();

    try {

      // we will localize all the files (jars, plans, hashtables) to the
      // scratch dir. let's create this first.
      Path scratchDir = new Path(ctx.getMRScratchDir());

      // jobConf will hold all the configuration for hadoop, tez, and hive
      JobConf jobConf = DagUtils.createConfiguration(conf);

      // unless already installed on all the cluster nodes, we'll have to
      // localize hive-exec.jar as well.
      LocalResource appJarLr = DagUtils.createHiveExecLocalResource(scratchDir);

      // next we translate the TezWork to a Tez DAG
      DAG dag = build(jobConf, work, scratchDir, appJarLr, ctx);

      // submit will send the job to the cluster and start executing
      DAGClient client = submit(jobConf, dag, scratchDir, appJarLr);

      // finally monitor will print progress until the job is done
      TezJobMonitor monitor = new TezJobMonitor();
      rc = monitor.monitorExecution(client);

    } catch (Exception e) {
      LOG.error("Failed to execute tez graph.", e);
    } finally {
      Utilities.clearWork(conf);
      try {
        ctx.clear();
      } catch (Exception e) {
        /*best effort*/
        LOG.warn("Failed to clean up after tez job");
      }
    }
    return rc;
  }

  private DAG build(JobConf conf, TezWork work, Path scratchDir,
      LocalResource appJarLr, Context ctx)
      throws Exception {

    Map<BaseWork, Vertex> workToVertex = new HashMap<BaseWork, Vertex>();
    Map<BaseWork, JobConf> workToConf = new HashMap<BaseWork, JobConf>();

    // we need to get the user specified local resources for this dag
    List<LocalResource> additionalLr = DagUtils.localizeTempFiles(conf);

    // getAllWork returns a topologically sorted list, which we use to make
    // sure that vertices are created before they are used in edges.
    List<BaseWork> ws = work.getAllWork();
    Collections.reverse(ws);

    // the name of the dag is what is displayed in the AM/Job UI
    DAG dag = new DAG(
        Utilities.abbreviate(HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYSTRING),
        HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVEJOBNAMELENGTH)));

    int i = ws.size();
    for (BaseWork w: ws) {

      // translate work to vertex
      JobConf wxConf = DagUtils.initializeVertexConf(conf, w);
      Vertex wx = DagUtils.createVertex(wxConf, w, scratchDir, i--,
          appJarLr, additionalLr, scratchDir.getFileSystem(conf), ctx);
      dag.addVertex(wx);
      workToVertex.put(w, wx);
      workToConf.put(w, wxConf);

      // add all dependencies (i.e.: edges) to the graph
      for (BaseWork v: work.getChildren(w)) {
        assert workToVertex.containsKey(v);
        Edge e = DagUtils.createEdge(wxConf, wx, workToConf.get(v), workToVertex.get(v));
        dag.addEdge(e);
      }
    }

    return dag;
  }

  private DAGClient submit(JobConf conf, DAG dag, Path scratchDir, LocalResource appJarLr)
      throws IOException, TezException, InterruptedException {

    TezClient tezClient = new TezClient(new TezConfiguration(conf));

    // environment variables used by application master
    Map<String,String> amEnv = new HashMap<String, String>();
    MRHelpers.updateEnvironmentForMRTasks(conf, amEnv, false);

    // setup local resources used by application master
    Map<String, LocalResource> amLrs = new HashMap<String, LocalResource>();
    amLrs.put(appJarLr.getResource().getFile(), appJarLr);

    // ready to start execution on the cluster
    DAGClient dagClient = tezClient.submitDAGApplication(dag, scratchDir,
        null, "default", Collections.singletonList(""), amEnv, amLrs,
        new TezConfiguration(conf));

    return dagClient;
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
