/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.security.auth.login.LoginException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;

import org.apache.hadoop.hive.ql.exec.SerializationUtilities;

import com.esotericsoftware.kryo.Kryo;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.InputStream;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.io.FileNotFoundException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.runtime.api.Event;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.tez.DagUtils;
import org.apache.hadoop.hive.ql.exec.tez.HiveSplitGenerator;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.mapreduce.hadoop.InputSplitInfoMem;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitsProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.InputSpecUpdate;
import org.apache.tez.runtime.api.events.InputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;


import com.google.common.base.Preconditions;

public class LlapInputFormat<V extends WritableComparable> implements InputFormat<NullWritable, V> {

  private static final Logger LOG = LoggerFactory.getLogger(LlapInputFormat.class);

  private TezWork work;
  private Schema schema;

  public LlapInputFormat(TezWork tezWork, Schema schema) {
    this.work = tezWork;
    this.schema = schema;
  }

  // need empty constructor for bean instantiation
  public LlapInputFormat() {}

  /*
   * This proxy record reader has the duty of establishing a connected socket with LLAP, then fire
   * off the work in the split to LLAP and finally return the connected socket back in an
   * LlapRecordReader. The LlapRecordReader class reads the results from the socket.
   */
  public RecordReader<NullWritable, V> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {

    LlapInputSplit llapSplit = (LlapInputSplit)split;

    // TODO: push event into LLAP

    // this is just the portion that sets up the io to receive data
    String host = split.getLocations()[0];
    String id = job.get(LlapOutputFormat.LLAP_OF_ID_KEY);

    HiveConf conf = new HiveConf();
    Socket socket = new Socket(host,
        conf.getIntVar(HiveConf.ConfVars.LLAP_DAEMON_OUTPUT_SERVICE_PORT));

    LOG.debug("Socket connected");

    socket.getOutputStream().write(id.getBytes());
    socket.getOutputStream().write(0);
    socket.getOutputStream().flush();

    LOG.debug("Registered id: " + id);

    return new LlapRecordReader(socket.getInputStream(), llapSplit.getSchema(), Text.class);
  }

  /*
   * getSplits() gets called as part of the GenericUDFGetSplits call to get splits. Here we create
   * an array of input splits from the work item we have, figure out the location for llap and pass
   * that back for the submission. getRecordReader method above uses that split info to assign the
   * work to llap.
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    // TODO: need to build proto of plan

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
      dag.addVertex(wx);
      utils.addCredentials(mapWork, dag);

      // we have the dag now proceed to get the splits:
      HiveSplitGenerator splitGenerator = new HiveSplitGenerator(null);
      splitGenerator.initializeSplitGenerator(wxConf, mapWork);
      List<Event> eventList = splitGenerator.initialize();

      // hack - just serializing with kryo for now. This needs to be done properly
      InputSplit[] result = new InputSplit[eventList.size()];
      int i = 0;
      ByteArrayOutputStream bos = new ByteArrayOutputStream(10240);

      InputConfigureVertexTasksEvent configureEvent = (InputConfigureVertexTasksEvent)
	eventList.remove(0);

      List<TaskLocationHint> hints = configureEvent.getLocationHint().getTaskLocationHints();
      for (Event event: eventList) {
	TaskLocationHint hint = hints.remove(0);
        Set<String> hosts = hint.getHosts();
	SplitLocationInfo[] locations = new SplitLocationInfo[hosts.size()];

	int j = 0;
	for (String host: hosts) {
	  locations[j++] = new SplitLocationInfo(host,false);
	}

	bos.reset();
	Kryo kryo = SerializationUtilities.borrowKryo();
	SerializationUtilities.serializeObjectByKryo(kryo, event, bos);
	SerializationUtilities.releaseKryo(kryo);
	result[i++] = new LlapInputSplit(bos.toByteArray(), locations, schema);
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
}
