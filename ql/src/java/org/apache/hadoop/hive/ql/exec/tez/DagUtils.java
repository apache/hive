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

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.security.auth.login.LoginException;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.hadoop.hive.ql.exec.mr.ExecReducer;
import org.apache.hadoop.hive.ql.exec.tez.tools.TezMergedLogicalInput;
import org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.io.HiveOutputFormatImpl;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.hive.shims.HadoopShimsSecure.NullOutputCommitter;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.split.TezGroupedSplitsInputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeManagerDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.GroupInputEdge;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.client.PreWarmContext;
import org.apache.tez.client.TezSessionConfiguration;
import org.apache.tez.dag.api.VertexGroup;
import org.apache.tez.mapreduce.common.MRInputAMSplitGenerator;
import org.apache.tez.mapreduce.hadoop.InputSplitInfo;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.partition.MRPartitioner;
import org.apache.tez.runtime.library.input.ConcatenatedMergedKeyValueInput;
import org.apache.tez.runtime.library.input.ShuffledMergedInputLegacy;
import org.apache.tez.runtime.library.input.ShuffledUnorderedKVInput;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;
import org.apache.tez.runtime.library.output.OnFileUnorderedKVOutput;
import org.apache.tez.runtime.library.output.OnFileUnorderedPartitionedKVOutput;

/**
 * DagUtils. DagUtils is a collection of helper methods to convert
 * map and reduce work to tez vertices and edges. It handles configuration
 * objects, file localization and vertex/edge creation.
 */
public class DagUtils {

  private static final Log LOG = LogFactory.getLog(DagUtils.class.getName());
  private static final String TEZ_DIR = "_tez_scratch_dir";
  private static DagUtils instance;

  private void addCredentials(MapWork mapWork, DAG dag) {
    Set<String> paths = mapWork.getPathToAliases().keySet();
    if (paths != null && !paths.isEmpty()) {
      Iterator<URI> pathIterator = Iterators.transform(paths.iterator(), new Function<String, URI>() {
        @Override
        public URI apply(String input) {
          return new Path(input).toUri();
        }
      });

      Set<URI> uris = new HashSet<URI>();
      Iterators.addAll(uris, pathIterator);

      if (LOG.isDebugEnabled()) {
        for (URI uri: uris) {
          LOG.debug("Marking URI as needing credentials: "+uri);
        }
      }
      dag.addURIsForCredentials(uris);
    }
  }

  private void addCredentials(ReduceWork reduceWork, DAG dag) {
    // nothing at the moment
  }

  /*
   * Creates the configuration object necessary to run a specific vertex from
   * map work. This includes input formats, input processor, etc.
   */
  private JobConf initializeVertexConf(JobConf baseConf, MapWork mapWork) {
    JobConf conf = new JobConf(baseConf);

    if (mapWork.getNumMapTasks() != null) {
      conf.setInt(MRJobConfig.NUM_MAPS, mapWork.getNumMapTasks().intValue());
    }

    if (mapWork.getMaxSplitSize() != null) {
      HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMAXSPLITSIZE,
          mapWork.getMaxSplitSize().longValue());
    }

    if (mapWork.getMinSplitSize() != null) {
      HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMINSPLITSIZE,
          mapWork.getMinSplitSize().longValue());
    }

    if (mapWork.getMinSplitSizePerNode() != null) {
      HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMINSPLITSIZEPERNODE,
          mapWork.getMinSplitSizePerNode().longValue());
    }

    if (mapWork.getMinSplitSizePerRack() != null) {
      HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMINSPLITSIZEPERRACK,
          mapWork.getMinSplitSizePerRack().longValue());
    }

    Utilities.setInputAttributes(conf, mapWork);

    String inpFormat = HiveConf.getVar(conf, HiveConf.ConfVars.HIVETEZINPUTFORMAT);
    if ((inpFormat == null) || (!StringUtils.isNotBlank(inpFormat))) {
      inpFormat = ShimLoader.getHadoopShims().getInputFormatClassName();
    }

    if (mapWork.isUseBucketizedHiveInputFormat()) {
      inpFormat = BucketizedHiveInputFormat.class.getName();
    }

    conf.set("mapred.mapper.class", ExecMapper.class.getName());
    conf.set("mapred.input.format.class", inpFormat);

    return conf;
  }

  /**
   * Given a Vertex group and a vertex createEdge will create an
   * Edge between them.
   *
   * @param group The parent VertexGroup
   * @param wConf The job conf of the child vertex
   * @param w The child vertex
   * @param edgeProp the edge property of connection between the two
   * endpoints.
   */
  public GroupInputEdge createEdge(VertexGroup group, JobConf wConf,
      Vertex w, TezEdgeProperty edgeProp)
    throws IOException {

    Class mergeInputClass;

    LOG.info("Creating Edge between " + group.getGroupName() + " and " + w.getVertexName());
    w.getProcessorDescriptor().setUserPayload(MRHelpers.createUserPayloadFromConf(wConf));

    EdgeType edgeType = edgeProp.getEdgeType();
    switch (edgeType) {
      case BROADCAST_EDGE:
        mergeInputClass = ConcatenatedMergedKeyValueInput.class;
        break;
      case CUSTOM_EDGE:
        mergeInputClass = ConcatenatedMergedKeyValueInput.class;
        int numBuckets = edgeProp.getNumBuckets();
        VertexManagerPluginDescriptor desc = new VertexManagerPluginDescriptor(
            CustomPartitionVertex.class.getName());
        byte[] userPayload = ByteBuffer.allocate(4).putInt(numBuckets).array();
        desc.setUserPayload(userPayload);
        w.setVertexManagerPlugin(desc);
        break;

      case CUSTOM_SIMPLE_EDGE:
        mergeInputClass = ConcatenatedMergedKeyValueInput.class;
        break;

      case SIMPLE_EDGE:
      default:
        mergeInputClass = TezMergedLogicalInput.class;
        break;
    }

    return new GroupInputEdge(group, w, createEdgeProperty(edgeProp),
        new InputDescriptor(mergeInputClass.getName()));
  }

  /**
   * Given two vertices a, b update their configurations to be used in an Edge a-b
   */
  public void updateConfigurationForEdge(JobConf vConf, Vertex v, JobConf wConf, Vertex w) 
    throws IOException {

    // Tez needs to setup output subsequent input pairs correctly
    MultiStageMRConfToTezTranslator.translateVertexConfToTez(wConf, vConf);

    // update payloads (configuration for the vertices might have changed)
    v.getProcessorDescriptor().setUserPayload(MRHelpers.createUserPayloadFromConf(vConf));
    w.getProcessorDescriptor().setUserPayload(MRHelpers.createUserPayloadFromConf(wConf));
  }

  /**
   * Given two vertices and their respective configuration objects createEdge
   * will create an Edge object that connects the two.
   *
   * @param vConf JobConf of the first vertex
   * @param v The first vertex (source)
   * @param wConf JobConf of the second vertex
   * @param w The second vertex (sink)
   * @return
   */
  public Edge createEdge(JobConf vConf, Vertex v, JobConf wConf, Vertex w,
      TezEdgeProperty edgeProp)
    throws IOException {

    updateConfigurationForEdge(vConf, v, wConf, w);

    if (edgeProp.getEdgeType() == EdgeType.CUSTOM_EDGE) {
      int numBuckets = edgeProp.getNumBuckets();
      byte[] userPayload = ByteBuffer.allocate(4).putInt(numBuckets).array();
      VertexManagerPluginDescriptor desc = new VertexManagerPluginDescriptor(
          CustomPartitionVertex.class.getName());
      desc.setUserPayload(userPayload);
      w.setVertexManagerPlugin(desc);
    }

    return new Edge(v, w, createEdgeProperty(edgeProp));
  }

  /*
   * Helper function to create an edge property from an edge type.
   */
  private EdgeProperty createEdgeProperty(TezEdgeProperty edgeProp) throws IOException {
    DataMovementType dataMovementType;
    Class logicalInputClass;
    Class logicalOutputClass;

    EdgeProperty edgeProperty = null;
    EdgeType edgeType = edgeProp.getEdgeType();
    switch (edgeType) {
      case BROADCAST_EDGE:
        dataMovementType = DataMovementType.BROADCAST;
        logicalOutputClass = OnFileUnorderedKVOutput.class;
        logicalInputClass = ShuffledUnorderedKVInput.class;
        break;

      case CUSTOM_EDGE:
        
        dataMovementType = DataMovementType.CUSTOM;
        logicalOutputClass = OnFileUnorderedPartitionedKVOutput.class;
        logicalInputClass = ShuffledUnorderedKVInput.class;
        EdgeManagerDescriptor edgeDesc = new EdgeManagerDescriptor(
            CustomPartitionEdge.class.getName());
        CustomEdgeConfiguration edgeConf = 
            new CustomEdgeConfiguration(edgeProp.getNumBuckets(), null);
          DataOutputBuffer dob = new DataOutputBuffer();
          edgeConf.write(dob);
          byte[] userPayload = dob.getData();
        edgeDesc.setUserPayload(userPayload);
        edgeProperty =
          new EdgeProperty(edgeDesc,
              DataSourceType.PERSISTED,
              SchedulingType.SEQUENTIAL,
              new OutputDescriptor(logicalOutputClass.getName()),
              new InputDescriptor(logicalInputClass.getName()));
        break;

      case CUSTOM_SIMPLE_EDGE:
        dataMovementType = DataMovementType.SCATTER_GATHER;
        logicalOutputClass = OnFileUnorderedPartitionedKVOutput.class;
        logicalInputClass = ShuffledUnorderedKVInput.class;
        break;

      case SIMPLE_EDGE:
      default:
        dataMovementType = DataMovementType.SCATTER_GATHER;
        logicalOutputClass = OnFileSortedOutput.class;
        logicalInputClass = ShuffledMergedInputLegacy.class;
        break;
    }

    if (edgeProperty == null) {
      edgeProperty =
        new EdgeProperty(dataMovementType,
            DataSourceType.PERSISTED,
            SchedulingType.SEQUENTIAL,
            new OutputDescriptor(logicalOutputClass.getName()),
            new InputDescriptor(logicalInputClass.getName()));
    }

    return edgeProperty;
  }

  /*
   * Helper to determine the size of the container requested
   * from yarn. Falls back to Map-reduce's map size if tez
   * container size isn't set.
   */
  private Resource getContainerResource(Configuration conf) {
    Resource containerResource;
    int memory = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVETEZCONTAINERSIZE) > 0 ?
      HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVETEZCONTAINERSIZE) :
      conf.getInt(MRJobConfig.MAP_MEMORY_MB, MRJobConfig.DEFAULT_MAP_MEMORY_MB);
    int cpus = conf.getInt(MRJobConfig.MAP_CPU_VCORES,
        MRJobConfig.DEFAULT_MAP_CPU_VCORES);
    return Resource.newInstance(memory, cpus);
  }

  /*
   * Helper to determine what java options to use for the containers
   * Falls back to Map-reduces map java opts if no tez specific options
   * are set
   */
  private String getContainerJavaOpts(Configuration conf) {
    String javaOpts = HiveConf.getVar(conf, HiveConf.ConfVars.HIVETEZJAVAOPTS);
    if (javaOpts != null && !javaOpts.isEmpty()) {
      String logLevel = HiveConf.getVar(conf, HiveConf.ConfVars.HIVETEZLOGLEVEL);
      List<String> logProps = Lists.newArrayList();
      MRHelpers.addLog4jSystemProperties(logLevel, logProps);
      StringBuilder sb = new StringBuilder();
      for (String str : logProps) {
        sb.append(str).append(" ");
      }
      return javaOpts + " " + sb.toString();
    }
    return MRHelpers.getMapJavaOpts(conf);
  }

  /*
   * Helper function to create Vertex from MapWork.
   */
  private Vertex createVertex(JobConf conf, MapWork mapWork,
      LocalResource appJarLr, List<LocalResource> additionalLr, FileSystem fs,
      Path mrScratchDir, Context ctx, TezWork tezWork) throws Exception {

    Path tezDir = getTezDir(mrScratchDir);

    // set up the operator plan
    Utilities.setMapWork(conf, mapWork, mrScratchDir, false);

    // create the directories FileSinkOperators need
    Utilities.createTmpDirs(conf, mapWork);

    // Tez ask us to call this even if there's no preceding vertex
    MultiStageMRConfToTezTranslator.translateVertexConfToTez(conf, null);

    // finally create the vertex
    Vertex map = null;

    // use tez to combine splits
    boolean useTezGroupedSplits = false;

    int numTasks = -1;
    Class amSplitGeneratorClass = null;
    InputSplitInfo inputSplitInfo = null;
    Class inputFormatClass = conf.getClass("mapred.input.format.class",
        InputFormat.class);

    boolean vertexHasCustomInput = false;
    if (tezWork != null) {
      for (BaseWork baseWork : tezWork.getParents(mapWork)) {
        if (tezWork.getEdgeType(baseWork, mapWork) == EdgeType.CUSTOM_EDGE) {
          vertexHasCustomInput = true;
        }
      }
    }
    if (vertexHasCustomInput) {
      useTezGroupedSplits = false;
      // grouping happens in execution phase. Setting the class to TezGroupedSplitsInputFormat 
      // here would cause pre-mature grouping which would be incorrect.
      inputFormatClass = HiveInputFormat.class;
      conf.setClass("mapred.input.format.class", HiveInputFormat.class, InputFormat.class);
      // mapreduce.tez.input.initializer.serialize.event.payload should be set to false when using
      // this plug-in to avoid getting a serialized event at run-time.
      conf.setBoolean("mapreduce.tez.input.initializer.serialize.event.payload", false);
    } else {
      // we'll set up tez to combine spits for us iff the input format
      // is HiveInputFormat
      if (inputFormatClass == HiveInputFormat.class) {
        useTezGroupedSplits = true;
        conf.setClass("mapred.input.format.class", TezGroupedSplitsInputFormat.class, InputFormat.class);
      }
    }

    if (HiveConf.getBoolVar(conf, ConfVars.HIVE_AM_SPLIT_GENERATION)) {
      // if we're generating the splits in the AM, we just need to set
      // the correct plugin.
      amSplitGeneratorClass = MRInputAMSplitGenerator.class;
    } else {
      // client side split generation means we have to compute them now
      inputSplitInfo = MRHelpers.generateInputSplits(conf,
          new Path(tezDir, "split_"+mapWork.getName().replaceAll(" ", "_")));
      numTasks = inputSplitInfo.getNumTasks();
    }

    byte[] serializedConf = MRHelpers.createUserPayloadFromConf(conf);
    map = new Vertex(mapWork.getName(),
        new ProcessorDescriptor(MapTezProcessor.class.getName()).
        setUserPayload(serializedConf), numTasks, getContainerResource(conf));
    Map<String, String> environment = new HashMap<String, String>();
    MRHelpers.updateEnvironmentForMRTasks(conf, environment, true);
    map.setTaskEnvironment(environment);
    map.setJavaOpts(getContainerJavaOpts(conf));

    assert mapWork.getAliasToWork().keySet().size() == 1;

    String alias = mapWork.getAliasToWork().keySet().iterator().next();

    byte[] mrInput = null;
    if (useTezGroupedSplits) {
      mrInput = MRHelpers.createMRInputPayloadWithGrouping(serializedConf,
          HiveInputFormat.class.getName());
    } else {
      mrInput = MRHelpers.createMRInputPayload(serializedConf, null);
    }
    map.addInput(alias,
        new InputDescriptor(MRInputLegacy.class.getName()).
        setUserPayload(mrInput), amSplitGeneratorClass);

    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    localResources.put(getBaseName(appJarLr), appJarLr);
    for (LocalResource lr: additionalLr) {
      localResources.put(getBaseName(lr), lr);
    }

    if (inputSplitInfo != null) {
      // only relevant for client-side split generation
      map.setTaskLocationsHint(inputSplitInfo.getTaskLocationHints());
      MRHelpers.updateLocalResourcesForInputSplits(FileSystem.get(conf), inputSplitInfo,
          localResources);
    }

    map.setTaskLocalResources(localResources);
    return map;
  }

  /*
   * Helper function to create JobConf for specific ReduceWork.
   */
  private JobConf initializeVertexConf(JobConf baseConf, ReduceWork reduceWork) {
    JobConf conf = new JobConf(baseConf);

    conf.set("mapred.reducer.class", ExecReducer.class.getName());

    boolean useSpeculativeExecReducers = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.HIVESPECULATIVEEXECREDUCERS);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HADOOPSPECULATIVEEXECREDUCERS,
        useSpeculativeExecReducers);

    return conf;
  }

  /*
   * Helper function to create Vertex for given ReduceWork.
   */
  private Vertex createVertex(JobConf conf, ReduceWork reduceWork,
      LocalResource appJarLr, List<LocalResource> additionalLr, FileSystem fs,
      Path mrScratchDir, Context ctx) throws Exception {

    // set up operator plan
    Utilities.setReduceWork(conf, reduceWork, mrScratchDir, false);

    // create the directories FileSinkOperators need
    Utilities.createTmpDirs(conf, reduceWork);

    // Call once here, will be updated when we find edges
    MultiStageMRConfToTezTranslator.translateVertexConfToTez(conf, null);

    // create the vertex
    Vertex reducer = new Vertex(reduceWork.getName(),
        new ProcessorDescriptor(ReduceTezProcessor.class.getName()).
        setUserPayload(MRHelpers.createUserPayloadFromConf(conf)),
        reduceWork.getNumReduceTasks(), getContainerResource(conf));

    Map<String, String> environment = new HashMap<String, String>();

    MRHelpers.updateEnvironmentForMRTasks(conf, environment, false);
    reducer.setTaskEnvironment(environment);

    reducer.setJavaOpts(getContainerJavaOpts(conf));

    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    localResources.put(getBaseName(appJarLr), appJarLr);
    for (LocalResource lr: additionalLr) {
      localResources.put(getBaseName(lr), lr);
    }
    reducer.setTaskLocalResources(localResources);

    return reducer;
  }

  /*
   * Helper method to create a yarn local resource.
   */
  private LocalResource createLocalResource(FileSystem remoteFs, Path file,
      LocalResourceType type, LocalResourceVisibility visibility) {

    FileStatus fstat = null;
    try {
      fstat = remoteFs.getFileStatus(file);
    } catch (IOException e) {
      e.printStackTrace();
    }

    URL resourceURL = ConverterUtils.getYarnUrlFromPath(file);
    long resourceSize = fstat.getLen();
    long resourceModificationTime = fstat.getModificationTime();
    LOG.info("Resource modification time: " + resourceModificationTime);

    LocalResource lr = Records.newRecord(LocalResource.class);
    lr.setResource(resourceURL);
    lr.setType(type);
    lr.setSize(resourceSize);
    lr.setVisibility(visibility);
    lr.setTimestamp(resourceModificationTime);

    return lr;
  }

  /**
   * @param sessionConfig session configuration
   * @param numContainers number of containers to pre-warm
   * @param localResources additional resources to pre-warm with
   * @return prewarm context object
   */
  public PreWarmContext createPreWarmContext(TezSessionConfiguration sessionConfig, int numContainers,
      Map<String, LocalResource> localResources) throws IOException, TezException {

    Configuration conf = sessionConfig.getTezConfiguration();

    ProcessorDescriptor prewarmProcDescriptor = new ProcessorDescriptor(HivePreWarmProcessor.class.getName());
    prewarmProcDescriptor.setUserPayload(MRHelpers.createUserPayloadFromConf(conf));

    PreWarmContext context = new PreWarmContext(prewarmProcDescriptor, getContainerResource(conf),
        numContainers, new VertexLocationHint(null));

    Map<String, LocalResource> combinedResources = new HashMap<String, LocalResource>();

    combinedResources.putAll(sessionConfig.getSessionResources());
    if (localResources != null) {
      combinedResources.putAll(localResources);
    }

    context.setLocalResources(combinedResources);

    /* boiler plate task env */
    Map<String, String> environment = new HashMap<String, String>();
    MRHelpers.updateEnvironmentForMRTasks(conf, environment, true);
    context.setEnvironment(environment);
    context.setJavaOpts(getContainerJavaOpts(conf));
    return context;
  }

  /**
   * @param conf
   * @return path to destination directory on hdfs
   * @throws LoginException if we are unable to figure user information
   * @throws IOException when any dfs operation fails.
   */
  public Path getDefaultDestDir(Configuration conf) throws LoginException, IOException {
    UserGroupInformation ugi = ShimLoader.getHadoopShims().getUGIForConf(conf);
    String userName = ShimLoader.getHadoopShims().getShortUserName(ugi);
    String userPathStr = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_USER_INSTALL_DIR);
    Path userPath = new Path(userPathStr);
    FileSystem fs = userPath.getFileSystem(conf);
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IOException(ErrorMsg.INVALID_HDFS_URI.format(userPathStr));
    }

    String jarPathStr = userPathStr + "/" + userName;
    String hdfsDirPathStr = jarPathStr;
    Path hdfsDirPath = new Path(hdfsDirPathStr);

    FileStatus fstatus = fs.getFileStatus(hdfsDirPath);
    if (!fstatus.isDir()) {
      throw new IOException(ErrorMsg.INVALID_DIR.format(hdfsDirPath.toString()));
    }

    Path retPath = new Path(hdfsDirPath.toString() + "/.hiveJars");

    fs.mkdirs(retPath);
    return retPath;
  }

  /**
   * Localizes files, archives and jars the user has instructed us
   * to provide on the cluster as resources for execution.
   *
   * @param conf
   * @return List<LocalResource> local resources to add to execution
   * @throws IOException when hdfs operation fails
   * @throws LoginException when getDefaultDestDir fails with the same exception
   */
  public List<LocalResource> localizeTempFilesFromConf(
      String hdfsDirPathStr, Configuration conf) throws IOException, LoginException {
    List<LocalResource> tmpResources = new ArrayList<LocalResource>();

    String addedFiles = Utilities.getResourceFiles(conf, SessionState.ResourceType.FILE);
    if (StringUtils.isNotBlank(addedFiles)) {
      HiveConf.setVar(conf, ConfVars.HIVEADDEDFILES, addedFiles);
    }
    String addedJars = Utilities.getResourceFiles(conf, SessionState.ResourceType.JAR);
    if (StringUtils.isNotBlank(addedJars)) {
      HiveConf.setVar(conf, ConfVars.HIVEADDEDJARS, addedJars);
    }
    String addedArchives = Utilities.getResourceFiles(conf, SessionState.ResourceType.ARCHIVE);
    if (StringUtils.isNotBlank(addedArchives)) {
      HiveConf.setVar(conf, ConfVars.HIVEADDEDARCHIVES, addedArchives);
    }

    String auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS);

    // need to localize the additional jars and files
    // we need the directory on hdfs to which we shall put all these files
    String allFiles = auxJars + "," + addedJars + "," + addedFiles + "," + addedArchives;
    addTempFiles(conf, tmpResources, hdfsDirPathStr, allFiles.split(","));
    return tmpResources;
  }

  /**
   * Localizes files, archives and jars from a provided array of names.
   * @param hdfsDirPathStr Destination directoty in HDFS.
   * @param conf Configuration.
   * @param inputOutputJars The file names to localize.
   * @return List<LocalResource> local resources to add to execution
   * @throws IOException when hdfs operation fails.
   * @throws LoginException when getDefaultDestDir fails with the same exception
   */
  public List<LocalResource> localizeTempFiles(String hdfsDirPathStr, Configuration conf,
      String[] inputOutputJars) throws IOException, LoginException {
    if (inputOutputJars == null) return null;
    List<LocalResource> tmpResources = new ArrayList<LocalResource>();
    addTempFiles(conf, tmpResources, hdfsDirPathStr, inputOutputJars);
    return tmpResources;
  }

  private void addTempFiles(Configuration conf,
      List<LocalResource> tmpResources, String hdfsDirPathStr,
      String[] files) throws IOException {
    for (String file : files) {
      if (!StringUtils.isNotBlank(file)) {
        continue;
      }
      Path hdfsFilePath = new Path(hdfsDirPathStr, getResourceBaseName(new Path(file)));
      LocalResource localResource = localizeResource(new Path(file),
          hdfsFilePath, conf);
      tmpResources.add(localResource);
    }
  }

  public FileStatus getHiveJarDirectory(Configuration conf) throws IOException, LoginException {
    FileStatus fstatus = null;
    String hdfsDirPathStr = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_JAR_DIRECTORY, null);
    if (hdfsDirPathStr != null) {
      LOG.info("Hive jar directory is " + hdfsDirPathStr);
      fstatus = validateTargetDir(new Path(hdfsDirPathStr), conf);
    }

    if (fstatus == null) {
      Path destDir = getDefaultDestDir(conf);
      LOG.info("Jar dir is null/directory doesn't exist. Choosing HIVE_INSTALL_DIR - " + destDir);
      fstatus = validateTargetDir(destDir, conf);
    }

    if (fstatus == null) {
      throw new IOException(ErrorMsg.NO_VALID_LOCATIONS.getMsg());
    }
    return fstatus;
  }

  public static FileStatus validateTargetDir(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IOException(ErrorMsg.INVALID_HDFS_URI.format(path.toString()));
    }
    FileStatus fstatus = null;
    try {
      fstatus = fs.getFileStatus(path);
    } catch (FileNotFoundException fe) {
      // do nothing
    }
    return (fstatus != null && fstatus.isDir()) ? fstatus : null;
  }

  // the api that finds the jar being used by this class on disk
  public String getExecJarPathLocal () throws URISyntaxException {
    // returns the location on disc of the jar of this class.
    return DagUtils.class.getProtectionDomain().getCodeSource().getLocation().toURI().toString();
  }

  /*
   * Helper function to retrieve the basename of a local resource
   */
  public String getBaseName(LocalResource lr) {
    return FilenameUtils.getName(lr.getResource().getFile());
  }

  /**
   * @param pathStr - the string from which we try to determine the resource base name
   * @return the name of the resource from a given path string.
   */
  public String getResourceBaseName(Path path) {
    return path.getName();
  }

  /**
   * @param src the source file.
   * @param dest the destination file.
   * @param conf the configuration
   * @return true if the file names match else returns false.
   * @throws IOException when any file system related call fails
   */
  private boolean checkPreExisting(Path src, Path dest, Configuration conf)
    throws IOException {
    FileSystem destFS = dest.getFileSystem(conf);
    FileSystem sourceFS = src.getFileSystem(conf);
    if (destFS.exists(dest)) {
      return (sourceFS.getFileStatus(src).getLen() == destFS.getFileStatus(dest).getLen());
    }
    return false;
  }

  /**
   * @param src path to the source for the resource
   * @param dest path in hdfs for the resource
   * @param conf
   * @return localresource from tez localization.
   * @throws IOException when any file system related calls fails.
   */
  public LocalResource localizeResource(Path src, Path dest, Configuration conf)
    throws IOException {
    FileSystem destFS = dest.getFileSystem(conf);
    if (!(destFS instanceof DistributedFileSystem)) {
      throw new IOException(ErrorMsg.INVALID_HDFS_URI.format(dest.toString()));
    }

    if (src != null) {
      // copy the src to the destination and create local resource.
      // do not overwrite.
      LOG.info("Localizing resource because it does not exist: " + src + " to dest: " + dest);
      try {
        destFS.copyFromLocalFile(false, false, src, dest);
      } catch (IOException e) {
        LOG.info("Looks like another thread is writing the same file will wait.");
        int waitAttempts =
            conf.getInt(HiveConf.ConfVars.HIVE_LOCALIZE_RESOURCE_NUM_WAIT_ATTEMPTS.varname,
                HiveConf.ConfVars.HIVE_LOCALIZE_RESOURCE_NUM_WAIT_ATTEMPTS.defaultIntVal);
        long sleepInterval =
            conf.getLong(HiveConf.ConfVars.HIVE_LOCALIZE_RESOURCE_WAIT_INTERVAL.varname,
                HiveConf.ConfVars.HIVE_LOCALIZE_RESOURCE_WAIT_INTERVAL.defaultLongVal);
        LOG.info("Number of wait attempts: " + waitAttempts + ". Wait interval: "
            + sleepInterval);
        boolean found = false;
        for (int i = 0; i < waitAttempts; i++) {
          if (!checkPreExisting(src, dest, conf)) {
            try {
              Thread.currentThread().sleep(sleepInterval);
            } catch (InterruptedException interruptedException) {
              throw new IOException(interruptedException);
            }
          } else {
            found = true;
            break;
          }
        }
        if (!found) {
          LOG.error("Could not find the jar that was being uploaded");
          throw new IOException("Previous writer likely failed to write " + dest +
              ". Failing because I am unlikely to write too.");
        }
      }
    }

    return createLocalResource(destFS, dest, LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION);
  }

  /**
   * Creates and initializes a JobConf object that can be used to execute
   * the DAG. The configuration object will contain configurations from mapred-site
   * overlaid with key/value pairs from the hiveConf object. Finally it will also
   * contain some hive specific configurations that do not change from DAG to DAG.
   *
   * @param hiveConf Current hiveConf for the execution
   * @return JobConf base configuration for job execution
   * @throws IOException
   */
  public JobConf createConfiguration(HiveConf hiveConf) throws IOException {
    hiveConf.setBoolean("mapred.mapper.new-api", false);

    JobConf conf = (JobConf) MRHelpers.getBaseMRConfiguration(hiveConf);

    conf.set("mapred.output.committer.class", NullOutputCommitter.class.getName());

    conf.setBoolean("mapred.committer.job.setup.cleanup.needed", false);
    conf.setBoolean("mapred.committer.job.task.cleanup.needed", false);

    conf.setClass("mapred.output.format.class", HiveOutputFormatImpl.class, OutputFormat.class);

    conf.set(MRJobConfig.OUTPUT_KEY_CLASS, HiveKey.class.getName());
    conf.set(MRJobConfig.OUTPUT_VALUE_CLASS, BytesWritable.class.getName());

    conf.set("mapred.partitioner.class", HiveConf.getVar(conf, HiveConf.ConfVars.HIVEPARTITIONER));
    conf.set("tez.runtime.partitioner.class", MRPartitioner.class.getName());

    return conf;
  }

  /**
   * Creates and initializes the JobConf object for a given BaseWork object.
   *
   * @param conf Any configurations in conf will be copied to the resulting new JobConf object.
   * @param work BaseWork will be used to populate the configuration object.
   * @return JobConf new configuration object
   */
  public JobConf initializeVertexConf(JobConf conf, BaseWork work) {

    // simply dispatch the call to the right method for the actual (sub-) type of
    // BaseWork.
    if (work instanceof MapWork) {
      return initializeVertexConf(conf, (MapWork)work);
    } else if (work instanceof ReduceWork) {
      return initializeVertexConf(conf, (ReduceWork)work);
    } else {
      assert false;
      return null;
    }
  }

  /**
   * Create a vertex from a given work object.
   *
   * @param conf JobConf to be used to this execution unit
   * @param work The instance of BaseWork representing the actual work to be performed
   * by this vertex.
   * @param scratchDir HDFS scratch dir for this execution unit.
   * @param list 
   * @param appJarLr Local resource for hive-exec.
   * @param additionalLr
   * @param fileSystem FS corresponding to scratchDir and LocalResources
   * @param ctx This query's context
   * @return Vertex
   */
  public Vertex createVertex(JobConf conf, BaseWork work,
      Path scratchDir, LocalResource appJarLr, 
      List<LocalResource> additionalLr,
      FileSystem fileSystem, Context ctx, boolean hasChildren, TezWork tezWork) throws Exception {

    Vertex v = null;
    // simply dispatch the call to the right method for the actual (sub-) type of
    // BaseWork.
    if (work instanceof MapWork) {
      v = createVertex(conf, (MapWork) work, appJarLr,
          additionalLr, fileSystem, scratchDir, ctx, tezWork);
    } else if (work instanceof ReduceWork) {
      v = createVertex(conf, (ReduceWork) work, appJarLr,
          additionalLr, fileSystem, scratchDir, ctx);
    } else {
      // something is seriously wrong if this is happening
      throw new HiveException(ErrorMsg.GENERIC_ERROR.getErrorCodedMsg());
    }

    // initialize stats publisher if necessary
    if (work.isGatheringStats()) {
      StatsPublisher statsPublisher;
      StatsFactory factory = StatsFactory.newFactory(conf);
      if (factory != null) {
        statsPublisher = factory.getStatsPublisher();
        if (!statsPublisher.init(conf)) { // creating stats table if not exists
          if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STATS_RELIABLE)) {
            throw
              new HiveException(ErrorMsg.STATSPUBLISHER_INITIALIZATION_ERROR.getErrorCodedMsg());
          }
        }
      }
    }


    // final vertices need to have at least one output
    if (!hasChildren) {
      v.addOutput("out_"+work.getName(),
          new OutputDescriptor(MROutput.class.getName())
          .setUserPayload(MRHelpers.createUserPayloadFromConf(conf)));
    }

    return v;
  }

  /**
   * Set up credentials for the base work on secure clusters
   */
  public void addCredentials(BaseWork work, DAG dag) {
    if (work instanceof MapWork) {
      addCredentials((MapWork) work, dag);
    } else if (work instanceof ReduceWork) {
      addCredentials((ReduceWork) work, dag);
    }
  }

  /**
   * createTezDir creates a temporary directory in the scratchDir folder to
   * be used with Tez. Assumes scratchDir exists.
   */
  public Path createTezDir(Path scratchDir, Configuration conf)
      throws IOException {
    UserGroupInformation ugi;
    String userName = System.getProperty("user.name");
    try {
      ugi = ShimLoader.getHadoopShims().getUGIForConf(conf);
      userName = ShimLoader.getHadoopShims().getShortUserName(ugi);
    } catch (LoginException e) {
      throw new IOException(e);
    }

    scratchDir = new Path(scratchDir, userName);

    Path tezDir = getTezDir(scratchDir);
    FileSystem fs = tezDir.getFileSystem(conf);
    LOG.debug("TezDir path set " + tezDir + " for user: " + userName);
    // since we are adding the user name to the scratch dir, we do not
    // need to give more permissions here
    fs.mkdirs(tezDir);

    return tezDir;

  }

  /**
   * Gets the tez dir that belongs to the hive scratch dir
   */
  public Path getTezDir(Path scratchDir) {
    return new Path(scratchDir, TEZ_DIR);
  }

  /**
   * Singleton
   * @return instance of this class
   */
  public static DagUtils getInstance() {
    if (instance == null) {
      instance = new DagUtils();
    }
    return instance;
  }

  private DagUtils() {
    // don't instantiate
  }
}
