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

import javax.security.auth.login.LoginException;

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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.hadoop.hive.ql.exec.mr.ExecReducer;
import org.apache.hadoop.hive.ql.exec.tez.tools.TezMergedLogicalInput;
import org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils.NullOutputCommitter;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.io.HiveOutputFormatImpl;
import org.apache.hadoop.hive.ql.io.merge.MergeFileMapper;
import org.apache.hadoop.hive.ql.io.merge.MergeFileOutputFormat;
import org.apache.hadoop.hive.ql.io.merge.MergeFileWork;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.TezWork.VertexType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.GroupInputEdge;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.PreWarmVertex;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexGroup;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.input.MultiMRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.partition.MRPartitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.comparator.TezBytesComparator;
import org.apache.tez.runtime.library.common.serializer.TezBytesWritableSerialization;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.input.ConcatenatedMergedKeyValueInput;

/**
 * DagUtils. DagUtils is a collection of helper methods to convert
 * map and reduce work to tez vertices and edges. It handles configuration
 * objects, file localization and vertex/edge creation.
 */
public class DagUtils {

  public static final String TEZ_TMP_DIR_KEY = "_hive_tez_tmp_dir";
  private static final Log LOG = LogFactory.getLog(DagUtils.class.getName());
  private static final String TEZ_DIR = "_tez_scratch_dir";
  private static DagUtils instance;
  // The merge file being currently processed.
  public static final String TEZ_MERGE_CURRENT_MERGE_FILE_PREFIX =
      "hive.tez.current.merge.file.prefix";
  // "A comma separated list of work names used as prefix.
  public static final String TEZ_MERGE_WORK_FILE_PREFIXES = "hive.tez.merge.file.prefixes";

  private void addCredentials(MapWork mapWork, DAG dag) {
    Set<String> paths = mapWork.getPathToAliases().keySet();
    if (!paths.isEmpty()) {
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
  private JobConf initializeVertexConf(JobConf baseConf, Context context, MapWork mapWork) {
    JobConf conf = new JobConf(baseConf);

    conf.set(Operator.CONTEXT_NAME_KEY, mapWork.getName());

    if (mapWork.getNumMapTasks() != null) {
      // Is this required ?
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

    if (mapWork.isUseBucketizedHiveInputFormat()) {
      inpFormat = BucketizedHiveInputFormat.class.getName();
    }

    if (mapWork.isUseOneNullRowInputFormat()) {
      inpFormat = CombineHiveInputFormat.class.getName();
    }

    if (mapWork.getDummyTableScan()) {
      // hive input format doesn't handle the special condition of no paths + 1
      // split correctly.
      inpFormat = CombineHiveInputFormat.class.getName();
    }

    conf.set(TEZ_TMP_DIR_KEY, context.getMRTmpPath().toUri().toString());
    conf.set("mapred.mapper.class", ExecMapper.class.getName());
    conf.set("mapred.input.format.class", inpFormat);

    if (mapWork instanceof MergeFileWork) {
      MergeFileWork mfWork = (MergeFileWork) mapWork;
      // This mapper class is used for serializaiton/deserializaiton of merge
      // file work.
      conf.set("mapred.mapper.class", MergeFileMapper.class.getName());
      conf.set("mapred.input.format.class", mfWork.getInputformat());
      conf.setClass("mapred.output.format.class", MergeFileOutputFormat.class,
          FileOutputFormat.class);
    }

    return conf;
  }

  /**
   * Given a Vertex group and a vertex createEdge will create an
   * Edge between them.
   *
   * @param group The parent VertexGroup
   * @param vConf The job conf of one of the parrent (grouped) vertices
   * @param w The child vertex
   * @param edgeProp the edge property of connection between the two
   * endpoints.
   */
  @SuppressWarnings("rawtypes")
  public GroupInputEdge createEdge(VertexGroup group, JobConf vConf, Vertex w,
      TezEdgeProperty edgeProp, VertexType vertexType)
    throws IOException {

    Class mergeInputClass;

    LOG.info("Creating Edge between " + group.getGroupName() + " and " + w.getName());

    EdgeType edgeType = edgeProp.getEdgeType();
    switch (edgeType) {
    case BROADCAST_EDGE:
      mergeInputClass = ConcatenatedMergedKeyValueInput.class;
      break;
    case CUSTOM_EDGE: {
      mergeInputClass = ConcatenatedMergedKeyValueInput.class;
      int numBuckets = edgeProp.getNumBuckets();
      CustomVertexConfiguration vertexConf = new CustomVertexConfiguration(numBuckets, vertexType);
      DataOutputBuffer dob = new DataOutputBuffer();
      vertexConf.write(dob);
      VertexManagerPluginDescriptor desc =
          VertexManagerPluginDescriptor.create(CustomPartitionVertex.class.getName());
      byte[] userPayloadBytes = dob.getData();
      ByteBuffer userPayload = ByteBuffer.wrap(userPayloadBytes);
      desc.setUserPayload(UserPayload.create(userPayload));
      w.setVertexManagerPlugin(desc);
      break;
    }

    case CUSTOM_SIMPLE_EDGE:
      mergeInputClass = ConcatenatedMergedKeyValueInput.class;
      break;

    case SIMPLE_EDGE:
      setupAutoReducerParallelism(edgeProp, w);
      // fall through

    default:
      mergeInputClass = TezMergedLogicalInput.class;
      break;
    }

    return GroupInputEdge.create(group, w, createEdgeProperty(edgeProp, vConf),
        InputDescriptor.create(mergeInputClass.getName()));
  }

  /**
   * Given two vertices and the configuration for the source vertex, createEdge
   * will create an Edge object that connects the two.
   *
   * @param vConf JobConf of the first (source) vertex
   * @param v The first vertex (source)
   * @param w The second vertex (sink)
   * @return
   */
  public Edge createEdge(JobConf vConf, Vertex v, Vertex w, TezEdgeProperty edgeProp,
      VertexType vertexType)
    throws IOException {

    switch(edgeProp.getEdgeType()) {
    case CUSTOM_EDGE: {
      int numBuckets = edgeProp.getNumBuckets();
      CustomVertexConfiguration vertexConf = new CustomVertexConfiguration(numBuckets, vertexType);
      DataOutputBuffer dob = new DataOutputBuffer();
      vertexConf.write(dob);
      VertexManagerPluginDescriptor desc = VertexManagerPluginDescriptor.create(
          CustomPartitionVertex.class.getName());
      byte[] userPayloadBytes = dob.getData();
      ByteBuffer userPayload = ByteBuffer.wrap(userPayloadBytes);
      desc.setUserPayload(UserPayload.create(userPayload));
      w.setVertexManagerPlugin(desc);
      break;
    }
    case SIMPLE_EDGE: {
      setupAutoReducerParallelism(edgeProp, w);
      break;
    }
    default:
      // nothing
    }

    return Edge.create(v, w, createEdgeProperty(edgeProp, vConf));
  }

  /*
   * Helper function to create an edge property from an edge type.
   */
  private EdgeProperty createEdgeProperty(TezEdgeProperty edgeProp, Configuration conf)
      throws IOException {
    MRHelpers.translateMRConfToTez(conf);
    String keyClass = conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS);
    String valClass = conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS);
    String partitionerClassName = conf.get("mapred.partitioner.class");
    Map<String, String> partitionerConf;

    EdgeType edgeType = edgeProp.getEdgeType();
    switch (edgeType) {
    case BROADCAST_EDGE:
      UnorderedKVEdgeConfig et1Conf = UnorderedKVEdgeConfig
          .newBuilder(keyClass, valClass)
          .setFromConfiguration(conf)
          .setKeySerializationClass(TezBytesWritableSerialization.class.getName(), null)
          .setValueSerializationClass(TezBytesWritableSerialization.class.getName(), null)
          .build();
      return et1Conf.createDefaultBroadcastEdgeProperty();
    case CUSTOM_EDGE:
      assert partitionerClassName != null;
      partitionerConf = createPartitionerConf(partitionerClassName, conf);
      UnorderedPartitionedKVEdgeConfig et2Conf = UnorderedPartitionedKVEdgeConfig
          .newBuilder(keyClass, valClass, MRPartitioner.class.getName(), partitionerConf)
          .setFromConfiguration(conf)
          .setKeySerializationClass(TezBytesWritableSerialization.class.getName(), null)
          .setValueSerializationClass(TezBytesWritableSerialization.class.getName(), null)
          .build();
      EdgeManagerPluginDescriptor edgeDesc =
          EdgeManagerPluginDescriptor.create(CustomPartitionEdge.class.getName());
      CustomEdgeConfiguration edgeConf =
          new CustomEdgeConfiguration(edgeProp.getNumBuckets(), null);
      DataOutputBuffer dob = new DataOutputBuffer();
      edgeConf.write(dob);
      byte[] userPayload = dob.getData();
      edgeDesc.setUserPayload(UserPayload.create(ByteBuffer.wrap(userPayload)));
      return et2Conf.createDefaultCustomEdgeProperty(edgeDesc);
    case CUSTOM_SIMPLE_EDGE:
      assert partitionerClassName != null;
      partitionerConf = createPartitionerConf(partitionerClassName, conf);
      UnorderedPartitionedKVEdgeConfig et3Conf = UnorderedPartitionedKVEdgeConfig
          .newBuilder(keyClass, valClass, MRPartitioner.class.getName(), partitionerConf)
          .setFromConfiguration(conf)
          .setKeySerializationClass(TezBytesWritableSerialization.class.getName(), null)
          .setValueSerializationClass(TezBytesWritableSerialization.class.getName(), null)
          .build();
      return et3Conf.createDefaultEdgeProperty();
    case SIMPLE_EDGE:
    default:
      assert partitionerClassName != null;
      partitionerConf = createPartitionerConf(partitionerClassName, conf);
      OrderedPartitionedKVEdgeConfig et4Conf = OrderedPartitionedKVEdgeConfig
          .newBuilder(keyClass, valClass, MRPartitioner.class.getName(), partitionerConf)
          .setFromConfiguration(conf)
          .setKeySerializationClass(TezBytesWritableSerialization.class.getName(),
              TezBytesComparator.class.getName(), null)
          .setValueSerializationClass(TezBytesWritableSerialization.class.getName(), null)
          .build();
      return et4Conf.createDefaultEdgeProperty();
    }
  }

  /**
   * Utility method to create a stripped down configuration for the MR partitioner.
   *
   * @param partitionerClassName
   *          the real MR partitioner class name
   * @param baseConf
   *          a base configuration to extract relevant properties
   * @return
   */
  private Map<String, String> createPartitionerConf(String partitionerClassName,
      Configuration baseConf) {
    Map<String, String> partitionerConf = new HashMap<String, String>();
    partitionerConf.put("mapred.partitioner.class", partitionerClassName);
    if (baseConf.get("mapreduce.totalorderpartitioner.path") != null) {
      partitionerConf.put("mapreduce.totalorderpartitioner.path",
      baseConf.get("mapreduce.totalorderpartitioner.path"));
    }
    return partitionerConf;
  }

  /*
   * Helper to determine the size of the container requested
   * from yarn. Falls back to Map-reduce's map size if tez
   * container size isn't set.
   */
  public static Resource getContainerResource(Configuration conf) {
    int memory = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVETEZCONTAINERSIZE) > 0 ?
      HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVETEZCONTAINERSIZE) :
      conf.getInt(MRJobConfig.MAP_MEMORY_MB, MRJobConfig.DEFAULT_MAP_MEMORY_MB);
    int cpus = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVETEZCPUVCORES) > 0 ?
      HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVETEZCPUVCORES) :
      conf.getInt(MRJobConfig.MAP_CPU_VCORES, MRJobConfig.DEFAULT_MAP_CPU_VCORES);
    return Resource.newInstance(memory, cpus);
  }

  /*
   * Helper to setup default environment for a task in YARN.
   */
  private Map<String, String> getContainerEnvironment(Configuration conf, boolean isMap) {
    Map<String, String> environment = new HashMap<String, String>();
    MRHelpers.updateEnvBasedOnMRTaskEnv(conf, environment, isMap);
    return environment;
  }

  /*
   * Helper to determine what java options to use for the containers
   * Falls back to Map-reduces map java opts if no tez specific options
   * are set
   */
  private String getContainerJavaOpts(Configuration conf) {
    String javaOpts = HiveConf.getVar(conf, HiveConf.ConfVars.HIVETEZJAVAOPTS);

    String logLevel = HiveConf.getVar(conf, HiveConf.ConfVars.HIVETEZLOGLEVEL);
    List<String> logProps = Lists.newArrayList();
    TezUtils.addLog4jSystemProperties(logLevel, logProps);
    StringBuilder sb = new StringBuilder();
    for (String str : logProps) {
      sb.append(str).append(" ");
    }
    logLevel = sb.toString();

    if (HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVETEZCONTAINERSIZE) > 0) {
      if (javaOpts != null) {
        return javaOpts + " " + logLevel;
      } else  {
        return logLevel;
      }
    } else {
      if (javaOpts != null && !javaOpts.isEmpty()) {
        LOG.warn(HiveConf.ConfVars.HIVETEZJAVAOPTS + " will be ignored because "
                 + HiveConf.ConfVars.HIVETEZCONTAINERSIZE + " is not set!");
      }
      return logLevel + " " + MRHelpers.getJavaOptsForMRMapper(conf);
    }
  }

  private Vertex createVertex(JobConf conf, MergeJoinWork mergeJoinWork, LocalResource appJarLr,
      List<LocalResource> additionalLr, FileSystem fs, Path mrScratchDir, Context ctx,
      VertexType vertexType)
      throws Exception {
    Utilities.setMergeWork(conf, mergeJoinWork, mrScratchDir, false);
    if (mergeJoinWork.getMainWork() instanceof MapWork) {
      List<BaseWork> mapWorkList = mergeJoinWork.getBaseWorkList();
      MapWork mapWork = (MapWork) (mergeJoinWork.getMainWork());
      Vertex mergeVx =
          createVertex(conf, mapWork, appJarLr, additionalLr, fs, mrScratchDir, ctx, vertexType);

      conf.setClass("mapred.input.format.class", HiveInputFormat.class, InputFormat.class);
      // mapreduce.tez.input.initializer.serialize.event.payload should be set
      // to false when using this plug-in to avoid getting a serialized event at run-time.
      conf.setBoolean("mapreduce.tez.input.initializer.serialize.event.payload", false);
      for (int i = 0; i < mapWorkList.size(); i++) {

        mapWork = (MapWork) (mapWorkList.get(i));
        conf.set(TEZ_MERGE_CURRENT_MERGE_FILE_PREFIX, mapWork.getName());
        conf.set(Utilities.INPUT_NAME, mapWork.getName());
        LOG.info("Going through each work and adding MultiMRInput");
        mergeVx.addDataSource(mapWork.getName(),
            MultiMRInput.createConfigBuilder(conf, HiveInputFormat.class).build());
      }

      VertexManagerPluginDescriptor desc =
        VertexManagerPluginDescriptor.create(CustomPartitionVertex.class.getName());
      // the +1 to the size is because of the main work.
      CustomVertexConfiguration vertexConf =
          new CustomVertexConfiguration(mergeJoinWork.getMergeJoinOperator().getConf()
              .getNumBuckets(), vertexType, mergeJoinWork.getBigTableAlias(),
              mapWorkList.size() + 1);
      DataOutputBuffer dob = new DataOutputBuffer();
      vertexConf.write(dob);
      byte[] userPayload = dob.getData();
      desc.setUserPayload(UserPayload.create(ByteBuffer.wrap(userPayload)));
      mergeVx.setVertexManagerPlugin(desc);
      return mergeVx;
    } else {
      Vertex mergeVx =
          createVertex(conf, (ReduceWork) mergeJoinWork.getMainWork(), appJarLr, additionalLr, fs,
              mrScratchDir, ctx);
      return mergeVx;
    }
  }

  /*
   * Helper function to create Vertex from MapWork.
   */
  private Vertex createVertex(JobConf conf, MapWork mapWork,
      LocalResource appJarLr, List<LocalResource> additionalLr, FileSystem fs,
      Path mrScratchDir, Context ctx, VertexType vertexType)
      throws Exception {

    Path tezDir = getTezDir(mrScratchDir);

    // set up the operator plan
    Utilities.cacheMapWork(conf, mapWork, mrScratchDir);

    // create the directories FileSinkOperators need
    Utilities.createTmpDirs(conf, mapWork);

    // finally create the vertex
    Vertex map = null;

    // use tez to combine splits
    boolean groupSplitsInInputInitializer;

    DataSourceDescriptor dataSource;

    int numTasks = -1;
    @SuppressWarnings("rawtypes")
    Class inputFormatClass = conf.getClass("mapred.input.format.class",
        InputFormat.class);

    boolean vertexHasCustomInput = VertexType.isCustomInputType(vertexType);
    LOG.info("Vertex has custom input? " + vertexHasCustomInput);
    if (vertexHasCustomInput) {
      groupSplitsInInputInitializer = false;
      // grouping happens in execution phase. The input payload should not enable grouping here,
      // it will be enabled in the CustomVertex.
      inputFormatClass = HiveInputFormat.class;
      conf.setClass("mapred.input.format.class", HiveInputFormat.class, InputFormat.class);
      // mapreduce.tez.input.initializer.serialize.event.payload should be set to false when using
      // this plug-in to avoid getting a serialized event at run-time.
      conf.setBoolean("mapreduce.tez.input.initializer.serialize.event.payload", false);
    } else {
      // we'll set up tez to combine spits for us iff the input format
      // is HiveInputFormat
      if (inputFormatClass == HiveInputFormat.class) {
        groupSplitsInInputInitializer = true;
      } else {
        groupSplitsInInputInitializer = false;
      }
    }

    if (mapWork instanceof MergeFileWork) {
      Path outputPath = ((MergeFileWork) mapWork).getOutputDir();
      // prepare the tmp output directory. The output tmp directory should
      // exist before jobClose (before renaming after job completion)
      Path tempOutPath = Utilities.toTempPath(outputPath);
      try {
        if (!fs.exists(tempOutPath)) {
          fs.mkdirs(tempOutPath);
        }
      } catch (IOException e) {
        throw new RuntimeException(
            "Can't make path " + outputPath + " : " + e.getMessage(), e);
      }
    }

    // remember mapping of plan to input
    conf.set(Utilities.INPUT_NAME, mapWork.getName());
    if (HiveConf.getBoolVar(conf, ConfVars.HIVE_AM_SPLIT_GENERATION)
        && !mapWork.isUseOneNullRowInputFormat()) {

      // set up the operator plan. (before setting up splits on the AM)
      Utilities.setMapWork(conf, mapWork, mrScratchDir, false);

      // if we're generating the splits in the AM, we just need to set
      // the correct plugin.
      if (groupSplitsInInputInitializer) {
        // Not setting a payload, since the MRInput payload is the same and can be accessed.
        InputInitializerDescriptor descriptor = InputInitializerDescriptor.create(
            HiveSplitGenerator.class.getName());
        dataSource = MRInputLegacy.createConfigBuilder(conf, inputFormatClass).groupSplits(true)
            .setCustomInitializerDescriptor(descriptor).build();
      } else {
        // Not HiveInputFormat, or a custom VertexManager will take care of grouping splits
        if (vertexHasCustomInput) {
          dataSource =
              MultiMRInput.createConfigBuilder(conf, inputFormatClass).groupSplits(false).build();
        } else {
          dataSource =
              MRInputLegacy.createConfigBuilder(conf, inputFormatClass).groupSplits(false).build();
        }
      }
    } else {
      // Setup client side split generation.
      dataSource = MRInputHelpers.configureMRInputWithLegacySplitGeneration(conf, new Path(tezDir,
          "split_" + mapWork.getName().replaceAll(" ", "_")), true);
      numTasks = dataSource.getNumberOfShards();

      // set up the operator plan. (after generating splits - that changes configs)
      Utilities.setMapWork(conf, mapWork, mrScratchDir, false);
    }

    UserPayload serializedConf = TezUtils.createUserPayloadFromConf(conf);
    String procClassName = MapTezProcessor.class.getName();
    if (mapWork instanceof MergeFileWork) {
      procClassName = MergeFileTezProcessor.class.getName();
    }
    map = Vertex.create(mapWork.getName(), ProcessorDescriptor.create(procClassName)
        .setUserPayload(serializedConf), numTasks, getContainerResource(conf));

    map.setTaskEnvironment(getContainerEnvironment(conf, true));
    map.setTaskLaunchCmdOpts(getContainerJavaOpts(conf));

    assert mapWork.getAliasToWork().keySet().size() == 1;

    // Add the actual source input
    String alias = mapWork.getAliasToWork().keySet().iterator().next();
    map.addDataSource(alias, dataSource);

    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    localResources.put(getBaseName(appJarLr), appJarLr);
    for (LocalResource lr: additionalLr) {
      localResources.put(getBaseName(lr), lr);
    }

    map.addTaskLocalFiles(localResources);
    return map;
  }

  /*
   * Helper function to create JobConf for specific ReduceWork.
   */
  private JobConf initializeVertexConf(JobConf baseConf, Context context, ReduceWork reduceWork) {
    JobConf conf = new JobConf(baseConf);

    conf.set(Operator.CONTEXT_NAME_KEY, reduceWork.getName());

    // Is this required ?
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
    conf.set(Utilities.INPUT_NAME, reduceWork.getName());
    Utilities.setReduceWork(conf, reduceWork, mrScratchDir, false);

    // create the directories FileSinkOperators need
    Utilities.createTmpDirs(conf, reduceWork);

    // create the vertex
    Vertex reducer = Vertex.create(reduceWork.getName(),
        ProcessorDescriptor.create(ReduceTezProcessor.class.getName()).
        setUserPayload(TezUtils.createUserPayloadFromConf(conf)),
            reduceWork.isAutoReduceParallelism() ? reduceWork.getMaxReduceTasks() : reduceWork
                .getNumReduceTasks(), getContainerResource(conf));

    reducer.setTaskEnvironment(getContainerEnvironment(conf, false));
    reducer.setTaskLaunchCmdOpts(getContainerJavaOpts(conf));

    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    localResources.put(getBaseName(appJarLr), appJarLr);
    for (LocalResource lr: additionalLr) {
      localResources.put(getBaseName(lr), lr);
    }
    reducer.addTaskLocalFiles(localResources);

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
   * @param numContainers number of containers to pre-warm
   * @param localResources additional resources to pre-warm with
   * @return prewarm vertex to run
   */
  public PreWarmVertex createPreWarmVertex(TezConfiguration conf,
      int numContainers, Map<String, LocalResource> localResources) throws
      IOException, TezException {

    ProcessorDescriptor prewarmProcDescriptor = ProcessorDescriptor.create(HivePreWarmProcessor.class.getName());
    prewarmProcDescriptor.setUserPayload(TezUtils.createUserPayloadFromConf(conf));

    PreWarmVertex prewarmVertex = PreWarmVertex.create("prewarm", prewarmProcDescriptor, numContainers,getContainerResource(conf));

    Map<String, LocalResource> combinedResources = new HashMap<String, LocalResource>();

    if (localResources != null) {
      combinedResources.putAll(localResources);
    }

    prewarmVertex.addTaskLocalFiles(localResources);
    prewarmVertex.setTaskLaunchCmdOpts(getContainerJavaOpts(conf));
    prewarmVertex.setTaskEnvironment(getContainerEnvironment(conf, false));
    return prewarmVertex;
  }

  /**
   * @param conf
   * @return path to destination directory on hdfs
   * @throws LoginException if we are unable to figure user information
   * @throws IOException when any dfs operation fails.
   */
  @SuppressWarnings("deprecation")
  public Path getDefaultDestDir(Configuration conf) throws LoginException, IOException {
    UserGroupInformation ugi = Utils.getUGI();
    String userName = ugi.getShortUserName();
    String userPathStr = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_USER_INSTALL_DIR);
    Path userPath = new Path(userPathStr);
    FileSystem fs = userPath.getFileSystem(conf);

    String jarPathStr = userPathStr + "/" + userName;
    String hdfsDirPathStr = jarPathStr;
    Path hdfsDirPath = new Path(hdfsDirPathStr);

    try {
      FileStatus fstatus = fs.getFileStatus(hdfsDirPath);
      if (!fstatus.isDir()) {
        throw new IOException(ErrorMsg.INVALID_DIR.format(hdfsDirPath.toString()));
      }
    } catch (FileNotFoundException e) {
      // directory does not exist, create it
      fs.mkdirs(hdfsDirPath);
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

    addTempResources(conf, tmpResources, hdfsDirPathStr, LocalResourceType.FILE, getTempFilesFromConf(conf));
    addTempResources(conf, tmpResources, hdfsDirPathStr, LocalResourceType.ARCHIVE, getTempArchivesFromConf(conf));
    return tmpResources;
  }

  private static String[] getTempFilesFromConf(Configuration conf) {
    String addedFiles = Utilities.getResourceFiles(conf, SessionState.ResourceType.FILE);
    if (StringUtils.isNotBlank(addedFiles)) {
      HiveConf.setVar(conf, ConfVars.HIVEADDEDFILES, addedFiles);
    }
    String addedJars = Utilities.getResourceFiles(conf, SessionState.ResourceType.JAR);
    if (StringUtils.isNotBlank(addedJars)) {
      HiveConf.setVar(conf, ConfVars.HIVEADDEDJARS, addedJars);
    }
    String auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS);

    // need to localize the additional jars and files
    // we need the directory on hdfs to which we shall put all these files
    String allFiles = auxJars + "," + addedJars + "," + addedFiles;
    return allFiles.split(",");
  }

  private static String[] getTempArchivesFromConf(Configuration conf) {
    String addedArchives = Utilities.getResourceFiles(conf, SessionState.ResourceType.ARCHIVE);
    if (StringUtils.isNotBlank(addedArchives)) {
      HiveConf.setVar(conf, ConfVars.HIVEADDEDARCHIVES, addedArchives);
      return addedArchives.split(",");
    }
    return new String[0];
  }

  /**
   * Localizes files, archives and jars from a provided array of names.
   * @param hdfsDirPathStr Destination directory in HDFS.
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
    addTempResources(conf, tmpResources, hdfsDirPathStr, LocalResourceType.FILE, inputOutputJars);
    return tmpResources;
  }

  private void addTempResources(Configuration conf,
      List<LocalResource> tmpResources, String hdfsDirPathStr,
      LocalResourceType type,
      String[] files) throws IOException {
    for (String file : files) {
      if (!StringUtils.isNotBlank(file)) {
        continue;
      }
      Path hdfsFilePath = new Path(hdfsDirPathStr, getResourceBaseName(new Path(file)));
      LocalResource localResource = localizeResource(new Path(file),
          hdfsFilePath, type, conf);
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

  @SuppressWarnings("deprecation")
  public static FileStatus validateTargetDir(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
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
   * @param path - the string from which we try to determine the resource base name
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
   * @param type local resource type (File/Archive)
   * @param conf
   * @return localresource from tez localization.
   * @throws IOException when any file system related calls fails.
   */
  public LocalResource localizeResource(Path src, Path dest, LocalResourceType type, Configuration conf)
    throws IOException {
    FileSystem destFS = dest.getFileSystem(conf);

    if (src != null && checkPreExisting(src, dest, conf) == false) {
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
        long sleepInterval = HiveConf.getTimeVar(
            conf, HiveConf.ConfVars.HIVE_LOCALIZE_RESOURCE_WAIT_INTERVAL,
            TimeUnit.MILLISECONDS);
        LOG.info("Number of wait attempts: " + waitAttempts + ". Wait interval: "
            + sleepInterval);
        boolean found = false;
        for (int i = 0; i < waitAttempts; i++) {
          if (!checkPreExisting(src, dest, conf)) {
            try {
              Thread.sleep(sleepInterval);
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

    return createLocalResource(destFS, dest, type,
        LocalResourceVisibility.PRIVATE);
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

    JobConf conf = new JobConf(new TezConfiguration(hiveConf));

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
  public JobConf initializeVertexConf(JobConf conf, Context context, BaseWork work) {

    // simply dispatch the call to the right method for the actual (sub-) type of
    // BaseWork.
    if (work instanceof MapWork) {
      return initializeVertexConf(conf, context, (MapWork)work);
    } else if (work instanceof ReduceWork) {
      return initializeVertexConf(conf, context, (ReduceWork)work);
    } else if (work instanceof MergeJoinWork) {
      return initializeVertexConf(conf, context, (MergeJoinWork) work);
    } else {
      assert false;
      return null;
    }
  }

  private JobConf initializeVertexConf(JobConf conf, Context context, MergeJoinWork work) {
    if (work.getMainWork() instanceof MapWork) {
      return initializeVertexConf(conf, context, (MapWork) (work.getMainWork()));
    } else {
      return initializeVertexConf(conf, context, (ReduceWork) (work.getMainWork()));
    }
  }

  /**
   * Create a vertex from a given work object.
   *
   * @param conf JobConf to be used to this execution unit
   * @param work The instance of BaseWork representing the actual work to be performed
   * by this vertex.
   * @param scratchDir HDFS scratch dir for this execution unit.
   * @param appJarLr Local resource for hive-exec.
   * @param additionalLr
   * @param fileSystem FS corresponding to scratchDir and LocalResources
   * @param ctx This query's context
   * @return Vertex
   */
  @SuppressWarnings("deprecation")
  public Vertex createVertex(JobConf conf, BaseWork work,
      Path scratchDir, LocalResource appJarLr,
      List<LocalResource> additionalLr, FileSystem fileSystem, Context ctx, boolean hasChildren,
      TezWork tezWork, VertexType vertexType) throws Exception {

    Vertex v = null;
    // simply dispatch the call to the right method for the actual (sub-) type of
    // BaseWork.
    if (work instanceof MapWork) {
      v = createVertex(conf, (MapWork) work, appJarLr, additionalLr, fileSystem, scratchDir, ctx,
              vertexType);
    } else if (work instanceof ReduceWork) {
      v = createVertex(conf, (ReduceWork) work, appJarLr,
          additionalLr, fileSystem, scratchDir, ctx);
    } else if (work instanceof MergeJoinWork) {
      v = createVertex(conf, (MergeJoinWork) work, appJarLr, additionalLr, fileSystem, scratchDir,
              ctx, vertexType);
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
      v.addDataSink("out_"+work.getName(), new DataSinkDescriptor(
          OutputDescriptor.create(MROutput.class.getName())
          .setUserPayload(TezUtils.createUserPayloadFromConf(conf)), null, null));
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
      ugi = Utils.getUGI();
      userName = ugi.getShortUserName();
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

  private void setupAutoReducerParallelism(TezEdgeProperty edgeProp, Vertex v)
    throws IOException {
    if (edgeProp.isAutoReduce()) {
      Configuration pluginConf = new Configuration(false);
      VertexManagerPluginDescriptor desc =
          VertexManagerPluginDescriptor.create(ShuffleVertexManager.class.getName());
      pluginConf.setBoolean(
          ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL, true);
      pluginConf.setInt(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM,
          edgeProp.getMinReducer());
      pluginConf.setLong(
          ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE,
          edgeProp.getInputSizePerReducer());
      UserPayload payload = TezUtils.createUserPayloadFromConf(pluginConf);
      desc.setUserPayload(payload);
      v.setVertexManagerPlugin(desc);
    }
  }

  private DagUtils() {
    // don't instantiate
  }
}
