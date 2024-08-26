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
package org.apache.hadoop.hive.ql.exec.tez;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import javax.security.auth.login.LoginException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.tez.dag.api.OutputCommitterDescriptor;
import org.apache.tez.mapreduce.common.MRInputSplitDistributor;
import org.apache.tez.mapreduce.hadoop.InputSplitInfo;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductConfig;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
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
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.TezWork.VertexType;
import org.apache.hadoop.hive.ql.plan.UnionWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsCollectionContext;
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
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
import org.apache.tez.dag.api.Vertex.VertexExecutionContext;
import org.apache.tez.dag.api.VertexGroup;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.input.MultiMRInput;
import org.apache.tez.mapreduce.partition.MRPartitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.comparator.TezBytesComparator;
import org.apache.tez.runtime.library.common.serializer.TezBytesWritableSerialization;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.input.ConcatenatedMergedKeyValueInput;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductVertexManager;

/**
 * DagUtils. DagUtils is a collection of helper methods to convert
 * map and reduce work to tez vertices and edges. It handles configuration
 * objects, file localization and vertex/edge creation.
 */
public class DagUtils {

  public static final String TEZ_TMP_DIR_KEY = "_hive_tez_tmp_dir";
  private static final Logger LOG = LoggerFactory.getLogger(DagUtils.class.getName());
  private static final String TEZ_DIR = "_tez_scratch_dir";
  private static final DagUtils instance = new DagUtils(defaultCredentialSuppliers());
  // The merge file being currently processed.
  public static final String TEZ_MERGE_CURRENT_MERGE_FILE_PREFIX =
      "hive.tez.current.merge.file.prefix";
  // A comma separated list of work names used as prefix.
  public static final String TEZ_MERGE_WORK_FILE_PREFIXES = "hive.tez.merge.file.prefixes";
  private final List<DagCredentialSupplier> credentialSuppliers;
  /**
   * Notifiers to synchronize resource localization across threads. If one thread is localizing
   * a file, other threads can wait on the corresponding notifier object instead of just sleeping
   * before re-checking HDFS. This is used just to avoid unnecesary waits; HDFS check still needs
   * to be performed to make sure the resource is there and matches the expected file.
   */
  private final ConcurrentHashMap<String, Object> copyNotifiers = new ConcurrentHashMap<>();

  class CollectFileSinkUrisNodeProcessor implements SemanticNodeProcessor {

    private final Set<URI> uris;
    private final Set<TableDesc> tableDescs;

    public CollectFileSinkUrisNodeProcessor(Set<URI> uris, Set<TableDesc> tableDescs) {
      this.uris = uris;
      this.tableDescs = tableDescs;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      for (Node n : stack) {
        Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) n;
        OperatorDesc desc = op.getConf();
        if (desc instanceof FileSinkDesc) {
          FileSinkDesc fileSinkDesc = (FileSinkDesc) desc;
          tableDescs.add(fileSinkDesc.getTableInfo());

          Path dirName = fileSinkDesc.getDirName();
          if (dirName != null) {
            uris.add(dirName.toUri());
          }
          Path destPath = fileSinkDesc.getDestPath();
          if (destPath != null) {
            uris.add(destPath.toUri());
          }
        }
      }
      return null;
    }
  }

  private void addCollectFileSinkUrisRules(Map<SemanticRule, SemanticNodeProcessor> opRules, SemanticNodeProcessor np) {
    opRules.put(new RuleRegExp("R1", FileSinkOperator.getOperatorName() + ".*"), np);
  }

  private void collectFileSinkUris(List<Node> topNodes, Set<URI> uris, Set<TableDesc> tableDescs) {

    CollectFileSinkUrisNodeProcessor np = new CollectFileSinkUrisNodeProcessor(uris, tableDescs);

    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    addCollectFileSinkUrisRules(opRules, np);

    SemanticDispatcher disp = new DefaultRuleDispatcher(np, opRules, null);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

    try {
      ogw.startWalking(topNodes, null);
    } catch (SemanticException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Set up credentials for the base work on secure clusters
   */
  public void addCredentials(BaseWork work, DAG dag, JobConf conf) {
    Set<URI> fileSinkUris = new HashSet<URI>();
    Set<TableDesc> fileSinkTableDescs = new HashSet<TableDesc>();
    collectNeededFileSinkData(work, fileSinkUris, fileSinkTableDescs);

    if (work instanceof MapWork){
      Set<Path> paths = ((MapWork)work).getPathToAliases().keySet();
      if (!paths.isEmpty()) {
        Iterator<URI> pathIterator = Iterators.transform(paths.iterator(), new Function<Path, URI>() {
          @Override
          public URI apply(Path path) {
            return path.toUri();
          }
        });

        Set<URI> uris = new HashSet<URI>();
        Iterators.addAll(uris, pathIterator);

        if (LOG.isDebugEnabled()) {
          for (URI uri: uris) {
            LOG.debug("Marking MapWork input URI as needing credentials: " + uri);
          }
        }
        dag.addURIsForCredentials(uris);
      }
      getCredentialsFromSuppliers(work, fileSinkTableDescs, dag, conf);
    }
    getCredentialsForFileSinks(work, fileSinkUris, dag);
  }

  private void getCredentialsFromSuppliers(BaseWork work, Set<TableDesc> tables, DAG dag, JobConf conf) {
    if (!UserGroupInformation.isSecurityEnabled()){
      return;
    }
    for (DagCredentialSupplier supplier : credentialSuppliers) {
      Text alias = supplier.getTokenAlias();
      Token<?> t = dag.getCredentials().getToken(alias);
      if (t != null) {
        continue;
      }
      LOG.debug("Attempt to get token {} for work {}", alias, work.getName());
      t = supplier.obtainToken(work, tables, conf);
      if (t != null) {
        dag.getCredentials().addToken(alias, t);
      }
    }
  }

  private static List<DagCredentialSupplier> defaultCredentialSuppliers() {
    // Class names of credential providers that should be used when adding credentials to the dag.
    // Use plain strings instead of {@link Class#getName()} to avoid compile scope dependencies to other modules.
    List<String> supplierClassNames =
        Collections.singletonList("org.apache.hadoop.hive.kafka.KafkaDagCredentialSupplier");
    List<DagCredentialSupplier> dagSuppliers = new ArrayList<>();
    for (String s : supplierClassNames) {
      try {
        Class<? extends DagCredentialSupplier> c = Class.forName(s).asSubclass(DagCredentialSupplier.class);
        dagSuppliers.add(c.getConstructor().newInstance());
      } catch (ReflectiveOperationException e) {
        LOG.error("Failed to add credential supplier", e);
      }
    }
    return dagSuppliers;
  }

  private void collectNeededFileSinkData(BaseWork work, Set<URI> fileSinkUris, Set<TableDesc> fileSinkTableDescs) {
    List<Node> topNodes = getTopNodes(work);
    LOG.debug("Collecting file sink uris for {} topnodes: {}", work.getClass(), topNodes);
    collectFileSinkUris(topNodes, fileSinkUris, fileSinkTableDescs);
  }

  private void getCredentialsForFileSinks(BaseWork baseWork, Set<URI> fileSinkUris, DAG dag) {
    if (LOG.isDebugEnabled()) {
      for (URI fileSinkUri : fileSinkUris) {
        LOG.debug("Marking {} output URI as needing credentials (filesink): {}",
            baseWork.getClass(), fileSinkUri);
      }
    }
    dag.addURIsForCredentials(fileSinkUris);
  }

  private List<Node> getTopNodes(BaseWork work) {
    List<Node> topNodes = new ArrayList<Node>();

    if (work instanceof MapWork) {
      Map<String, Operator<? extends OperatorDesc>> aliasToWork = ((MapWork) work).getAliasToWork();
      for (Operator<? extends OperatorDesc> operator : aliasToWork.values()) {
        topNodes.add(operator);
      }
    } else if (work instanceof ReduceWork) {
      topNodes.add(((ReduceWork) work).getReducer());
    } else if (work instanceof MergeJoinWork) {
      for (Operator<? extends OperatorDesc> operator : ((MergeJoinWork) work)
          .getAllRootOperators()) {
        topNodes.add(operator);
      }
    } else if (work instanceof UnionWork) {
      for (Operator<? extends OperatorDesc> operator : ((UnionWork) work).getAllRootOperators()) {
        topNodes.add(operator);
      }
    }

    return topNodes;
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
      HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPRED_MAX_SPLIT_SIZE,
          mapWork.getMaxSplitSize().longValue());
    }

    if (mapWork.getMinSplitSize() != null) {
      HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPRED_MIN_SPLIT_SIZE,
          mapWork.getMinSplitSize().longValue());
    }

    if (mapWork.getMinSplitSizePerNode() != null) {
      HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPRED_MIN_SPLIT_SIZE_PER_NODE,
          mapWork.getMinSplitSizePerNode().longValue());
    }

    if (mapWork.getMinSplitSizePerRack() != null) {
      HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPRED_MIN_SPLIT_SIZE_PER_RACK,
          mapWork.getMinSplitSizePerRack().longValue());
    }

    Utilities.setInputAttributes(conf, mapWork);

    String inpFormat = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_TEZ_INPUT_FORMAT);

    if (mapWork.isUseBucketizedHiveInputFormat()) {
      inpFormat = BucketizedHiveInputFormat.class.getName();
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
      // This mapper class is used for serialization/deserialization of merge
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
      TezEdgeProperty edgeProp, BaseWork work, TezWork tezWork)
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
      CustomVertexConfiguration vertexConf
              = new CustomVertexConfiguration(numBuckets, tezWork.getVertexType(work));
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

    case ONE_TO_ONE_EDGE:
      mergeInputClass = ConcatenatedMergedKeyValueInput.class;
      break;

    case XPROD_EDGE:
      mergeInputClass = ConcatenatedMergedKeyValueInput.class;
      break;

    case SIMPLE_EDGE:
      setupAutoReducerParallelism(edgeProp, w);
      // fall through

    default:
      mergeInputClass = TezMergedLogicalInput.class;
      break;
    }

    return GroupInputEdge.create(group, w, createEdgeProperty(w, edgeProp, vConf, work, tezWork),
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
      BaseWork work, TezWork tezWork)
    throws IOException {

    switch(edgeProp.getEdgeType()) {
    case CUSTOM_EDGE: {
      int numBuckets = edgeProp.getNumBuckets();
      CustomVertexConfiguration vertexConf =
              new CustomVertexConfiguration(numBuckets, tezWork.getVertexType(work));
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
    case XPROD_EDGE:
      break;

    case SIMPLE_EDGE: {
      setupAutoReducerParallelism(edgeProp, w);
      break;
    }
    case CUSTOM_SIMPLE_EDGE: {
      setupQuickStart(edgeProp, w);
      break;
    }

    default:
      // nothing
    }

    return Edge.create(v, w, createEdgeProperty(w, edgeProp, vConf, work, tezWork));
  }

  /*
   * Helper function to create an edge property from an edge type.
   */
  private EdgeProperty createEdgeProperty(Vertex w, TezEdgeProperty edgeProp,
                                          Configuration conf, BaseWork work, TezWork tezWork)
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
      UnorderedPartitionedKVEdgeConfig.Builder et3Conf = UnorderedPartitionedKVEdgeConfig
          .newBuilder(keyClass, valClass, MRPartitioner.class.getName(), partitionerConf)
          .setFromConfiguration(conf)
          .setKeySerializationClass(TezBytesWritableSerialization.class.getName(), null)
          .setValueSerializationClass(TezBytesWritableSerialization.class.getName(), null);
      if (edgeProp.getBufferSize() != null) {
        et3Conf.setAdditionalConfiguration(
            TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB,
            edgeProp.getBufferSize().toString());
      }
      return et3Conf.build().createDefaultEdgeProperty();
    case ONE_TO_ONE_EDGE:
      UnorderedKVEdgeConfig et4Conf = UnorderedKVEdgeConfig
          .newBuilder(keyClass, valClass)
          .setFromConfiguration(conf)
          .setKeySerializationClass(TezBytesWritableSerialization.class.getName(), null)
          .setValueSerializationClass(TezBytesWritableSerialization.class.getName(), null)
          .build();
      return et4Conf.createDefaultOneToOneEdgeProperty();
    case XPROD_EDGE:
      EdgeManagerPluginDescriptor edgeManagerDescriptor =
        EdgeManagerPluginDescriptor.create(CartesianProductEdgeManager.class.getName());
      List<String> crossProductSources = new ArrayList<>();
      for (BaseWork parentWork : tezWork.getParents(work)) {
        if (EdgeType.XPROD_EDGE == tezWork.getEdgeType(parentWork, work)) {
          crossProductSources.add(parentWork.getName());
        }
      }
      CartesianProductConfig cpConfig = new CartesianProductConfig(crossProductSources);
      edgeManagerDescriptor.setUserPayload(cpConfig.toUserPayload(new TezConfiguration(conf)));
      UnorderedPartitionedKVEdgeConfig cpEdgeConf =
        UnorderedPartitionedKVEdgeConfig.newBuilder(keyClass, valClass,
          ValueHashPartitioner.class.getName())
            .setFromConfiguration(conf)
            .setKeySerializationClass(TezBytesWritableSerialization.class.getName(), null)
            .setValueSerializationClass(TezBytesWritableSerialization.class.getName(), null)
            .build();
      return cpEdgeConf.createDefaultCustomEdgeProperty(edgeManagerDescriptor);
    case SIMPLE_EDGE:
      // fallthrough
    default:
      assert partitionerClassName != null;
      partitionerConf = createPartitionerConf(partitionerClassName, conf);
      OrderedPartitionedKVEdgeConfig et5Conf = OrderedPartitionedKVEdgeConfig
          .newBuilder(keyClass, valClass, MRPartitioner.class.getName(), partitionerConf)
          .setFromConfiguration(conf)
          .setKeySerializationClass(TezBytesWritableSerialization.class.getName(),
              TezBytesComparator.class.getName(), null)
          .setValueSerializationClass(TezBytesWritableSerialization.class.getName(), null)
          .build();
      return et5Conf.createDefaultEdgeProperty();
    }
  }

  public static class ValueHashPartitioner implements Partitioner {

    @Override
    public int getPartition(Object key, Object value, int numPartitions) {
      return (value.hashCode() & 2147483647) % numPartitions;
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
    int memorySizeMb = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_TEZ_CONTAINER_SIZE);
    if (memorySizeMb <= 0) {
      LOG.warn("No Tez container size specified by {}. Falling back to MapReduce container MB {}",
          HiveConf.ConfVars.HIVE_TEZ_CONTAINER_SIZE,  MRJobConfig.MAP_MEMORY_MB);
      memorySizeMb = conf.getInt(MRJobConfig.MAP_MEMORY_MB, MRJobConfig.DEFAULT_MAP_MEMORY_MB);
      // When config is explicitly set to "-1" defaultValue does not work!
      if (memorySizeMb <= 0) {
        LOG.warn("Falling back to default container MB {}", MRJobConfig.DEFAULT_MAP_MEMORY_MB);
        memorySizeMb = MRJobConfig.DEFAULT_MAP_MEMORY_MB;
      }
    }
    int cpuCores = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_TEZ_CPU_VCORES);
    if (cpuCores <= 0) {
      LOG.warn("No Tez VCore size specified by {}. Falling back to MapReduce container VCores {}",
          HiveConf.ConfVars.HIVE_TEZ_CPU_VCORES,  MRJobConfig.MAP_CPU_VCORES);
      cpuCores = conf.getInt(MRJobConfig.MAP_CPU_VCORES, MRJobConfig.DEFAULT_MAP_CPU_VCORES);
      if (cpuCores <= 0) {
        LOG.warn("Falling back to default container VCores {}", MRJobConfig.DEFAULT_MAP_CPU_VCORES);
        cpuCores = MRJobConfig.DEFAULT_MAP_CPU_VCORES;
      }
    }
    Resource resource = Resource.newInstance(memorySizeMb, cpuCores);
    LOG.debug("Tez container resource: {}", resource);
    return resource;
  }

  /*
   * Helper to setup default environment for a task in YARN.
   */
  @VisibleForTesting
  Map<String, String> getContainerEnvironment(Configuration conf, boolean isMap) {
    Map<String, String> environment = new HashMap<String, String>();
    MRHelpers.updateEnvBasedOnMRTaskEnv(conf, environment, isMap);
    return environment;
  }

  /*
   * Helper to determine what java options to use for the containers
   * Falls back to Map-reduces map java opts if no tez specific options
   * are set
   */
  private static String getContainerJavaOpts(Configuration conf) {
    String javaOpts = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_TEZ_JAVA_OPTS);

    String logLevel = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_TEZ_LOG_LEVEL);
    List<String> logProps = Lists.newArrayList();
    TezUtils.addLog4jSystemProperties(logLevel, logProps);
    StringBuilder sb = new StringBuilder();
    for (String str : logProps) {
      sb.append(str).append(" ");
    }
    logLevel = sb.toString();

    String finalOpts = null;
    if (HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_TEZ_CONTAINER_SIZE) > 0) {
      finalOpts = Strings.nullToEmpty(javaOpts) + " " + logLevel;
    } else {
      if (javaOpts != null && !javaOpts.isEmpty()) {
        LOG.warn(HiveConf.ConfVars.HIVE_TEZ_JAVA_OPTS + " will be ignored because "
                 + HiveConf.ConfVars.HIVE_TEZ_CONTAINER_SIZE + " is not set!");
      }
      finalOpts = logLevel + " " + MRHelpers.getJavaOptsForMRMapper(conf);
    }
    LOG.debug("Tez container final opts: {}", finalOpts);
    return finalOpts;
  }

  private Vertex createVertexFromMergeWork(JobConf conf, MergeJoinWork mergeJoinWork,
      Path mrScratchDir, VertexType vertexType) throws Exception {
    Utilities.setMergeWork(conf, mergeJoinWork, mrScratchDir, false);
    if (mergeJoinWork.getMainWork() instanceof MapWork) {
      List<BaseWork> mapWorkList = mergeJoinWork.getBaseWorkList();
      MapWork mapWork = (MapWork) (mergeJoinWork.getMainWork());
      Vertex mergeVx = createVertexFromMapWork(
          conf, mapWork, mrScratchDir, vertexType);

      Class<?> inputFormatClass = conf.getClass("mapred.input.format.class",
              HiveInputFormat.class);
      if (inputFormatClass != BucketizedHiveInputFormat.class &&
              inputFormatClass != HiveInputFormat.class) {
        // As of now only these two formats are supported.
        inputFormatClass = HiveInputFormat.class;
      }
      conf.setClass("mapred.input.format.class", inputFormatClass, InputFormat.class);
      // mapreduce.tez.input.initializer.serialize.event.payload should be set
      // to false when using this plug-in to avoid getting a serialized event at run-time.
      conf.setBoolean("mapreduce.tez.input.initializer.serialize.event.payload", false);
      for (int i = 0; i < mapWorkList.size(); i++) {

        mapWork = (MapWork) (mapWorkList.get(i));
        conf.set(TEZ_MERGE_CURRENT_MERGE_FILE_PREFIX, mapWork.getName());
        conf.set(Utilities.INPUT_NAME, mapWork.getName());
        LOG.info("Going through each work and adding MultiMRInput");
        mergeVx.addDataSource(mapWork.getName(),
            MultiMRInput.createConfigBuilder(conf, inputFormatClass).build());
      }

      // To be populated for SMB joins only for all the small tables
      Map<String, Integer> inputToBucketMap = new HashMap<>();
      if (mergeJoinWork.getMergeJoinOperator().getParentOperators().size() == 1
              && mergeJoinWork.getMergeJoinOperator().getOpTraits() != null) {
        // This is an SMB join.
        for (BaseWork work : mapWorkList) {
          MapWork mw = (MapWork) work;
          Map<String, Operator<?>> aliasToWork = mw.getAliasToWork();
          Preconditions.checkState(aliasToWork.size() == 1,
                  "More than 1 alias in SMB mapwork");
          inputToBucketMap.put(mw.getName(), mw.getWorks().get(0).getOpTraits().getNumBuckets());
        }
      }
      VertexManagerPluginDescriptor desc =
        VertexManagerPluginDescriptor.create(CustomPartitionVertex.class.getName());
      // the +1 to the size is because of the main work.
      CustomVertexConfiguration vertexConf =
          new CustomVertexConfiguration(mergeJoinWork.getMergeJoinOperator().getConf()
              .getNumBuckets(), vertexType, mergeJoinWork.getBigTableAlias(),
              mapWorkList.size() + 1, inputToBucketMap);
      DataOutputBuffer dob = new DataOutputBuffer();
      vertexConf.write(dob);
      byte[] userPayload = dob.getData();
      desc.setUserPayload(UserPayload.create(ByteBuffer.wrap(userPayload)));
      mergeVx.setVertexManagerPlugin(desc);
      return mergeVx;
    } else {
      return createVertexFromReduceWork(conf,
          (ReduceWork) mergeJoinWork.getMainWork(), mrScratchDir);
    }
  }

  /*
   * Helper function to create Vertex from MapWork.
   */
  private Vertex createVertexFromMapWork(JobConf conf, MapWork mapWork, Path mrScratchDir,
      VertexType vertexType) throws Exception {

    // set up the operator plan
    Utilities.cacheMapWork(conf, mapWork, mrScratchDir);

    // create the directories FileSinkOperators need
    Utilities.createTmpDirs(conf, mapWork);

    // finally, create the vertex
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
      if (inputFormatClass != BucketizedHiveInputFormat.class &&
              inputFormatClass != HiveInputFormat.class) {
        // As of now only these two formats are supported.
        inputFormatClass = HiveInputFormat.class;
      }
      conf.setClass("mapred.input.format.class", inputFormatClass, InputFormat.class);
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
        FileSystem tmpOutFS = tempOutPath.getFileSystem(conf);
        if (!tmpOutFS.exists(tempOutPath)) {
          tmpOutFS.mkdirs(tempOutPath);
        }
      } catch (IOException e) {
        throw new RuntimeException(
            "Can't make path " + outputPath + " : " + e.getMessage(), e);
      }
    }

    // remember mapping of plan to input
    conf.set(Utilities.INPUT_NAME, mapWork.getName());
    if (HiveConf.getBoolVar(conf, ConfVars.HIVE_AM_SPLIT_GENERATION)) {

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
        if (vertexHasCustomInput && vertexType == VertexType.MULTI_INPUT_UNINITIALIZED_EDGES) {
          // SMB Join.
          dataSource =
              MultiMRInput.createConfigBuilder(conf, inputFormatClass).groupSplits(false).build();
        } else {
          dataSource =
              MRInputLegacy.createConfigBuilder(conf, inputFormatClass).groupSplits(false).build();
        }
      }
    } else {
      // Setup client side split generation.

      // we need to set this, because with HS2 and client side split
      // generation we end up not finding the map work. This is
      // because of thread local madness (tez split generation is
      // multithreaded - HS2 plan cache uses thread locals). Setting
      // VECTOR_MODE/USE_VECTORIZED_INPUT_FILE_FORMAT causes the split gen code to use the conf instead
      // of the map work.
      conf.setBoolean(Utilities.VECTOR_MODE, mapWork.getVectorMode());
      conf.setBoolean(Utilities.USE_VECTORIZED_INPUT_FILE_FORMAT, mapWork.getUseVectorizedInputFileFormat());

      InputSplitInfo inputSplitInfo = MRInputHelpers.generateInputSplitsToMem(conf, false, 0);
      InputInitializerDescriptor descriptor = InputInitializerDescriptor.create(MRInputSplitDistributor.class.getName());
      InputDescriptor inputDescriptor = InputDescriptor.create(MRInputLegacy.class.getName())
              .setUserPayload(UserPayload
                      .create(MRRuntimeProtos.MRInputUserPayloadProto.newBuilder()
                              .setConfigurationBytes(TezUtils.createByteStringFromConf(conf))
                              .setSplits(inputSplitInfo.getSplitsProto()).build().toByteString()
                              .asReadOnlyByteBuffer()));

      dataSource = DataSourceDescriptor.create(inputDescriptor, descriptor, null);
      numTasks = inputSplitInfo.getNumTasks();

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

    assert mapWork.getAliasToWork().keySet().size() == 1;

    // Add the actual source input
    String alias = mapWork.getAliasToWork().keySet().iterator().next();
    map.addDataSource(alias, dataSource);
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
    // HIVE-23354 enforces that MR speculative execution is disabled
    conf.setBoolean(org.apache.hadoop.mapreduce.MRJobConfig.REDUCE_SPECULATIVE, false);
    conf.setBoolean(org.apache.hadoop.mapreduce.MRJobConfig.MAP_SPECULATIVE, false);
    return conf;
  }

  private VertexExecutionContext createVertexExecutionContext(BaseWork work) {
    VertexExecutionContext vertexExecutionContext = VertexExecutionContext.createExecuteInContainers(true);
    if (work.getLlapMode()) {
      vertexExecutionContext = VertexExecutionContext
          .create(TezSessionState.LLAP_SERVICE, TezSessionState.LLAP_SERVICE,
              TezSessionState.LLAP_SERVICE);
    }
    if (work.getUberMode()) {
      vertexExecutionContext = VertexExecutionContext.createExecuteInAm(true);
    }
    return vertexExecutionContext;
  }

  /*
   * Helper function to create Vertex for given ReduceWork.
   */
  private Vertex createVertexFromReduceWork(JobConf conf, ReduceWork reduceWork, Path mrScratchDir)
          throws Exception {

    // set up operator plan
    conf.set(Utilities.INPUT_NAME, reduceWork.getName());
    Utilities.setReduceWork(conf, reduceWork, mrScratchDir, false);

    // create the directories FileSinkOperators need
    Utilities.createTmpDirs(conf, reduceWork);

    // create the vertex
    Vertex reducer = Vertex.create(reduceWork.getName(),
        ProcessorDescriptor.create(ReduceTezProcessor.class.getName()).
            setUserPayload(TezUtils.createUserPayloadFromConf(conf)),
        reduceWork.isAutoReduceParallelism() ?
            reduceWork.getMaxReduceTasks() :
            reduceWork.getNumReduceTasks(), getContainerResource(conf));

    reducer.setTaskEnvironment(getContainerEnvironment(conf, false));
    return reducer;
  }

  public static Map<String, LocalResource> createTezLrMap(
      LocalResource appJarLr, Collection<LocalResource> additionalLr) {
    // Note: interestingly this would exclude LLAP app jars that the session adds for LLAP case.
    //       Of course, it doesn't matter because vertices run ON LLAP and have those jars, and
    //       moreover we anyway don't localize jars for the vertices on LLAP; but in theory
    //       this is still crappy code that assumes there's one and only app jar.
    Map<String, LocalResource> localResources = new HashMap<>();
    if (appJarLr != null) {
      localResources.put(getBaseName(appJarLr), appJarLr);
    }
    if (additionalLr != null) {
      for (LocalResource lr: additionalLr) {
        localResources.put(getBaseName(lr), lr);
      }
    }
    return localResources;
  }

  /*
   * Helper method to create a yarn local resource.
   */
  private LocalResource createLocalResource(FileSystem remoteFs, Path file,
      LocalResourceType type, LocalResourceVisibility visibility) throws IOException {

    final FileStatus fstat = remoteFs.getFileStatus(file);

    URL resourceURL = ConverterUtils.getYarnUrlFromPath(file);
    long resourceSize = fstat.getLen();
    long resourceModificationTime = fstat.getModificationTime();
    LOG.info("Resource modification time: " + resourceModificationTime + " for " + file);

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
   * @return List&lt;LocalResource&gt; local resources to add to execution
   * @throws IOException when hdfs operation fails
   * @throws LoginException when getDefaultDestDir fails with the same exception
   */
  public List<LocalResource> localizeTempFilesFromConf(
      String hdfsDirPathStr, Configuration conf) throws IOException, LoginException {
    List<LocalResource> tmpResources = new ArrayList<LocalResource>();

    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ADD_FILES_USE_HDFS_LOCATION)) {
      // reference HDFS based resource directly, to use distribute cache efficiently.
      addHdfsResource(conf, tmpResources, LocalResourceType.FILE, getHdfsTempFilesFromConf(conf));
      // local resources are session based.
      tmpResources.addAll(
          addTempResources(conf, hdfsDirPathStr, LocalResourceType.FILE,
              getLocalTempFilesFromConf(conf), null).values()
      );
    } else {
      // all resources including HDFS are session based.
      tmpResources.addAll(
          addTempResources(conf, hdfsDirPathStr, LocalResourceType.FILE,
              getTempFilesFromConf(conf), null).values()
      );
    }

    tmpResources.addAll(
        addTempResources(conf, hdfsDirPathStr, LocalResourceType.ARCHIVE,
            getTempArchivesFromConf(conf), null).values()
    );
    return tmpResources;
  }

  private void addHdfsResource(Configuration conf, List<LocalResource> tmpResources,
                               LocalResourceType type, String[] files) throws IOException {
    for (String file: files) {
      if (StringUtils.isNotBlank(file)) {
        Path dest = new Path(file);
        FileSystem destFS = dest.getFileSystem(conf);
        LocalResource localResource = createLocalResource(destFS, dest, type,
            LocalResourceVisibility.PRIVATE);
        tmpResources.add(localResource);
      }
    }
  }

  private static String[] getHdfsTempFilesFromConf(Configuration conf) {
    String addedFiles = Utilities.getHdfsResourceFiles(conf, SessionState.ResourceType.FILE);
    String addedJars = Utilities.getHdfsResourceFiles(conf, SessionState.ResourceType.JAR);
    String allFiles = addedJars + "," + addedFiles;
    return allFiles.split(",");
  }

  private static String[] getLocalTempFilesFromConf(Configuration conf) {
    String addedFiles = Utilities.getLocalResourceFiles(conf, SessionState.ResourceType.FILE);
    String addedJars = Utilities.getLocalResourceFiles(conf, SessionState.ResourceType.JAR);
    String auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_AUX_JARS);
    String reloadableAuxJars = SessionState.get() == null ? null : SessionState.get().getReloadableAuxJars();
    String allFiles =
        HiveStringUtils.joinIgnoringEmpty(new String[]{auxJars, reloadableAuxJars, addedJars, addedFiles}, ',');
    return allFiles.split(",");
  }

  public static String[] getTempFilesFromConf(Configuration conf) {
    if (conf == null) {
      return new String[0]; // In tests.
    }
    String addedFiles = Utilities.getResourceFiles(conf, SessionState.ResourceType.FILE);
    if (StringUtils.isNotBlank(addedFiles)) {
      HiveConf.setVar(conf, ConfVars.HIVE_ADDED_FILES, addedFiles);
    }
    String addedJars = Utilities.getResourceFiles(conf, SessionState.ResourceType.JAR);
    if (StringUtils.isNotBlank(addedJars)) {
      HiveConf.setVar(conf, ConfVars.HIVE_ADDED_JARS, addedJars);
    }
    String auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_AUX_JARS);
    String reloadableAuxJars = SessionState.get() == null ? null : SessionState.get().getReloadableAuxJars();

    // need to localize the additional jars and files
    // we need the directory on hdfs to which we shall put all these files
    String allFiles =
        HiveStringUtils.joinIgnoringEmpty(new String[]{auxJars, reloadableAuxJars, addedJars, addedFiles}, ',');
    return allFiles.split(",");
  }

  private static String[] getTempArchivesFromConf(Configuration conf) {
    String addedArchives = Utilities.getResourceFiles(conf, SessionState.ResourceType.ARCHIVE);
    if (StringUtils.isNotBlank(addedArchives)) {
      HiveConf.setVar(conf, ConfVars.HIVE_ADDED_ARCHIVES, addedArchives);
      return addedArchives.split(",");
    }
    return new String[0];
  }

  /**
   * Localizes files, archives and jars from a provided array of names.
   * @param hdfsDirPathStr Destination directory in HDFS.
   * @param conf Configuration.
   * @param inputOutputJars The file names to localize.
   * @return Map&lt;String, LocalResource&gt; (srcPath, local resources) to add to execution
   * @throws IOException when hdfs operation fails.
   */
  public Map<String, LocalResource> localizeTempFiles(String hdfsDirPathStr, Configuration conf,
      String[] inputOutputJars, String[] skipJars) throws IOException {
    if (inputOutputJars == null) {
      return null;
    }
    return addTempResources(conf, hdfsDirPathStr, LocalResourceType.FILE, inputOutputJars, skipJars);
  }

  private Map<String, LocalResource> addTempResources(Configuration conf, String hdfsDirPathStr,
      LocalResourceType type, String[] files, String[] skipFiles) throws IOException {
    HashSet<Path> skipFileSet = null;
    Map<String, LocalResource> tmpResourcesMap = new HashMap<>();
    if (skipFiles != null) {
      skipFileSet = new HashSet<>();
      for (String skipFile : skipFiles) {
        if (StringUtils.isBlank(skipFile)) {
          continue;
        }
        skipFileSet.add(new Path(skipFile));
      }
    }
    for (String file : files) {
      if (!StringUtils.isNotBlank(file)) {
        continue;
      }
      Path path = new Path(file);
      if (skipFileSet != null && skipFileSet.contains(path)) {
        LOG.info("Skipping vertex resource " + file + " that already exists in the session");
        continue;
      }
      path = FileUtils.resolveSymlinks(path, conf);
      Path hdfsFilePath = new Path(hdfsDirPathStr, getResourceBaseName(path));
      LocalResource localResource = localizeResource(path, hdfsFilePath, type, conf);
      tmpResourcesMap.put(file, localResource);
    }
    return tmpResourcesMap;
  }

  public FileStatus getHiveJarDirectory(Configuration conf) throws IOException, LoginException {
    FileStatus fstatus = null;
    String hdfsDirPathStr = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_JAR_DIRECTORY, (String)null);
    if (hdfsDirPathStr != null) {
      LOG.info("Hive jar directory is " + hdfsDirPathStr);
      fstatus = validateTargetDir(new Path(hdfsDirPathStr), conf);
    }

    if (fstatus == null) {
      Path destDir = getDefaultDestDir(conf);
      LOG.info("Jar dir is null / directory doesn't exist. Choosing HIVE_INSTALL_DIR - " + destDir);
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
  public String getExecJarPathLocal(Configuration configuration) {
    try {
      // returns the location on disc of the jar of this class.
      String uri = DagUtils.class.getProtectionDomain().getCodeSource().getLocation().toURI().toString();
      if (uri.endsWith(".jar")) {
        return uri;
      }
    } catch (Exception ignored) {}
    //Fall back to hive config, if the uri could not get, or it does not point to a .jar file
    String jar = configuration.get(ConfVars.HIVE_JAR.varname);
    if (!StringUtils.isBlank(jar)) {
      return jar;
    }
    throw new RuntimeException("Could not get hive-exec local path");
  }

  /**
   * Testing related; creates an empty archive to served being localized as hive-exec
   */
  private URI createEmptyArchive() {
    try {
      File outputJar = new File(System.getProperty("build.test.dir"), "empty.jar");
      ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(outputJar));
      zos.close();
      return outputJar.toURI();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /*
   * Helper function to retrieve the basename of a local resource
   */
  public static String getBaseName(LocalResource lr) {
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
  private boolean checkPreExisting(FileSystem sourceFS, Path src, Path dest, Configuration conf)
    throws IOException {
    FileSystem destFS = dest.getFileSystem(conf);
    FileStatus destStatus = FileUtils.getFileStatusOrNull(destFS, dest);
    if (destStatus != null) {
      return (sourceFS.getFileStatus(src).getLen() == destStatus.getLen());
    }
    return false;
  }

  /**
   * Localizes a resources. Should be thread-safe.
   * @param src path to the source for the resource
   * @param dest path in hdfs for the resource
   * @param type local resource type (File/Archive)
   * @param conf
   * @return localresource from tez localization.
   * @throws IOException when any file system related calls fails.
   */
  public LocalResource localizeResource(
      Path src, Path dest, LocalResourceType type, Configuration conf) throws IOException {
    FileSystem destFS = dest.getFileSystem(conf);
    FileSystem srcFs;
    if (src.toUri().getScheme() != null) {
      srcFs = src.getFileSystem(conf);
    } else {
      srcFs = FileSystem.getLocal(conf);
    }

    if (!checkPreExisting(srcFs, src, dest, conf)) {
      // copy the src to the destination and create local resource.
      // do not overwrite.
      String srcStr = src.toString();
      LOG.info("Localizing resource because it does not exist: " + srcStr + " to dest: " + dest);
      Object notifierNew = new Object(),
          notifierOld = copyNotifiers.putIfAbsent(srcStr, notifierNew),
          notifier = (notifierOld == null) ? notifierNew : notifierOld;
      // To avoid timing issues with notifications (and given that HDFS check is anyway the
      // authoritative one), don't wait infinitely for the notifier, just wait a little bit
      // and check HDFS before and after.
      if (notifierOld != null
          && checkOrWaitForTheFile(srcFs, src, dest, conf, notifierOld, 1, 150, false)) {
        return createLocalResource(destFS, dest, type, LocalResourceVisibility.PRIVATE);
      }
      try {
        // FileUtil.copy takes care of copy from local filesystem internally.
        FileUtil.copy(srcFs, src, destFS, dest, false, false, conf);
        synchronized (notifier) {
          notifier.notifyAll(); // Notify if we have successfully copied the file.
        }
        copyNotifiers.remove(srcStr, notifier);
      } catch (IOException e) {
        if ("Exception while contacting value generator".equals(e.getMessage())) {
          // HADOOP-13155, fixed version: 2.8.0, 3.0.0-alpha1
          throw new IOException("copyFromLocalFile failed due to HDFS KMS failure", e);
        }

        LOG.info("Looks like another thread or process is writing the same file");
        int waitAttempts = HiveConf.getIntVar(
            conf, ConfVars.HIVE_LOCALIZE_RESOURCE_NUM_WAIT_ATTEMPTS);
        long sleepInterval = HiveConf.getTimeVar(
            conf, HiveConf.ConfVars.HIVE_LOCALIZE_RESOURCE_WAIT_INTERVAL, TimeUnit.MILLISECONDS);
        // Only log on the first wait, and check after wait on the last iteration.
        if (!checkOrWaitForTheFile(
            srcFs, src, dest, conf, notifierOld, waitAttempts, sleepInterval, true)) {
          LOG.error("Could not find the jar that was being uploaded: src = {}, dest = {}, type = {}", src, dest, type);
          throw new IOException("Could not find jar while attempting to localize resource. Previous writer may have " +
              "failed to write " + dest + ". Failing because I am unlikely to write too. Refer to exception for more " +
              "troubleshooting details.", e);
        }
      } finally {
        if (notifier == notifierNew) {
          copyNotifiers.remove(srcStr, notifierNew);
        }
      }
    }
    return createLocalResource(destFS, dest, type, LocalResourceVisibility.PRIVATE);
  }

  public boolean checkOrWaitForTheFile(FileSystem srcFs, Path src, Path dest, Configuration conf,
      Object notifier, int waitAttempts, long sleepInterval, boolean doLog) throws IOException {
    for (int i = 0; i < waitAttempts; i++) {
      if (checkPreExisting(srcFs, src, dest, conf)) {
        return true;
      }
      if (doLog && i == 0) {
        LOG.info("Waiting for the file " + dest + " (" + waitAttempts + " attempts, with "
            + sleepInterval + "ms interval)");
      }
      try {
        if (notifier != null) {
          // The writing thread has given us an object to wait on.
          synchronized (notifier) {
            notifier.wait(sleepInterval);
          }
        } else {
          // Some other process is probably writing the file. Just sleep.
          Thread.sleep(sleepInterval);
        }
      } catch (InterruptedException interruptedException) {
        throw new IOException(interruptedException);
      }
    }
    return checkPreExisting(srcFs, src, dest, conf); // One last check.
  }

  /**
   * Creates and initializes a JobConf object that can be used to execute
   * the DAG. The configuration object will contain configurations from mapred-site
   * overlaid with key/value pairs from the conf object. Finally it will also
   * contain some hive specific configurations that do not change from DAG to DAG.
   *
   * @param hiveConf Current conf for the execution
   * @return JobConf base configuration for job execution
   * @throws IOException
   */
  public JobConf createConfiguration(HiveConf hiveConf) throws IOException {
    return createConfiguration(hiveConf, false);
  }

  /**
   * Creates and initializes a JobConf object that can be used to execute
   * the DAG. This can skip the configs which are already included in AM configs.
   * @param hiveConf Current conf for the execution
   * @param skipAMConf Skip the configs where are already set across all DAGs
   * @return JobConf base configuration for job execution
   * @throws IOException
   */
  public JobConf createConfiguration(HiveConf hiveConf, boolean skipAMConf) throws IOException {
    hiveConf.setBoolean("mapred.mapper.new-api", false);

    Predicate<String> findDefaults =
        (s) -> ((s != null) && (s.endsWith(".xml") || (s.endsWith(".java") && !"HiveConf.java".equals(s))));

    // since this is an inclusion filter, negate the predicate
    JobConf conf =
        TezConfigurationFactory
            .wrapWithJobConf(hiveConf, skipAMConf ? findDefaults.negate() : null);

    if (conf.get("mapred.output.committer.class") == null) {
      conf.set("mapred.output.committer.class", NullOutputCommitter.class.getName());
    }

    conf.setBoolean("mapred.committer.job.setup.cleanup.needed", false);
    conf.setBoolean("mapred.committer.job.task.cleanup.needed", false);

    conf.setClass("mapred.output.format.class", HiveOutputFormatImpl.class, OutputFormat.class);

    conf.set(MRJobConfig.OUTPUT_KEY_CLASS, HiveKey.class.getName());
    conf.set(MRJobConfig.OUTPUT_VALUE_CLASS, BytesWritable.class.getName());

    conf.set("mapred.partitioner.class", HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_PARTITIONER));
    conf.set("tez.runtime.partitioner.class", MRPartitioner.class.getName());

    // Removing job credential entry/ cannot be set on the tasks
    conf.unset("mapreduce.job.credentials.binary");

    // TODO: convert this to a predicate too
    hiveConf.stripHiddenConfigurations(conf);

    // Remove hive configs which are used only in HS2 and not needed for execution
    conf.unset(ConfVars.HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST.varname);
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
   * @param workUnit The instance of BaseWork representing the actual work to be performed
   * by this vertex.
   * @param scratchDir HDFS scratch dir for this execution unit.
   * @return Vertex
   */
  @SuppressWarnings("deprecation")
  public Vertex createVertex(JobConf conf, BaseWork workUnit, Path scratchDir,
      TezWork tezWork, Map<String, LocalResource> localResources) throws Exception {

    Vertex vertex;
    // simply dispatch the call to the right method for the actual (sub-) type of
    // BaseWork.
    VertexType vertexType = tezWork.getVertexType(workUnit);
    if (workUnit instanceof MapWork) {
      vertex = createVertexFromMapWork(
          conf, (MapWork) workUnit, scratchDir, vertexType);
    } else if (workUnit instanceof ReduceWork) {
      vertex = createVertexFromReduceWork(conf, (ReduceWork) workUnit, scratchDir);
    } else if (workUnit instanceof MergeJoinWork) {
      vertex = createVertexFromMergeWork(
          conf, (MergeJoinWork) workUnit, scratchDir, vertexType);
      // set VertexManagerPlugin if whether it's a cross product destination vertex
      List<String> crossProductSources = new ArrayList<>();
      for (BaseWork parentWork : tezWork.getParents(workUnit)) {
        if (tezWork.getEdgeType(parentWork, workUnit) == EdgeType.XPROD_EDGE) {
          crossProductSources.add(parentWork.getName());
        }
      }

      if (!crossProductSources.isEmpty()) {
        CartesianProductConfig cpConfig = new CartesianProductConfig(crossProductSources);
        vertex.setVertexManagerPlugin(
          VertexManagerPluginDescriptor.create(CartesianProductVertexManager.class.getName())
            .setUserPayload(cpConfig.toUserPayload(new TezConfiguration(conf))));
        // parallelism shouldn't be set for cartesian product vertex
      }
    } else {
      // something is seriously wrong if this is happening
      throw new HiveException(ErrorMsg.GENERIC_ERROR.getErrorCodedMsg());
    }
    VertexExecutionContext vertexExecutionContext = createVertexExecutionContext(workUnit);
    vertex.addTaskLocalFiles(localResources);
    vertex.setTaskLaunchCmdOpts(getContainerJavaOpts(conf));
    vertex.setExecutionContext(vertexExecutionContext);

    // initialize stats publisher if necessary
    if (workUnit.isGatheringStats()) {
      StatsPublisher statsPublisher;
      StatsFactory factory = StatsFactory.newFactory(conf);
      if (factory != null) {
        StatsCollectionContext sCntxt = new StatsCollectionContext(conf);
        sCntxt.setStatsTmpDirs(Utilities.getStatsTmpDirs(workUnit, conf));
        statsPublisher = factory.getStatsPublisher();
        if (!statsPublisher.init(sCntxt)) { // creating stats table if not exists
          if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STATS_RELIABLE)) {
            throw
              new HiveException(ErrorMsg.STATSPUBLISHER_INITIALIZATION_ERROR.getErrorCodedMsg());
          }
        }
      }
    }

    final Class outputKlass;
    if (HiveOutputFormatImpl.class.getName().equals(conf.get("mapred.output.format.class"))) {
      // Hive uses this output format, when it is going to write all its data through FS operator
      outputKlass = NullMROutput.class;
    } else {
      outputKlass = MROutput.class;
    }

    // If there is a fileSink add a DataSink to the vertex
    boolean hasFileSink = workUnit.getAllOperators().stream().anyMatch(o -> o instanceof FileSinkOperator);
    // final vertices need to have at least one output
    boolean endVertex = tezWork.getLeaves().contains(workUnit);
    if (endVertex || hasFileSink) {
      OutputCommitterDescriptor ocd = null;
      String committer = HiveConf.getVar(conf, ConfVars.TEZ_MAPREDUCE_OUTPUT_COMMITTER);
      if (committer != null && !committer.isEmpty()) {
        ocd = OutputCommitterDescriptor.create(committer);
      }
      vertex.addDataSink("out_"+workUnit.getName(), new DataSinkDescriptor(
          OutputDescriptor.create(outputKlass.getName())
          .setUserPayload(vertex.getProcessorDescriptor().getUserPayload()), ocd, null));
    }

    return vertex;
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
    if (!HiveConf.getBoolVar(conf, ConfVars.HIVE_RPC_QUERY_PLAN)) {
      FileSystem fs = tezDir.getFileSystem(conf);
      LOG.debug("TezDir path set " + tezDir + " for user: " + userName);
      // since we are adding the user name to the scratch dir, we do not
      // need to give more permissions here
      // Since we are doing RPC creating a dir is not necessary
      fs.mkdirs(tezDir);
    }

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
      pluginConf.setFloat(
          ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION,
          edgeProp.getMinSrcFraction());
      pluginConf.setFloat(
          ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION,
          edgeProp.getMaxSrcFraction());
      UserPayload payload = TezUtils.createUserPayloadFromConf(pluginConf);
      desc.setUserPayload(payload);
      v.setVertexManagerPlugin(desc);
    }
  }

  private void setupQuickStart(TezEdgeProperty edgeProp, Vertex v)
    throws IOException {
    if (!edgeProp.isSlowStart()) {
      Configuration pluginConf = new Configuration(false);
      VertexManagerPluginDescriptor desc =
              VertexManagerPluginDescriptor.create(ShuffleVertexManager.class.getName());
      pluginConf.setFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION, 0);
      pluginConf.setFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION, 0);
      UserPayload payload = TezUtils.createUserPayloadFromConf(pluginConf);
      desc.setUserPayload(payload);
      v.setVertexManagerPlugin(desc);
    }
  }

  public String createDagName(Configuration conf, QueryPlan plan) {
    String name = getUserSpecifiedDagName(conf);
    if (name == null) {
      name = plan.getQueryId();
    }

    assert name != null;
    return name;
  }

  public static String getUserSpecifiedDagName(Configuration conf) {
    String name = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_QUERY_NAME);
    return (name != null) ? name : conf.get("mapred.job.name");
  }

  @VisibleForTesting
  DagUtils(List<DagCredentialSupplier> suppliers) {
    this.credentialSuppliers = suppliers;
  }

  /**
   * TODO This method is temporary. Ideally Hive should only need to pass to Tez the amount of memory
   *      it requires to do the map join, and Tez should take care of figuring out how much to allocate
   * Adjust the percentage of memory to be reserved for the processor from Tez
   * based on the actual requested memory by the Map Join, i.e. HIVE_CONVERT_JOIN_NOCONDITIONAL_TASK_THRESHOLD
   * @return the adjusted percentage
   */
  static double adjustMemoryReserveFraction(long memoryRequested, HiveConf conf) {
    // User specified fraction always takes precedence
    if (conf.getFloatVar(ConfVars.TEZ_TASK_SCALE_MEMORY_RESERVE_FRACTION) > 0) {
      return conf.getFloatVar(ConfVars.TEZ_TASK_SCALE_MEMORY_RESERVE_FRACTION);
    }

    float tezHeapFraction = conf.getFloatVar(ConfVars.TEZ_CONTAINER_MAX_JAVA_HEAP_FRACTION);
    float tezMinReserveFraction = conf.getFloatVar(ConfVars.TEZ_TASK_SCALE_MEMORY_RESERVE_FRACTION_MIN);
    float tezMaxReserveFraction = conf.getFloatVar(ConfVars.TEZ_TASK_SCALE_MEMORY_RESERVE_FRACTION_MAX);

    Resource resource = getContainerResource(conf);
    long containerSize = (long) resource.getMemory() * 1024 * 1024;
    String javaOpts = getContainerJavaOpts(conf);
    long xmx = parseRightmostXmx(javaOpts);

    if (xmx <= 0) {
      xmx = (long) (tezHeapFraction * containerSize);
    }

    long actualMemToBeAllocated = (long) (tezMinReserveFraction * xmx);

    if (actualMemToBeAllocated < memoryRequested) {
      LOG.warn("The actual amount of memory to be allocated " + actualMemToBeAllocated +
          " is less than the amount of requested memory for Map Join conversion " + memoryRequested);
      float frac = (float) memoryRequested / xmx;
      LOG.info("Fraction after calculation: " + frac);
      if (frac <= tezMinReserveFraction) {
        return tezMinReserveFraction;
      } else if (frac > tezMinReserveFraction && frac < tezMaxReserveFraction) {
        LOG.info("Will adjust Tez setting " + TezConfiguration.TEZ_TASK_SCALE_MEMORY_RESERVE_FRACTION +
            " to " + frac + " to allocate more memory");
        return frac;
      } else {  // frac >= tezMaxReserveFraction
        return tezMaxReserveFraction;
      }
    }

    return tezMinReserveFraction;  // the default fraction
  }

  /**
   * Parse a Java opts string, try to find the rightmost -Xmx option value (since there may be more than one)
   * @param javaOpts Java opts string to parse
   * @return the rightmost -Xmx value in bytes. If Xmx is not set, return -1
   */
  static long parseRightmostXmx(String javaOpts) {
    // Find the last matching -Xmx following word boundaries
    // Format: -Xmx<size>[g|G|m|M|k|K]
    Pattern JAVA_OPTS_XMX_PATTERN = Pattern.compile(".*(?:^|\\s)-Xmx(\\d+)([gGmMkK]?)(?:$|\\s).*");
    Matcher m = JAVA_OPTS_XMX_PATTERN.matcher(javaOpts);

    if (m.matches()) {
      long size = Long.parseLong(m.group(1));
      if (size <= 0) {
        return -1;
      }

      if (m.group(2).isEmpty()) {
        // -Xmx specified in bytes
        return size;
      }

      char unit = m.group(2).charAt(0);
      switch (unit) {
        case 'k':
        case 'K':
          // -Xmx specified in KB
          return size * 1024;
        case 'm':
        case 'M':
          // -Xmx specified in MB
          return size * 1024 * 1024;
        case 'g':
        case 'G':
          // -Xmx specified in GB
          return size * 1024 * 1024 * 1024;
      }
    }

    // -Xmx not specified
    return -1;
  }

  // The utility of this method is not certain.
  public static Map<String, LocalResource> getResourcesUpdatableForAm(
      Collection<LocalResource> allNonAppResources) {
    HashMap<String, LocalResource> allNonAppFileResources = new HashMap<>();
    if (allNonAppResources == null) {
      return allNonAppFileResources;
    }
    for (LocalResource lr : allNonAppResources) {
      if (lr.getType() == LocalResourceType.FILE) {
        // TEZ AM will only localize FILE (no script operators in the AM)
        allNonAppFileResources.put(DagUtils.getBaseName(lr), lr);
      }
    }
    return allNonAppFileResources;
  }
}
