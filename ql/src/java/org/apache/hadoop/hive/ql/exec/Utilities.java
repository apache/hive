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

package org.apache.hadoop.hive.ql.exec;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.HiveInterruptCallback;
import org.apache.hadoop.hive.common.HiveInterruptUtils;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.hadoop.hive.ql.exec.mr.ExecReducer;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.exec.tez.DagUtils;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.ContentSummaryInputFormat;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.OneNullRowInputFormat;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.ReworkMapredInputFormat;
import org.apache.hadoop.hive.ql.io.SelfDescribingInputFormatInterface;
import org.apache.hadoop.hive.ql.io.merge.MergeFileMapper;
import org.apache.hadoop.hive.ql.io.merge.MergeFileWork;
import org.apache.hadoop.hive.ql.io.rcfile.stats.PartialScanMapper;
import org.apache.hadoop.hive.ql.io.rcfile.stats.PartialScanWork;
import org.apache.hadoop.hive.ql.io.rcfile.truncate.ColumnTruncateMapper;
import org.apache.hadoop.hive.ql.io.rcfile.truncate.ColumnTruncateWork;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.InputEstimator;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.physical.Vectorizer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.api.Adjacency;
import org.apache.hadoop.hive.ql.plan.api.Graph;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hive.common.util.ACLConfigurationParser;
import org.apache.hive.common.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.DefaultPersistenceDelegate;
import java.beans.Encoder;
import java.beans.Expression;
import java.beans.Statement;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTransientException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Utilities.
 *
 */
@SuppressWarnings("nls")
public final class Utilities {

  /**
   * The object in the reducer are composed of these top level fields.
   */

  public static final String HADOOP_LOCAL_FS = "file:///";
  public static final String HADOOP_LOCAL_FS_SCHEME = "file";
  public static final String MAP_PLAN_NAME = "map.xml";
  public static final String REDUCE_PLAN_NAME = "reduce.xml";
  public static final String MERGE_PLAN_NAME = "merge.xml";
  public static final String INPUT_NAME = "iocontext.input.name";
  public static final String HAS_MAP_WORK = "has.map.work";
  public static final String HAS_REDUCE_WORK = "has.reduce.work";
  public static final String MAPRED_MAPPER_CLASS = "mapred.mapper.class";
  public static final String MAPRED_REDUCER_CLASS = "mapred.reducer.class";
  public static final String HIVE_ADDED_JARS = "hive.added.jars";
  public static final String VECTOR_MODE = "VECTOR_MODE";
  public static final String USE_VECTORIZED_INPUT_FILE_FORMAT = "USE_VECTORIZED_INPUT_FILE_FORMAT";
  public static final String MAPNAME = "Map ";
  public static final String REDUCENAME = "Reducer ";

  @Deprecated
  protected static final String DEPRECATED_MAPRED_DFSCLIENT_PARALLELISM_MAX = "mapred.dfsclient.parallelism.max";

  /**
   * ReduceField:
   * KEY: record key
   * VALUE: record value
   */
  public static enum ReduceField {
    KEY, VALUE
  };

  public static List<String> reduceFieldNameList;
  static {
    reduceFieldNameList = new ArrayList<String>();
    for (ReduceField r : ReduceField.values()) {
      reduceFieldNameList.add(r.toString());
    }
  }

  public static String removeValueTag(String column) {
    if (column.startsWith(ReduceField.VALUE + ".")) {
      return column.substring(6);
    }
    return column;
  }

  private Utilities() {
    // prevent instantiation
  }

  private static GlobalWorkMapFactory gWorkMap = new GlobalWorkMapFactory();

  private static final String CLASS_NAME = Utilities.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  public static void clearWork(Configuration conf) {
    Path mapPath = getPlanPath(conf, MAP_PLAN_NAME);
    Path reducePath = getPlanPath(conf, REDUCE_PLAN_NAME);

    // if the plan path hasn't been initialized just return, nothing to clean.
    if (mapPath == null && reducePath == null) {
      return;
    }

    try {
      FileSystem fs = mapPath.getFileSystem(conf);
      if (fs.exists(mapPath)) {
        fs.delete(mapPath, true);
      }
      if (fs.exists(reducePath)) {
        fs.delete(reducePath, true);
      }

    } catch (Exception e) {
      LOG.warn("Failed to clean-up tmp directories.", e);
    } finally {
      // where a single process works with multiple plans - we must clear
      // the cache before working with the next plan.
      clearWorkMapForConf(conf);
    }
  }

  public static MapredWork getMapRedWork(Configuration conf) {
    MapredWork w = new MapredWork();
    w.setMapWork(getMapWork(conf));
    w.setReduceWork(getReduceWork(conf));
    return w;
  }

  public static void cacheMapWork(Configuration conf, MapWork work, Path hiveScratchDir) {
    cacheBaseWork(conf, MAP_PLAN_NAME, work, hiveScratchDir);
  }

  public static void setMapWork(Configuration conf, MapWork work) {
    setBaseWork(conf, MAP_PLAN_NAME, work);
  }

  public static MapWork getMapWork(Configuration conf) {
    if (!conf.getBoolean(HAS_MAP_WORK, false)) {
      return null;
    }
    return (MapWork) getBaseWork(conf, MAP_PLAN_NAME);
  }

  public static void setReduceWork(Configuration conf, ReduceWork work) {
    setBaseWork(conf, REDUCE_PLAN_NAME, work);
  }

  public static ReduceWork getReduceWork(Configuration conf) {
    if (!conf.getBoolean(HAS_REDUCE_WORK, false)) {
      return null;
    }
    return (ReduceWork) getBaseWork(conf, REDUCE_PLAN_NAME);
  }

  public static Path setMergeWork(JobConf conf, MergeJoinWork mergeJoinWork, Path mrScratchDir,
      boolean useCache) {
    for (BaseWork baseWork : mergeJoinWork.getBaseWorkList()) {
      setBaseWork(conf, baseWork, mrScratchDir, baseWork.getName() + MERGE_PLAN_NAME, useCache);
      String prefixes = conf.get(DagUtils.TEZ_MERGE_WORK_FILE_PREFIXES);
      if (prefixes == null) {
        prefixes = baseWork.getName();
      } else {
        prefixes = prefixes + "," + baseWork.getName();
      }
      conf.set(DagUtils.TEZ_MERGE_WORK_FILE_PREFIXES, prefixes);
    }

    // nothing to return
    return null;
  }

  public static BaseWork getMergeWork(Configuration jconf) {
    if ((jconf.get(DagUtils.TEZ_MERGE_CURRENT_MERGE_FILE_PREFIX) == null)
        || (jconf.get(DagUtils.TEZ_MERGE_CURRENT_MERGE_FILE_PREFIX).isEmpty())) {
      return null;
    }
    return getMergeWork(jconf, jconf.get(DagUtils.TEZ_MERGE_CURRENT_MERGE_FILE_PREFIX));
  }

  public static BaseWork getMergeWork(Configuration jconf, String prefix) {
    if (prefix == null || prefix.isEmpty()) {
      return null;
    }

    return getBaseWork(jconf, prefix + MERGE_PLAN_NAME);
  }

  public static void cacheBaseWork(Configuration conf, String name, BaseWork work,
      Path hiveScratchDir) {
    try {
      setPlanPath(conf, hiveScratchDir);
      setBaseWork(conf, name, work);
    } catch (IOException e) {
      LOG.error("Failed to cache plan", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Pushes work into the global work map
   */
  public static void setBaseWork(Configuration conf, String name, BaseWork work) {
    Path path = getPlanPath(conf, name);
    setHasWork(conf, name);
    gWorkMap.get(conf).put(path, work);
  }

  /**
   * Returns the Map or Reduce plan
   * Side effect: the BaseWork returned is also placed in the gWorkMap
   * @param conf
   * @param name
   * @return BaseWork based on the name supplied will return null if name is null
   * @throws RuntimeException if the configuration files are not proper or if plan can not be loaded
   */
  private static BaseWork getBaseWork(Configuration conf, String name) {
    Path path = null;
    InputStream in = null;
    Kryo kryo = SerializationUtilities.borrowKryo();
    try {
      String engine = HiveConf.getVar(conf, ConfVars.HIVE_EXECUTION_ENGINE);
      if (engine.equals("spark")) {
        // TODO Add jar into current thread context classloader as it may be invoked by Spark driver inside
        // threads, should be unnecessary while SPARK-5377 is resolved.
        String addedJars = conf.get(HIVE_ADDED_JARS);
        if (addedJars != null && !addedJars.isEmpty()) {
          ClassLoader loader = Thread.currentThread().getContextClassLoader();
          ClassLoader newLoader = addToClassPath(loader, addedJars.split(";"));
          Thread.currentThread().setContextClassLoader(newLoader);
          kryo.setClassLoader(newLoader);
        }
      }

      path = getPlanPath(conf, name);
      LOG.info("PLAN PATH = " + path);
      if (path == null) { // Map/reduce plan may not be generated
        return null;
      }

      BaseWork gWork = gWorkMap.get(conf).get(path);
      if (gWork == null) {
        Path localPath = path;
        LOG.debug("local path = " + localPath);
        final long serializedSize;
        final String planMode;
        if (HiveConf.getBoolVar(conf, ConfVars.HIVE_RPC_QUERY_PLAN)) {
          LOG.debug("Loading plan from string: "+path.toUri().getPath());
          String planString = conf.getRaw(path.toUri().getPath());
          if (planString == null) {
            LOG.info("Could not find plan string in conf");
            return null;
          }
          serializedSize = planString.length();
          planMode = "RPC";
          byte[] planBytes = Base64.decodeBase64(planString);
          in = new ByteArrayInputStream(planBytes);
          in = new InflaterInputStream(in);
        } else {
          LOG.debug("Open file to read in plan: " + localPath);
          FileSystem fs = localPath.getFileSystem(conf);
          in = fs.open(localPath);
          serializedSize = fs.getFileStatus(localPath).getLen();
          planMode = "FILE";
        }

        if(MAP_PLAN_NAME.equals(name)){
          if (ExecMapper.class.getName().equals(conf.get(MAPRED_MAPPER_CLASS))){
            gWork = SerializationUtilities.deserializePlan(kryo, in, MapWork.class);
          } else if(MergeFileMapper.class.getName().equals(conf.get(MAPRED_MAPPER_CLASS))) {
            gWork = SerializationUtilities.deserializePlan(kryo, in, MergeFileWork.class);
          } else if(ColumnTruncateMapper.class.getName().equals(conf.get(MAPRED_MAPPER_CLASS))) {
            gWork = SerializationUtilities.deserializePlan(kryo, in, ColumnTruncateWork.class);
          } else if(PartialScanMapper.class.getName().equals(conf.get(MAPRED_MAPPER_CLASS))) {
            gWork = SerializationUtilities.deserializePlan(kryo, in, PartialScanWork.class);
          } else {
            throw new RuntimeException("unable to determine work from configuration ."
                + MAPRED_MAPPER_CLASS + " was "+ conf.get(MAPRED_MAPPER_CLASS)) ;
          }
        } else if (REDUCE_PLAN_NAME.equals(name)) {
          if(ExecReducer.class.getName().equals(conf.get(MAPRED_REDUCER_CLASS))) {
            gWork = SerializationUtilities.deserializePlan(kryo, in, ReduceWork.class);
          } else {
            throw new RuntimeException("unable to determine work from configuration ."
                + MAPRED_REDUCER_CLASS +" was "+ conf.get(MAPRED_REDUCER_CLASS)) ;
          }
        } else if (name.contains(MERGE_PLAN_NAME)) {
          if (name.startsWith(MAPNAME)) {
            gWork = SerializationUtilities.deserializePlan(kryo, in, MapWork.class);
          } else if (name.startsWith(REDUCENAME)) {
            gWork = SerializationUtilities.deserializePlan(kryo, in, ReduceWork.class);
          } else {
            throw new RuntimeException("Unknown work type: " + name);
          }
        }
        LOG.info("Deserialized plan (via {}) - name: {} size: {}", planMode,
            gWork.getName(), humanReadableByteCount(serializedSize));
        gWorkMap.get(conf).put(path, gWork);
      } else if (LOG.isDebugEnabled()) {
        LOG.debug("Found plan in cache for name: " + name);
      }
      return gWork;
    } catch (FileNotFoundException fnf) {
      // happens. e.g.: no reduce work.
      LOG.debug("No plan file found: " + path + "; " + fnf.getMessage());
      return null;
    } catch (Exception e) {
      String msg = "Failed to load plan: " + path;
      LOG.error("Failed to load plan: " + path, e);
      throw new RuntimeException(msg, e);
    } finally {
      SerializationUtilities.releaseKryo(kryo);
      if (in != null) {
        try {
          in.close();
        } catch (IOException cantBlameMeForTrying) { }
      }
    }
  }

  private static void setHasWork(Configuration conf, String name) {
    if (MAP_PLAN_NAME.equals(name)) {
      conf.setBoolean(HAS_MAP_WORK, true);
    } else if (REDUCE_PLAN_NAME.equals(name)) {
      conf.setBoolean(HAS_REDUCE_WORK, true);
    }
  }

  public static void setWorkflowAdjacencies(Configuration conf, QueryPlan plan) {
    try {
      Graph stageGraph = plan.getQueryPlan().getStageGraph();
      if (stageGraph == null) {
        return;
      }
      List<Adjacency> adjList = stageGraph.getAdjacencyList();
      if (adjList == null) {
        return;
      }
      for (Adjacency adj : adjList) {
        List<String> children = adj.getChildren();
        if (children == null || children.isEmpty()) {
          return;
        }
        conf.setStrings("mapreduce.workflow.adjacency."+adj.getNode(),
            children.toArray(new String[children.size()]));
      }
    } catch (IOException e) {
    }
  }

  public static List<String> getFieldSchemaString(List<FieldSchema> fl) {
    if (fl == null) {
      return null;
    }

    ArrayList<String> ret = new ArrayList<String>();
    for (FieldSchema f : fl) {
      ret.add(f.getName() + " " + f.getType()
          + (f.getComment() != null ? (" " + f.getComment()) : ""));
    }
    return ret;
  }

  public static void setMapRedWork(Configuration conf, MapredWork w, Path hiveScratchDir) {
    String useName = conf.get(INPUT_NAME);
    if (useName == null) {
      useName = "mapreduce";
    }
    conf.set(INPUT_NAME, useName);
    setMapWork(conf, w.getMapWork(), hiveScratchDir, true);
    if (w.getReduceWork() != null) {
      conf.set(INPUT_NAME, useName);
      setReduceWork(conf, w.getReduceWork(), hiveScratchDir, true);
    }
  }

  public static Path setMapWork(Configuration conf, MapWork w, Path hiveScratchDir, boolean useCache) {
    return setBaseWork(conf, w, hiveScratchDir, MAP_PLAN_NAME, useCache);
  }

  public static Path setReduceWork(Configuration conf, ReduceWork w, Path hiveScratchDir, boolean useCache) {
    return setBaseWork(conf, w, hiveScratchDir, REDUCE_PLAN_NAME, useCache);
  }

  private static Path setBaseWork(Configuration conf, BaseWork w, Path hiveScratchDir, String name, boolean useCache) {
    Kryo kryo = SerializationUtilities.borrowKryo();
    try {
      setPlanPath(conf, hiveScratchDir);

      Path planPath = getPlanPath(conf, name);
      setHasWork(conf, name);

      OutputStream out = null;

      final long serializedSize;
      final String planMode;
      if (HiveConf.getBoolVar(conf, ConfVars.HIVE_RPC_QUERY_PLAN)) {
        // add it to the conf
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        try {
          out = new DeflaterOutputStream(byteOut, new Deflater(Deflater.BEST_SPEED));
          SerializationUtilities.serializePlan(kryo, w, out);
          out.close();
          out = null;
        } finally {
          IOUtils.closeStream(out);
        }
        final String serializedPlan = Base64.encodeBase64String(byteOut.toByteArray());
        serializedSize = serializedPlan.length();
        planMode = "RPC";
        conf.set(planPath.toUri().getPath(), serializedPlan);
      } else {
        // use the default file system of the conf
        FileSystem fs = planPath.getFileSystem(conf);
        try {
          out = fs.create(planPath);
          SerializationUtilities.serializePlan(kryo, w, out);
          out.close();
          out = null;
          long fileLen = fs.getFileStatus(planPath).getLen();
          serializedSize = fileLen;
          planMode = "FILE";
        } finally {
          IOUtils.closeStream(out);
        }

        // Serialize the plan to the default hdfs instance
        // Except for hadoop local mode execution where we should be
        // able to get the plan directly from the cache
        if (useCache && !ShimLoader.getHadoopShims().isLocalMode(conf)) {
          // Set up distributed cache
          if (!DistributedCache.getSymlink(conf)) {
            DistributedCache.createSymlink(conf);
          }
          String uriWithLink = planPath.toUri().toString() + "#" + name;
          DistributedCache.addCacheFile(new URI(uriWithLink), conf);

          // set replication of the plan file to a high number. we use the same
          // replication factor as used by the hadoop jobclient for job.xml etc.
          short replication = (short) conf.getInt("mapred.submit.replication", 10);
          fs.setReplication(planPath, replication);
        }
      }

      LOG.info("Serialized plan (via {}) - name: {} size: {}", planMode, w.getName(),
          humanReadableByteCount(serializedSize));
      // Cache the plan in this process
      gWorkMap.get(conf).put(planPath, w);
      return planPath;
    } catch (Exception e) {
      String msg = "Error caching " + name + ": " + e;
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    } finally {
      SerializationUtilities.releaseKryo(kryo);
    }
  }

  private static Path getPlanPath(Configuration conf, String name) {
    Path planPath = getPlanPath(conf);
    if (planPath == null) {
      return null;
    }
    return new Path(planPath, name);
  }

  private static void setPlanPath(Configuration conf, Path hiveScratchDir) throws IOException {
    if (getPlanPath(conf) == null) {
      // this is the unique conf ID, which is kept in JobConf as part of the plan file name
      String jobID = UUID.randomUUID().toString();
      Path planPath = new Path(hiveScratchDir, jobID);
      FileSystem fs = planPath.getFileSystem(conf);
      fs.mkdirs(planPath);
      HiveConf.setVar(conf, HiveConf.ConfVars.PLAN, planPath.toUri().toString());
    }
  }

  public static Path getPlanPath(Configuration conf) {
    String plan = HiveConf.getVar(conf, HiveConf.ConfVars.PLAN);
    if (plan != null && !plan.isEmpty()) {
      return new Path(plan);
    }
    return null;
  }

  public static class CollectionPersistenceDelegate extends DefaultPersistenceDelegate {
    @Override
    protected Expression instantiate(Object oldInstance, Encoder out) {
      return new Expression(oldInstance, oldInstance.getClass(), "new", null);
    }

    @Override
    protected void initialize(Class type, Object oldInstance, Object newInstance, Encoder out) {
      Iterator ite = ((Collection) oldInstance).iterator();
      while (ite.hasNext()) {
        out.writeStatement(new Statement(oldInstance, "add", new Object[] {ite.next()}));
      }
    }
  }

  public static TableDesc defaultTd;
  static {
    // by default we expect ^A separated strings
    // This tableDesc does not provide column names. We should always use
    // PlanUtils.getDefaultTableDesc(String separatorCode, String columns)
    // or getBinarySortableTableDesc(List<FieldSchema> fieldSchemas) when
    // we know the column names.
    defaultTd = PlanUtils.getDefaultTableDesc("" + Utilities.ctrlaCode);
  }

  public static final int carriageReturnCode = 13;
  public static final int newLineCode = 10;
  public static final int tabCode = 9;
  public static final int ctrlaCode = 1;

  public static final String INDENT = "  ";

  // Note: When DDL supports specifying what string to represent null,
  // we should specify "NULL" to represent null in the temp table, and then
  // we can make the following translation deprecated.
  public static final String nullStringStorage = "\\N";
  public static final String nullStringOutput = "NULL";

  public static Random randGen = new Random();

  /**
   * Gets the task id if we are running as a Hadoop job. Gets a random number otherwise.
   */
  public static String getTaskId(Configuration hconf) {
    String taskid = (hconf == null) ? null : hconf.get("mapred.task.id");
    if ((taskid == null) || taskid.equals("")) {
      return ("" + Math.abs(randGen.nextInt()));
    } else {
      /*
       * extract the task and attempt id from the hadoop taskid. in version 17 the leading component
       * was 'task_'. thereafter the leading component is 'attempt_'. in 17 - hadoop also seems to
       * have used _map_ and _reduce_ to denote map/reduce task types
       */
      String ret = taskid.replaceAll(".*_[mr]_", "").replaceAll(".*_(map|reduce)_", "");
      return (ret);
    }
  }

  public static HashMap makeMap(Object... olist) {
    HashMap ret = new HashMap();
    for (int i = 0; i < olist.length; i += 2) {
      ret.put(olist[i], olist[i + 1]);
    }
    return (ret);
  }

  public static Properties makeProperties(String... olist) {
    Properties ret = new Properties();
    for (int i = 0; i < olist.length; i += 2) {
      ret.setProperty(olist[i], olist[i + 1]);
    }
    return (ret);
  }

  public static ArrayList makeList(Object... olist) {
    ArrayList ret = new ArrayList();
    for (Object element : olist) {
      ret.add(element);
    }
    return (ret);
  }

  public static TableDesc getTableDesc(Table tbl) {
    Properties props = tbl.getMetadata();
    props.put(serdeConstants.SERIALIZATION_LIB, tbl.getDeserializer().getClass().getName());
    return (new TableDesc(tbl.getInputFormatClass(), tbl
        .getOutputFormatClass(), props));
  }

  // column names and column types are all delimited by comma
  public static TableDesc getTableDesc(String cols, String colTypes) {
    return (new TableDesc(SequenceFileInputFormat.class,
        HiveSequenceFileOutputFormat.class, Utilities.makeProperties(
        serdeConstants.SERIALIZATION_FORMAT, "" + Utilities.ctrlaCode,
        serdeConstants.LIST_COLUMNS, cols,
        serdeConstants.LIST_COLUMN_TYPES, colTypes,
        serdeConstants.SERIALIZATION_LIB,LazySimpleSerDe.class.getName())));
  }

  public static PartitionDesc getPartitionDesc(Partition part) throws HiveException {
    return new PartitionDesc(part);
  }

  public static PartitionDesc getPartitionDescFromTableDesc(TableDesc tblDesc, Partition part,
    boolean usePartSchemaProperties) throws HiveException {
    return new PartitionDesc(part, tblDesc, usePartSchemaProperties);
  }

  private static String getOpTreeSkel_helper(Operator<?> op, String indent) {
    if (op == null) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    sb.append(indent);
    sb.append(op.toString());
    sb.append("\n");
    if (op.getChildOperators() != null) {
      for (Object child : op.getChildOperators()) {
        sb.append(getOpTreeSkel_helper((Operator<?>) child, indent + "  "));
      }
    }

    return sb.toString();
  }

  public static String getOpTreeSkel(Operator<?> op) {
    return getOpTreeSkel_helper(op, "");
  }

  private static boolean isWhitespace(int c) {
    if (c == -1) {
      return false;
    }
    return Character.isWhitespace((char) c);
  }

  public static boolean contentsEqual(InputStream is1, InputStream is2, boolean ignoreWhitespace)
      throws IOException {
    try {
      if ((is1 == is2) || (is1 == null && is2 == null)) {
        return true;
      }

      if (is1 == null || is2 == null) {
        return false;
      }

      while (true) {
        int c1 = is1.read();
        while (ignoreWhitespace && isWhitespace(c1)) {
          c1 = is1.read();
        }
        int c2 = is2.read();
        while (ignoreWhitespace && isWhitespace(c2)) {
          c2 = is2.read();
        }
        if (c1 == -1 && c2 == -1) {
          return true;
        }
        if (c1 != c2) {
          break;
        }
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    return false;
  }

  /**
   * convert "From src insert blah blah" to "From src insert ... blah"
   */
  public static String abbreviate(String str, int max) {
    str = str.trim();

    int len = str.length();
    int suffixlength = 20;

    if (len <= max) {
      return str;
    }

    suffixlength = Math.min(suffixlength, (max - 3) / 2);
    String rev = StringUtils.reverse(str);

    // get the last few words
    String suffix = WordUtils.abbreviate(rev, 0, suffixlength, "");
    suffix = StringUtils.reverse(suffix);

    // first few ..
    String prefix = StringUtils.abbreviate(str, max - suffix.length());

    return prefix + suffix;
  }

  public static final String NSTR = "";

  /**
   * StreamStatus.
   *
   */
  public static enum StreamStatus {
    EOF, TERMINATED
  }

  public static StreamStatus readColumn(DataInput in, OutputStream out) throws IOException {

    boolean foundCrChar = false;
    while (true) {
      int b;
      try {
        b = in.readByte();
      } catch (EOFException e) {
        return StreamStatus.EOF;
      }

      if (b == Utilities.newLineCode) {
        return StreamStatus.TERMINATED;
      }

      out.write(b);
    }
    // Unreachable
  }

  /**
   * Convert an output stream to a compressed output stream based on codecs and compression options
   * specified in the Job Configuration.
   *
   * @param jc
   *          Job Configuration
   * @param out
   *          Output Stream to be converted into compressed output stream
   * @return compressed output stream
   */
  public static OutputStream createCompressedStream(JobConf jc, OutputStream out)
      throws IOException {
    boolean isCompressed = FileOutputFormat.getCompressOutput(jc);
    return createCompressedStream(jc, out, isCompressed);
  }

  /**
   * Convert an output stream to a compressed output stream based on codecs codecs in the Job
   * Configuration. Caller specifies directly whether file is compressed or not
   *
   * @param jc
   *          Job Configuration
   * @param out
   *          Output Stream to be converted into compressed output stream
   * @param isCompressed
   *          whether the output stream needs to be compressed or not
   * @return compressed output stream
   */
  public static OutputStream createCompressedStream(JobConf jc, OutputStream out,
      boolean isCompressed) throws IOException {
    if (isCompressed) {
      Class<? extends CompressionCodec> codecClass = FileOutputFormat.getOutputCompressorClass(jc,
          DefaultCodec.class);
      CompressionCodec codec = ReflectionUtil.newInstance(codecClass, jc);
      return codec.createOutputStream(out);
    } else {
      return (out);
    }
  }

  /**
   * Based on compression option and configured output codec - get extension for output file. This
   * is only required for text files - not sequencefiles
   *
   * @param jc
   *          Job Configuration
   * @param isCompressed
   *          Whether the output file is compressed or not
   * @return the required file extension (example: .gz)
   * @deprecated Use {@link #getFileExtension(JobConf, boolean, HiveOutputFormat)}
   */
  @Deprecated
  public static String getFileExtension(JobConf jc, boolean isCompressed) {
    return getFileExtension(jc, isCompressed, new HiveIgnoreKeyTextOutputFormat());
  }

  /**
   * Based on compression option, output format, and configured output codec -
   * get extension for output file. Text files require an extension, whereas
   * others, like sequence files, do not.
   * <p>
   * The property <code>hive.output.file.extension</code> is used to determine
   * the extension - if set, it will override other logic for choosing an
   * extension.
   *
   * @param jc
   *          Job Configuration
   * @param isCompressed
   *          Whether the output file is compressed or not
   * @param hiveOutputFormat
   *          The output format, used to detect if the format is text
   * @return the required file extension (example: .gz)
   */
  public static String getFileExtension(JobConf jc, boolean isCompressed,
      HiveOutputFormat<?, ?> hiveOutputFormat) {
    String extension = HiveConf.getVar(jc, HiveConf.ConfVars.OUTPUT_FILE_EXTENSION);
    if (!StringUtils.isEmpty(extension)) {
      return extension;
    }
    if ((hiveOutputFormat instanceof HiveIgnoreKeyTextOutputFormat) && isCompressed) {
      Class<? extends CompressionCodec> codecClass = FileOutputFormat.getOutputCompressorClass(jc,
          DefaultCodec.class);
      CompressionCodec codec = ReflectionUtil.newInstance(codecClass, jc);
      return codec.getDefaultExtension();
    }
    return "";
  }

  /**
   * Create a sequencefile output stream based on job configuration.
   *
   * @param jc
   *          Job configuration
   * @param fs
   *          File System to create file in
   * @param file
   *          Path to be created
   * @param keyClass
   *          Java Class for key
   * @param valClass
   *          Java Class for value
   * @return output stream over the created sequencefile
   */
  public static SequenceFile.Writer createSequenceWriter(JobConf jc, FileSystem fs, Path file,
      Class<?> keyClass, Class<?> valClass, Progressable progressable) throws IOException {
    boolean isCompressed = FileOutputFormat.getCompressOutput(jc);
    return createSequenceWriter(jc, fs, file, keyClass, valClass, isCompressed, progressable);
  }

  /**
   * Create a sequencefile output stream based on job configuration Uses user supplied compression
   * flag (rather than obtaining it from the Job Configuration).
   *
   * @param jc
   *          Job configuration
   * @param fs
   *          File System to create file in
   * @param file
   *          Path to be created
   * @param keyClass
   *          Java Class for key
   * @param valClass
   *          Java Class for value
   * @return output stream over the created sequencefile
   */
  public static SequenceFile.Writer createSequenceWriter(JobConf jc, FileSystem fs, Path file,
      Class<?> keyClass, Class<?> valClass, boolean isCompressed, Progressable progressable)
      throws IOException {
    CompressionCodec codec = null;
    CompressionType compressionType = CompressionType.NONE;
    Class codecClass = null;
    if (isCompressed) {
      compressionType = SequenceFileOutputFormat.getOutputCompressionType(jc);
      codecClass = FileOutputFormat.getOutputCompressorClass(jc, DefaultCodec.class);
      codec = (CompressionCodec) ReflectionUtil.newInstance(codecClass, jc);
    }
    return SequenceFile.createWriter(fs, jc, file, keyClass, valClass, compressionType, codec,
      progressable);

  }

  /**
   * Create a RCFile output stream based on job configuration Uses user supplied compression flag
   * (rather than obtaining it from the Job Configuration).
   *
   * @param jc
   *          Job configuration
   * @param fs
   *          File System to create file in
   * @param file
   *          Path to be created
   * @return output stream over the created rcfile
   */
  public static RCFile.Writer createRCFileWriter(JobConf jc, FileSystem fs, Path file,
      boolean isCompressed, Progressable progressable) throws IOException {
    CompressionCodec codec = null;
    if (isCompressed) {
      Class<?> codecClass = FileOutputFormat.getOutputCompressorClass(jc, DefaultCodec.class);
      codec = (CompressionCodec) ReflectionUtil.newInstance(codecClass, jc);
    }
    return new RCFile.Writer(fs, jc, file, progressable, codec);
  }

  /**
   * Shamelessly cloned from GenericOptionsParser.
   */
  public static String realFile(String newFile, Configuration conf) throws IOException {
    Path path = new Path(newFile);
    URI pathURI = path.toUri();
    FileSystem fs;

    if (pathURI.getScheme() == null) {
      fs = FileSystem.getLocal(conf);
    } else {
      fs = path.getFileSystem(conf);
    }

    if (!fs.exists(path)) {
      return null;
    }

    String file = path.makeQualified(fs).toString();
    return file;
  }

  public static List<String> mergeUniqElems(List<String> src, List<String> dest) {
    if (dest == null) {
      return src;
    }
    if (src == null) {
      return dest;
    }
    int pos = 0;

    while (pos < dest.size()) {
      if (!src.contains(dest.get(pos))) {
        src.add(dest.get(pos));
      }
      pos++;
    }

    return src;
  }

  private static final String tmpPrefix = "_tmp.";
  private static final String taskTmpPrefix = "_task_tmp.";

  public static Path toTaskTempPath(Path orig) {
    if (orig.getName().indexOf(taskTmpPrefix) == 0) {
      return orig;
    }
    return new Path(orig.getParent(), taskTmpPrefix + orig.getName());
  }

  public static Path toTempPath(Path orig) {
    if (orig.getName().indexOf(tmpPrefix) == 0) {
      return orig;
    }
    return new Path(orig.getParent(), tmpPrefix + orig.getName());
  }

  /**
   * Given a path, convert to a temporary path.
   */
  public static Path toTempPath(String orig) {
    return toTempPath(new Path(orig));
  }

  /**
   * Detect if the supplied file is a temporary path.
   */
  public static boolean isTempPath(FileStatus file) {
    String name = file.getPath().getName();
    // in addition to detecting hive temporary files, we also check hadoop
    // temporary folders that used to show up in older releases
    return (name.startsWith("_task") || name.startsWith(tmpPrefix));
  }

  /**
   * Rename src to dst, or in the case dst already exists, move files in src to dst. If there is an
   * existing file with the same name, the new file's name will be appended with "_1", "_2", etc.
   *
   * @param fs
   *          the FileSystem where src and dst are on.
   * @param src
   *          the src directory
   * @param dst
   *          the target directory
   * @throws IOException
   */
  public static void rename(FileSystem fs, Path src, Path dst) throws IOException, HiveException {
    if (!fs.rename(src, dst)) {
      throw new HiveException("Unable to move: " + src + " to: " + dst);
    }
  }

  /**
   * Rename src to dst, or in the case dst already exists, move files in src to dst. If there is an
   * existing file with the same name, the new file's name will be appended with "_1", "_2", etc.
   *
   * @param fs
   *          the FileSystem where src and dst are on.
   * @param src
   *          the src directory
   * @param dst
   *          the target directory
   * @throws IOException
   */
  public static void renameOrMoveFiles(FileSystem fs, Path src, Path dst) throws IOException,
      HiveException {
    if (!fs.exists(dst)) {
      if (!fs.rename(src, dst)) {
        throw new HiveException("Unable to move: " + src + " to: " + dst);
      }
    } else {
      // move file by file
      FileStatus[] files = fs.listStatus(src);
      for (FileStatus file : files) {

        Path srcFilePath = file.getPath();
        String fileName = srcFilePath.getName();
        Path dstFilePath = new Path(dst, fileName);
        if (file.isDir()) {
          renameOrMoveFiles(fs, srcFilePath, dstFilePath);
        }
        else {
          if (fs.exists(dstFilePath)) {
            int suffix = 0;
            do {
              suffix++;
              dstFilePath = new Path(dst, fileName + "_" + suffix);
            } while (fs.exists(dstFilePath));
          }

          if (!fs.rename(srcFilePath, dstFilePath)) {
            throw new HiveException("Unable to move: " + src + " to: " + dst);
          }
        }
      }
    }
  }

  /**
   * The first group will contain the task id. The second group is the optional extension. The file
   * name looks like: "0_0" or "0_0.gz". There may be a leading prefix (tmp_). Since getTaskId() can
   * return an integer only - this should match a pure integer as well. {1,6} is used to limit
   * matching for attempts #'s 0-999999.
   */
  private static final Pattern FILE_NAME_TO_TASK_ID_REGEX =
      Pattern.compile("^.*?([0-9]+)(_[0-9]{1,6})?(\\..*)?$");

  /**
   * Some jobs like "INSERT INTO" jobs create copies of files like 0000001_0_copy_2.
   * For such files,
   * Group 1: 00000001 [taskId]
   * Group 3: 0        [task attempId]
   * Group 4: _copy_2  [copy suffix]
   * Group 6: copy     [copy keyword]
   * Group 8: 2        [copy file index]
   */
  private static final String COPY_KEYWORD = "_copy_"; // copy keyword
  private static final Pattern COPY_FILE_NAME_TO_TASK_ID_REGEX =
      Pattern.compile("^.*?"+ // any prefix
                      "([0-9]+)"+ // taskId
                      "(_)"+ // separator
                      "([0-9]{1,6})?"+ // attemptId (limited to 6 digits)
                      "((_)(\\Bcopy\\B)(_)" +
                      "([0-9]{1,6})$)?"+ // copy file index
                      "(\\..*)?$"); // any suffix/file extension

  /**
   * This retruns prefix part + taskID for bucket join for partitioned table
   */
  private static final Pattern FILE_NAME_PREFIXED_TASK_ID_REGEX =
      Pattern.compile("^.*?((\\(.*\\))?[0-9]+)(_[0-9]{1,6})?(\\..*)?$");

  /**
   * This breaks a prefixed bucket number into the prefix and the taskID
   */
  private static final Pattern PREFIXED_TASK_ID_REGEX =
      Pattern.compile("^(.*?\\(.*\\))?([0-9]+)$");

  /**
   * This breaks a prefixed bucket number out into a single integer
   */
  private static final Pattern PREFIXED_BUCKET_ID_REGEX =
      Pattern.compile("^(0*([0-9]+))_([0-9]+).*");
  /**
   * Get the task id from the filename. It is assumed that the filename is derived from the output
   * of getTaskId
   *
   * @param filename
   *          filename to extract taskid from
   */
  public static String getTaskIdFromFilename(String filename) {
    return getIdFromFilename(filename, FILE_NAME_TO_TASK_ID_REGEX);
  }

  /**
   * Get the part-spec + task id from the filename. It is assumed that the filename is derived
   * from the output of getTaskId
   *
   * @param filename
   *          filename to extract taskid from
   */
  public static String getPrefixedTaskIdFromFilename(String filename) {
    return getIdFromFilename(filename, FILE_NAME_PREFIXED_TASK_ID_REGEX);
  }

  private static String getIdFromFilename(String filename, Pattern pattern) {
    String taskId = filename;
    int dirEnd = filename.lastIndexOf(Path.SEPARATOR);
    if (dirEnd != -1) {
      taskId = filename.substring(dirEnd + 1);
    }

    Matcher m = pattern.matcher(taskId);
    if (!m.matches()) {
      LOG.warn("Unable to get task id from file name: " + filename + ". Using last component"
          + taskId + " as task id.");
    } else {
      taskId = m.group(1);
    }
    LOG.debug("TaskId for " + filename + " = " + taskId);
    return taskId;
  }

  public static String getFileNameFromDirName(String dirName) {
    int dirEnd = dirName.lastIndexOf(Path.SEPARATOR);
    if (dirEnd != -1) {
      return dirName.substring(dirEnd + 1);
    }
    return dirName;
  }

  /**
   * Replace the task id from the filename. It is assumed that the filename is derived from the
   * output of getTaskId
   *
   * @param filename
   *          filename to replace taskid "0_0" or "0_0.gz" by 33 to "33_0" or "33_0.gz"
   */
  public static String replaceTaskIdFromFilename(String filename, int bucketNum) {
    return replaceTaskIdFromFilename(filename, String.valueOf(bucketNum));
  }

  public static String replaceTaskIdFromFilename(String filename, String fileId) {
    String taskId = getTaskIdFromFilename(filename);
    String newTaskId = replaceTaskId(taskId, fileId);
    String ret = replaceTaskIdFromFilename(filename, taskId, newTaskId);
    return (ret);
  }

  /**
   * Replace taskId with input bucketNum. For example, if taskId is 000000 and bucketNum is 1,
   * return should be 000001; if taskId is (ds%3D1)000000 and bucketNum is 1, return should be
   * (ds%3D1)000001. This method is different from the replaceTaskId(String, String) method.
   * In this method, the pattern is in taskId.
   * @param taskId
   * @param bucketNum
   * @return
   */
  public static String replaceTaskId(String taskId, int bucketNum) {
    String bucketNumStr = String.valueOf(bucketNum);
    Matcher m = PREFIXED_TASK_ID_REGEX.matcher(taskId);
    if (!m.matches()) {
        LOG.warn("Unable to determine bucket number from task id: " + taskId + ". Using " +
            "task ID as bucket number.");
        return adjustBucketNumLen(bucketNumStr, taskId);
    } else {
      String adjustedBucketNum = adjustBucketNumLen(bucketNumStr, m.group(2));
      return (m.group(1) == null ? "" : m.group(1)) + adjustedBucketNum;
    }
  }

  /**
   * Returns strBucketNum with enough 0's prefixing the task ID portion of the String to make it
   * equal in length to taskId
   *
   * @param taskId - the taskId used as a template for length
   * @param strBucketNum - the bucket number of the output, may or may not be prefixed
   * @return
   */
  private static String replaceTaskId(String taskId, String strBucketNum) {
    Matcher m = PREFIXED_TASK_ID_REGEX.matcher(strBucketNum);
    if (!m.matches()) {
      LOG.warn("Unable to determine bucket number from file ID: " + strBucketNum + ". Using " +
          "file ID as bucket number.");
      return adjustBucketNumLen(strBucketNum, taskId);
    } else {
      String adjustedBucketNum = adjustBucketNumLen(m.group(2), taskId);
      return (m.group(1) == null ? "" : m.group(1)) + adjustedBucketNum;
    }
  }

  /**
   * Adds 0's to the beginning of bucketNum until bucketNum and taskId are the same length.
   *
   * @param bucketNum - the bucket number, should not be prefixed
   * @param taskId - the taskId used as a template for length
   * @return
   */
  private static String adjustBucketNumLen(String bucketNum, String taskId) {
    int bucketNumLen = bucketNum.length();
    int taskIdLen = taskId.length();
    StringBuilder s = new StringBuilder();
    for (int i = 0; i < taskIdLen - bucketNumLen; i++) {
      s.append("0");
    }
    s.append(bucketNum);
    return s.toString();
  }

  /**
   * Replace the oldTaskId appearing in the filename by the newTaskId. The string oldTaskId could
   * appear multiple times, we should only replace the last one.
   *
   * @param filename
   * @param oldTaskId
   * @param newTaskId
   * @return
   */
  private static String replaceTaskIdFromFilename(String filename, String oldTaskId,
      String newTaskId) {

    String[] spl = filename.split(oldTaskId);

    if ((spl.length == 0) || (spl.length == 1)) {
      return filename.replaceAll(oldTaskId, newTaskId);
    }

    StringBuilder snew = new StringBuilder();
    for (int idx = 0; idx < spl.length - 1; idx++) {
      if (idx > 0) {
        snew.append(oldTaskId);
      }
      snew.append(spl[idx]);
    }
    snew.append(newTaskId);
    snew.append(spl[spl.length - 1]);
    return snew.toString();
  }

  /**
   * returns null if path is not exist
   */
  public static FileStatus[] listStatusIfExists(Path path, FileSystem fs) throws IOException {
    try {
      return fs.listStatus(path, FileUtils.HIDDEN_FILES_PATH_FILTER);
    } catch (FileNotFoundException e) {
      // FS in hadoop 2.0 throws FNF instead of returning null
      return null;
    }
  }

  public static void mvFileToFinalPath(Path specPath, Configuration hconf,
      boolean success, Logger log, DynamicPartitionCtx dpCtx, FileSinkDesc conf,
      Reporter reporter) throws IOException,
      HiveException {

    FileSystem fs = specPath.getFileSystem(hconf);
    Path tmpPath = Utilities.toTempPath(specPath);
    Path taskTmpPath = Utilities.toTaskTempPath(specPath);
    if (success) {
      FileStatus[] statuses = HiveStatsUtils.getFileStatusRecurse(
          tmpPath, ((dpCtx == null) ? 1 : dpCtx.getNumDPCols()), fs);
      if(statuses != null && statuses.length > 0) {
        PerfLogger perfLogger = SessionState.getPerfLogger();
        perfLogger.PerfLogBegin("FileSinkOperator", "RemoveTempOrDuplicateFiles");
        // remove any tmp file or double-committed output files
        List<Path> emptyBuckets = Utilities.removeTempOrDuplicateFiles(fs, statuses, dpCtx, conf, hconf);
        perfLogger.PerfLogEnd("FileSinkOperator", "RemoveTempOrDuplicateFiles");
        // create empty buckets if necessary
        if (emptyBuckets.size() > 0) {
          perfLogger.PerfLogBegin("FileSinkOperator", "CreateEmptyBuckets");
          createEmptyBuckets(hconf, emptyBuckets, conf, reporter);
          perfLogger.PerfLogEnd("FileSinkOperator", "CreateEmptyBuckets");
        }

        // move to the file destination
        log.info("Moving tmp dir: " + tmpPath + " to: " + specPath);
        perfLogger.PerfLogBegin("FileSinkOperator", "RenameOrMoveFiles");
        Utilities.renameOrMoveFiles(fs, tmpPath, specPath);
        perfLogger.PerfLogEnd("FileSinkOperator", "RenameOrMoveFiles");
      }
    } else {
      fs.delete(tmpPath, true);
    }
    fs.delete(taskTmpPath, true);
  }

  /**
   * Check the existence of buckets according to bucket specification. Create empty buckets if
   * needed.
   *
   * @param hconf
   * @param paths A list of empty buckets to create
   * @param conf The definition of the FileSink.
   * @param reporter The mapreduce reporter object
   * @throws HiveException
   * @throws IOException
   */
  private static void createEmptyBuckets(Configuration hconf, List<Path> paths,
      FileSinkDesc conf, Reporter reporter)
      throws HiveException, IOException {

    JobConf jc;
    if (hconf instanceof JobConf) {
      jc = new JobConf(hconf);
    } else {
      // test code path
      jc = new JobConf(hconf);
    }
    HiveOutputFormat<?, ?> hiveOutputFormat = null;
    Class<? extends Writable> outputClass = null;
    boolean isCompressed = conf.getCompressed();
    TableDesc tableInfo = conf.getTableInfo();
    try {
      Serializer serializer = (Serializer) tableInfo.getDeserializerClass().newInstance();
      serializer.initialize(null, tableInfo.getProperties());
      outputClass = serializer.getSerializedClass();
      hiveOutputFormat = HiveFileFormatUtils.getHiveOutputFormat(hconf, conf.getTableInfo());
    } catch (SerDeException e) {
      throw new HiveException(e);
    } catch (InstantiationException e) {
      throw new HiveException(e);
    } catch (IllegalAccessException e) {
      throw new HiveException(e);
    }

    for (Path path : paths) {
      RecordWriter writer = HiveFileFormatUtils.getRecordWriter(
          jc, hiveOutputFormat, outputClass, isCompressed,
          tableInfo.getProperties(), path, reporter);
      writer.close(false);
      LOG.info("created empty bucket for enforcing bucketing at " + path);
    }
  }

  /**
   * Remove all temporary files and duplicate (double-committed) files from a given directory.
   */
  public static void removeTempOrDuplicateFiles(FileSystem fs, Path path) throws IOException {
    removeTempOrDuplicateFiles(fs, path, null,null,null);
  }

  public static List<Path> removeTempOrDuplicateFiles(FileSystem fs, Path path,
      DynamicPartitionCtx dpCtx, FileSinkDesc conf, Configuration hconf) throws IOException {
    if (path  == null) {
      return null;
    }
    FileStatus[] stats = HiveStatsUtils.getFileStatusRecurse(path,
        ((dpCtx == null) ? 1 : dpCtx.getNumDPCols()), fs);
    return removeTempOrDuplicateFiles(fs, stats, dpCtx, conf, hconf);
  }

  /**
   * Remove all temporary files and duplicate (double-committed) files from a given directory.
   *
   * @return a list of path names corresponding to should-be-created empty buckets.
   */
  public static List<Path> removeTempOrDuplicateFiles(FileSystem fs, FileStatus[] fileStats,
      DynamicPartitionCtx dpCtx, FileSinkDesc conf, Configuration hconf) throws IOException {
    if (fileStats == null) {
      return null;
    }

    List<Path> result = new ArrayList<Path>();
    HashMap<String, FileStatus> taskIDToFile = null;
    if (dpCtx != null) {
      FileStatus parts[] = fileStats;

      for (int i = 0; i < parts.length; ++i) {
        assert parts[i].isDir() : "dynamic partition " + parts[i].getPath()
            + " is not a directory";
        FileStatus[] items = fs.listStatus(parts[i].getPath());

        // remove empty directory since DP insert should not generate empty partitions.
        // empty directories could be generated by crashed Task/ScriptOperator
        if (items.length == 0) {
          if (!fs.delete(parts[i].getPath(), true)) {
            LOG.error("Cannot delete empty directory " + parts[i].getPath());
            throw new IOException("Cannot delete empty directory " + parts[i].getPath());
          }
        }

        taskIDToFile = removeTempOrDuplicateFiles(items, fs);
        // if the table is bucketed and enforce bucketing, we should check and generate all buckets
        if (dpCtx.getNumBuckets() > 0 && taskIDToFile != null && !"tez".equalsIgnoreCase(hconf.get(ConfVars.HIVE_EXECUTION_ENGINE.varname))) {
          // refresh the file list
          items = fs.listStatus(parts[i].getPath());
          // get the missing buckets and generate empty buckets
          String taskID1 = taskIDToFile.keySet().iterator().next();
          Path bucketPath = taskIDToFile.values().iterator().next().getPath();
          for (int j = 0; j < dpCtx.getNumBuckets(); ++j) {
            String taskID2 = replaceTaskId(taskID1, j);
            if (!taskIDToFile.containsKey(taskID2)) {
              // create empty bucket, file name should be derived from taskID2
              URI bucketUri = bucketPath.toUri();
              String path2 = replaceTaskIdFromFilename(bucketUri.getPath().toString(), j);
              result.add(new Path(bucketUri.getScheme(), bucketUri.getAuthority(), path2));
            }
          }
        }
      }
    } else {
      FileStatus[] items = fileStats;
      if (items.length == 0) {
        return result;
      }
      taskIDToFile = removeTempOrDuplicateFiles(items, fs);
      if(taskIDToFile != null && taskIDToFile.size() > 0 && conf != null && conf.getTable() != null
          && (conf.getTable().getNumBuckets() > taskIDToFile.size()) && !"tez".equalsIgnoreCase(hconf.get(ConfVars.HIVE_EXECUTION_ENGINE.varname))) {
          // get the missing buckets and generate empty buckets for non-dynamic partition
        String taskID1 = taskIDToFile.keySet().iterator().next();
        Path bucketPath = taskIDToFile.values().iterator().next().getPath();
        for (int j = 0; j < conf.getTable().getNumBuckets(); ++j) {
          String taskID2 = replaceTaskId(taskID1, j);
          if (!taskIDToFile.containsKey(taskID2)) {
            // create empty bucket, file name should be derived from taskID2
            URI bucketUri = bucketPath.toUri();
            String path2 = replaceTaskIdFromFilename(bucketUri.getPath().toString(), j);
            result.add(new Path(bucketUri.getScheme(), bucketUri.getAuthority(), path2));
          }
        }
      }
    }

    return result;
  }

  public static HashMap<String, FileStatus> removeTempOrDuplicateFiles(FileStatus[] items,
      FileSystem fs) throws IOException {

    if (items == null || fs == null) {
      return null;
    }

    HashMap<String, FileStatus> taskIdToFile = new HashMap<String, FileStatus>();

    for (FileStatus one : items) {
      if (isTempPath(one)) {
        if (!fs.delete(one.getPath(), true)) {
          throw new IOException("Unable to delete tmp file: " + one.getPath());
        }
      } else {
        String taskId = getPrefixedTaskIdFromFilename(one.getPath().getName());
        FileStatus otherFile = taskIdToFile.get(taskId);
        if (otherFile == null) {
          taskIdToFile.put(taskId, one);
        } else {
          // Compare the file sizes of all the attempt files for the same task, the largest win
          // any attempt files could contain partial results (due to task failures or
          // speculative runs), but the largest should be the correct one since the result
          // of a successful run should never be smaller than a failed/speculative run.
          FileStatus toDelete = null;

          // "LOAD .. INTO" and "INSERT INTO" commands will generate files with
          // "_copy_x" suffix. These files are usually read by map tasks and the
          // task output gets written to some tmp path. The output file names will
          // be of format taskId_attemptId. The usual path for all these tasks is
          // srcPath -> taskTmpPath -> tmpPath -> finalPath.
          // But, MergeFileTask can move files directly from src path to final path
          // without copying it to tmp path. In such cases, different files with
          // "_copy_x" suffix will be identified as duplicates (change in value
          // of x is wrongly identified as attempt id) and will be deleted.
          // To avoid that we will ignore files with "_copy_x" suffix from duplicate
          // elimination.
          if (!isCopyFile(one.getPath().getName())) {
            if (otherFile.getLen() >= one.getLen()) {
              toDelete = one;
            } else {
              toDelete = otherFile;
              taskIdToFile.put(taskId, one);
            }
            long len1 = toDelete.getLen();
            long len2 = taskIdToFile.get(taskId).getLen();
            if (!fs.delete(toDelete.getPath(), true)) {
              throw new IOException(
                  "Unable to delete duplicate file: " + toDelete.getPath()
                      + ". Existing file: " +
                      taskIdToFile.get(taskId).getPath());
            } else {
              LOG.warn("Duplicate taskid file removed: " + toDelete.getPath() +
                  " with length "
                  + len1 + ". Existing file: " +
                  taskIdToFile.get(taskId).getPath() + " with length "
                  + len2);
            }
          } else {
            LOG.info(one.getPath() + " file identified as duplicate. This file is" +
                " not deleted as it has copySuffix.");
          }
        }
      }
    }
    return taskIdToFile;
  }

  public static boolean isCopyFile(String filename) {
    String taskId = filename;
    String copyFileSuffix = null;
    int dirEnd = filename.lastIndexOf(Path.SEPARATOR);
    if (dirEnd != -1) {
      taskId = filename.substring(dirEnd + 1);
    }
    Matcher m = COPY_FILE_NAME_TO_TASK_ID_REGEX.matcher(taskId);
    if (!m.matches()) {
      LOG.warn("Unable to verify if file name " + filename + " has _copy_ suffix.");
    } else {
      taskId = m.group(1);
      copyFileSuffix = m.group(4);
    }

    LOG.debug("Filename: " + filename + " TaskId: " + taskId + " CopySuffix: " + copyFileSuffix);
    if (taskId != null && copyFileSuffix != null) {
      return true;
    }

    return false;
  }

  public static String getBucketFileNameFromPathSubString(String bucketName) {
    try {
      return bucketName.split(COPY_KEYWORD)[0];
    } catch (Exception e) {
      e.printStackTrace();
      return bucketName;
    }
  }

  /* compute bucket id from from Split */
  public static int parseSplitBucket(InputSplit split) {
    if (split instanceof FileSplit) {
      return getBucketIdFromFile(((FileSplit) split).getPath().getName());
    }
    // cannot get this for combined splits
    return -1;
  }

  public static int getBucketIdFromFile(String bucketName) {
    Matcher m = PREFIXED_BUCKET_ID_REGEX.matcher(bucketName);
    if (m.matches()) {
      if (m.group(2).isEmpty()) {
        // all zeros
        return m.group(1).isEmpty() ? -1 : 0;
      }
      return Integer.parseInt(m.group(2));
    }
    // Check to see if the bucketName matches the pattern "bucket_([0-9]+).*"
    // This can happen in ACID cases when we have splits on delta files, where the filenames
    // are of the form delta_x_y/bucket_a.
    if (bucketName.startsWith(AcidUtils.BUCKET_PREFIX)) {
      m = AcidUtils.BUCKET_DIGIT_PATTERN.matcher(bucketName);
      if (m.find()) {
          return Integer.parseInt(m.group());
      }
      // Note that legacy bucket digit pattern are being ignored here.
    }
    return -1;
  }

  public static String getNameMessage(Throwable e) {
    return e.getClass().getName() + "(" + e.getMessage() + ")";
  }

  public static String getResourceFiles(Configuration conf, SessionState.ResourceType t) {
    // fill in local files to be added to the task environment
    SessionState ss = SessionState.get();
    Set<String> files = (ss == null) ? null : ss.list_resource(t, null);
    if (files != null) {
      List<String> realFiles = new ArrayList<String>(files.size());
      for (String one : files) {
        try {
          String onefile = realFile(one, conf);
          if (onefile != null) {
            realFiles.add(realFile(one, conf));
          } else {
            LOG.warn("The file " + one + " does not exist.");
          }
        } catch (IOException e) {
          throw new RuntimeException("Cannot validate file " + one + "due to exception: "
              + e.getMessage(), e);
        }
      }
      return StringUtils.join(realFiles, ",");
    } else {
      return "";
    }
  }

  /**
   * get session specified class loader and get current class loader if fall
   *
   * @return
   */
  public static ClassLoader getSessionSpecifiedClassLoader() {
    SessionState state = SessionState.get();
    if (state == null || state.getConf() == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Hive Conf not found or Session not initiated, use thread based class loader instead");
      }
      return JavaUtils.getClassLoader();
    }
    ClassLoader sessionCL = state.getConf().getClassLoader();
    if (sessionCL != null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Use session specified class loader");  //it's normal case
      }
      return sessionCL;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Session specified class loader not found, use thread based class loader");
    }
    return JavaUtils.getClassLoader();
  }

  public static void restoreSessionSpecifiedClassLoader(ClassLoader prev) {
    SessionState state = SessionState.get();
    if (state != null && state.getConf() != null) {
      ClassLoader current = state.getConf().getClassLoader();
      if (current != prev && JavaUtils.closeClassLoadersTo(current, prev)) {
        Thread.currentThread().setContextClassLoader(prev);
        state.getConf().setClassLoader(prev);
      }
    }
  }

  /**
   * Create a URL from a string representing a path to a local file.
   * The path string can be just a path, or can start with file:/, file:///
   * @param onestr  path string
   * @return
   */
  private static URL urlFromPathString(String onestr) {
    URL oneurl = null;
    try {
      if (StringUtils.indexOf(onestr, "file:/") == 0) {
        oneurl = new URL(onestr);
      } else {
        oneurl = new File(onestr).toURL();
      }
    } catch (Exception err) {
      LOG.error("Bad URL " + onestr + ", ignoring path");
    }
    return oneurl;
  }

  private static boolean useExistingClassLoader(ClassLoader cl) {
    if (!(cl instanceof UDFClassLoader)) {
      // Cannot use the same classloader if it is not an instance of {@code UDFClassLoader}
      return false;
    }
    final UDFClassLoader udfClassLoader = (UDFClassLoader) cl;
    if (udfClassLoader.isClosed()) {
      // The classloader may have been closed, Cannot add to the same instance
      return false;
    }
    return true;
  }

  /**
   * Add new elements to the classpath.
   *
   * @param newPaths
   *          Array of classpath elements
   */
  public static ClassLoader addToClassPath(ClassLoader cloader, String[] newPaths) {
    final URLClassLoader loader = (URLClassLoader) cloader;
    if (useExistingClassLoader(cloader)) {
      final UDFClassLoader udfClassLoader = (UDFClassLoader) loader;
      for (String path : newPaths) {
        udfClassLoader.addURL(urlFromPathString(path));
      }
      return udfClassLoader;
    } else {
      return createUDFClassLoader(loader, newPaths);
    }
  }

  public static ClassLoader createUDFClassLoader(URLClassLoader loader, String[] newPaths) {
    final Set<URL> curPathsSet = Sets.newHashSet(loader.getURLs());
    final List<URL> curPaths = Lists.newArrayList(curPathsSet);
    for (String onestr : newPaths) {
      final URL oneurl = urlFromPathString(onestr);
      if (oneurl != null && !curPathsSet.contains(oneurl)) {
        curPaths.add(oneurl);
      }
    }
    return new UDFClassLoader(curPaths.toArray(new URL[0]), loader);
  }

  /**
   * remove elements from the classpath.
   *
   * @param pathsToRemove
   *          Array of classpath elements
   */
  public static void removeFromClassPath(String[] pathsToRemove) throws IOException {
    Thread curThread = Thread.currentThread();
    URLClassLoader loader = (URLClassLoader) curThread.getContextClassLoader();
    Set<URL> newPath = new HashSet<URL>(Arrays.asList(loader.getURLs()));

    for (String onestr : pathsToRemove) {
      URL oneurl = urlFromPathString(onestr);
      if (oneurl != null) {
        newPath.remove(oneurl);
      }
    }
    JavaUtils.closeClassLoader(loader);
   // This loader is closed, remove it from cached registry loaders to avoid removing it again.
    Registry reg = SessionState.getRegistry();
    if(reg != null) {
      reg.removeFromUDFLoaders(loader);
    }

    loader = new UDFClassLoader(newPath.toArray(new URL[0]));
    curThread.setContextClassLoader(loader);
    SessionState.get().getConf().setClassLoader(loader);
  }

  public static String formatBinaryString(byte[] array, int start, int length) {
    StringBuilder sb = new StringBuilder();
    for (int i = start; i < start + length; i++) {
      sb.append("x");
      sb.append(array[i] < 0 ? array[i] + 256 : array[i] + 0);
    }
    return sb.toString();
  }

  public static List<String> getColumnNamesFromSortCols(List<Order> sortCols) {
    List<String> names = new ArrayList<String>();
    for (Order o : sortCols) {
      names.add(o.getCol());
    }
    return names;
  }

  public static List<String> getColumnNamesFromFieldSchema(List<FieldSchema> partCols) {
    List<String> names = new ArrayList<String>();
    for (FieldSchema o : partCols) {
      names.add(o.getName());
    }
    return names;
  }

  public static List<String> getInternalColumnNamesFromSignature(List<ColumnInfo> colInfos) {
    List<String> names = new ArrayList<String>();
    for (ColumnInfo ci : colInfos) {
      names.add(ci.getInternalName());
    }
    return names;
  }

  public static List<String> getColumnNames(Properties props) {
    List<String> names = new ArrayList<String>();
    String colNames = props.getProperty(serdeConstants.LIST_COLUMNS);
    String[] cols = colNames.trim().split(",");
    if (cols != null) {
      for (String col : cols) {
        if (col != null && !col.trim().equals("")) {
          names.add(col);
        }
      }
    }
    return names;
  }

  public static List<String> getColumnTypes(Properties props) {
    List<String> names = new ArrayList<String>();
    String colNames = props.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    ArrayList<TypeInfo> cols = TypeInfoUtils.getTypeInfosFromTypeString(colNames);
    for (TypeInfo col : cols) {
      names.add(col.getTypeName());
    }
    return names;
  }

  /**
   * Extract db and table name from dbtable string, where db and table are separated by "."
   * If there is no db name part, set the current sessions default db
   * @param dbtable
   * @return String array with two elements, first is db name, second is table name
   * @throws HiveException
   */
  public static String[] getDbTableName(String dbtable) throws SemanticException {
    return getDbTableName(SessionState.get().getCurrentDatabase(), dbtable);
  }

  public static String[] getDbTableName(String defaultDb, String dbtable) throws SemanticException {
    if (dbtable == null) {
      return new String[2];
    }
    String[] names =  dbtable.split("\\.");
    switch (names.length) {
      case 2:
        return names;
      case 1:
        return new String [] {defaultDb, dbtable};
      default:
        throw new SemanticException(ErrorMsg.INVALID_TABLE_NAME, dbtable);
    }
  }

  /**
   * Accepts qualified name which is in the form of dbname.tablename and returns dbname from it
   *
   * @param dbTableName
   * @return dbname
   * @throws SemanticException input string is not qualified name
   */
  public static String getDatabaseName(String dbTableName) throws SemanticException {
    String[] split = dbTableName.split("\\.");
    if (split.length != 2) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE_NAME, dbTableName);
    }
    return split[0];
  }

  /**
   * Accepts qualified name which is in the form of dbname.tablename and returns tablename from it
   *
   * @param dbTableName
   * @return tablename
   * @throws SemanticException input string is not qualified name
   */
  public static String getTableName(String dbTableName) throws SemanticException {
    String[] split = dbTableName.split("\\.");
    if (split.length != 2) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE_NAME, dbTableName);
    }
    return split[1];
  }

  public static void validateColumnNames(List<String> colNames, List<String> checkCols)
      throws SemanticException {
    Iterator<String> checkColsIter = checkCols.iterator();
    while (checkColsIter.hasNext()) {
      String toCheck = checkColsIter.next();
      boolean found = false;
      Iterator<String> colNamesIter = colNames.iterator();
      while (colNamesIter.hasNext()) {
        String colName = colNamesIter.next();
        if (toCheck.equalsIgnoreCase(colName)) {
          found = true;
          break;
        }
      }
      if (!found) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg());
      }
    }
  }

  /**
   * Gets the default notification interval to send progress updates to the tracker. Useful for
   * operators that may not output data for a while.
   *
   * @param hconf
   * @return the interval in milliseconds
   */
  public static int getDefaultNotificationInterval(Configuration hconf) {
    int notificationInterval;
    Integer expInterval = Integer.decode(hconf.get("mapred.tasktracker.expiry.interval"));

    if (expInterval != null) {
      notificationInterval = expInterval.intValue() / 2;
    } else {
      // 5 minutes
      notificationInterval = 5 * 60 * 1000;
    }
    return notificationInterval;
  }

  /**
   * Copies the storage handler properties configured for a table descriptor to a runtime job
   * configuration.
   *
   * @param tbl
   *          table descriptor from which to read
   *
   * @param job
   *          configuration which receives configured properties
   */
  public static void copyTableJobPropertiesToConf(TableDesc tbl, Configuration job) {
    Properties tblProperties = tbl.getProperties();
    for(String name: tblProperties.stringPropertyNames()) {
      if (job.get(name) == null) {
        String val = (String) tblProperties.get(name);
        if (val != null) {
          job.set(name, StringEscapeUtils.escapeJava(val));
        }
      }
    }
    Map<String, String> jobProperties = tbl.getJobProperties();
    if (jobProperties == null) {
      return;
    }
    for (Map.Entry<String, String> entry : jobProperties.entrySet()) {
      job.set(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Copies the storage handler proeprites configured for a table descriptor to a runtime job
   * configuration.  This differs from {@link #copyTablePropertiesToConf(org.apache.hadoop.hive.ql.plan.TableDesc, org.apache.hadoop.mapred.JobConf)}
   * in that it does not allow parameters already set in the job to override the values from the
   * table.  This is important for setting the config up for reading,
   * as the job may already have values in it from another table.
   * @param tbl
   * @param job
   */
  public static void copyTablePropertiesToConf(TableDesc tbl, JobConf job) {
    Properties tblProperties = tbl.getProperties();
    for(String name: tblProperties.stringPropertyNames()) {
      String val = (String) tblProperties.get(name);
      if (val != null) {
        job.set(name, StringEscapeUtils.escapeJava(val));
      }
    }
    Map<String, String> jobProperties = tbl.getJobProperties();
    if (jobProperties == null) {
      return;
    }
    for (Map.Entry<String, String> entry : jobProperties.entrySet()) {
      job.set(entry.getKey(), entry.getValue());
    }
  }

  private static final Object INPUT_SUMMARY_LOCK = new Object();

  /**
   * Returns the maximum number of executors required to get file information from several input locations.
   * It checks whether HIVE_EXEC_INPUT_LISTING_MAX_THREADS or DEPRECATED_MAPRED_DFSCLIENT_PARALLELISM_MAX are > 1
   *
   * @param conf Configuration object to get the maximum number of threads.
   * @param inputLocationListSize Number of input locations required to process.
   * @return The maximum number of executors to use.
   */
  @VisibleForTesting
  static int getMaxExecutorsForInputListing(final Configuration conf, int inputLocationListSize) {
    if (inputLocationListSize < 1) return 0;

    int maxExecutors = 1;

    if (inputLocationListSize > 1) {
      int listingMaxThreads = HiveConf.getIntVar(conf, ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS);

      // DEPRECATED_MAPRED_DFSCLIENT_PARALLELISM_MAX must be removed on next Hive version (probably on 3.0).
      // If HIVE_EXEC_INPUT_LISTING_MAX_THREADS is not set, then we check of the deprecated configuration.
      if (listingMaxThreads <= 0) {
        listingMaxThreads = conf.getInt(DEPRECATED_MAPRED_DFSCLIENT_PARALLELISM_MAX, 0);
        if (listingMaxThreads > 0) {
          LOG.warn("Deprecated configuration is used: " + DEPRECATED_MAPRED_DFSCLIENT_PARALLELISM_MAX +
              ". Please use " + ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS.varname);
        }
      }

      if (listingMaxThreads > 1) {
        maxExecutors = Math.min(inputLocationListSize, listingMaxThreads);
      }
    }

    return maxExecutors;
  }

  /**
   * Calculate the total size of input files.
   *
   * @param ctx
   *          the hadoop job context
   * @param work
   *          map reduce job plan
   * @param filter
   *          filter to apply to the input paths before calculating size
   * @return the summary of all the input paths.
   * @throws IOException
   */
  public static ContentSummary getInputSummary(final Context ctx, MapWork work, PathFilter filter)
      throws IOException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.INPUT_SUMMARY);

    long[] summary = {0, 0, 0};

    final Set<Path> pathNeedProcess = new HashSet<>();

    // Since multiple threads could call this method concurrently, locking
    // this method will avoid number of threads out of control.
    synchronized (INPUT_SUMMARY_LOCK) {
      // For each input path, calculate the total size.
      for (Path path : work.getPathToAliases().keySet()) {
        Path p = path;

        if (filter != null && !filter.accept(p)) {
          continue;
        }

        ContentSummary cs = ctx.getCS(path);
        if (cs == null) {
          if (path == null) {
            continue;
          }
          pathNeedProcess.add(path);
        } else {
          summary[0] += cs.getLength();
          summary[1] += cs.getFileCount();
          summary[2] += cs.getDirectoryCount();
        }
      }

      // Process the case when name node call is needed
      final Map<String, ContentSummary> resultMap = new ConcurrentHashMap<String, ContentSummary>();
      ArrayList<Future<?>> results = new ArrayList<Future<?>>();
      final ExecutorService executor;

      int numExecutors = getMaxExecutorsForInputListing(ctx.getConf(), pathNeedProcess.size());
      if (numExecutors > 1) {
        LOG.info("Using " + numExecutors + " threads for getContentSummary");
        executor = Executors.newFixedThreadPool(numExecutors,
            new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("Get-Input-Summary-%d").build());
      } else {
        executor = null;
      }

      HiveInterruptCallback interrup = HiveInterruptUtils.add(new HiveInterruptCallback() {
        @Override
        public void interrupt() {
          for (Path path : pathNeedProcess) {
            try {
              path.getFileSystem(ctx.getConf()).close();
            } catch (IOException ignore) {
                LOG.debug("Failed to close filesystem", ignore);
            }
          }
          if (executor != null) {
            executor.shutdownNow();
          }
        }
      });
      try {
        Configuration conf = ctx.getConf();
        JobConf jobConf = new JobConf(conf);
        for (Path path : pathNeedProcess) {
          final Path p = path;
          final String pathStr = path.toString();
          // All threads share the same Configuration and JobConf based on the
          // assumption that they are thread safe if only read operations are
          // executed. It is not stated in Hadoop's javadoc, the sourcce codes
          // clearly showed that they made efforts for it and we believe it is
          // thread safe. Will revisit this piece of codes if we find the assumption
          // is not correct.
          final Configuration myConf = conf;
          final JobConf myJobConf = jobConf;
          final Map<String, Operator<?>> aliasToWork = work.getAliasToWork();
          final Map<Path, ArrayList<String>> pathToAlias = work.getPathToAliases();
          final PartitionDesc partDesc = work.getPathToPartitionInfo().get(p);
          Runnable r = new Runnable() {
            @Override
            public void run() {
              try {
                Class<? extends InputFormat> inputFormatCls = partDesc
                    .getInputFileFormatClass();
                InputFormat inputFormatObj = HiveInputFormat.getInputFormatFromCache(
                    inputFormatCls, myJobConf);
                if (inputFormatObj instanceof ContentSummaryInputFormat) {
                  ContentSummaryInputFormat cs = (ContentSummaryInputFormat) inputFormatObj;
                  resultMap.put(pathStr, cs.getContentSummary(p, myJobConf));
                  return;
                }

                String metaTableStorage = null;
                if (partDesc.getTableDesc() != null &&
                    partDesc.getTableDesc().getProperties() != null) {
                  metaTableStorage = partDesc.getTableDesc().getProperties()
                      .getProperty(hive_metastoreConstants.META_TABLE_STORAGE, null);
                }
                if (partDesc.getProperties() != null) {
                  metaTableStorage = partDesc.getProperties()
                      .getProperty(hive_metastoreConstants.META_TABLE_STORAGE, metaTableStorage);
                }

                HiveStorageHandler handler = HiveUtils.getStorageHandler(myConf, metaTableStorage);
                if (handler instanceof InputEstimator) {
                  long total = 0;
                  TableDesc tableDesc = partDesc.getTableDesc();
                  InputEstimator estimator = (InputEstimator) handler;
                  for (String alias : HiveFileFormatUtils.doGetAliasesFromPath(pathToAlias, p)) {
                    JobConf jobConf = new JobConf(myJobConf);
                    TableScanOperator scanOp = (TableScanOperator) aliasToWork.get(alias);
                    Utilities.setColumnNameList(jobConf, scanOp, true);
                    Utilities.setColumnTypeList(jobConf, scanOp, true);
                    PlanUtils.configureInputJobPropertiesForStorageHandler(tableDesc);
                    Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf);
                    total += estimator.estimate(jobConf, scanOp, -1).getTotalLength();
                  }
                  resultMap.put(pathStr, new ContentSummary(total, -1, -1));
                } else {
                  // todo: should nullify summary for non-native tables,
                  // not to be selected as a mapjoin target
                  FileSystem fs = p.getFileSystem(myConf);
                  resultMap.put(pathStr, fs.getContentSummary(p));
                }
              } catch (Exception e) {
                // We safely ignore this exception for summary data.
                // We don't update the cache to protect it from polluting other
                // usages. The worst case is that IOException will always be
                // retried for another getInputSummary(), which is fine as
                // IOException is not considered as a common case.
                LOG.info("Cannot get size of " + pathStr + ". Safely ignored.");
              }
            }
          };

          if (executor == null) {
            r.run();
          } else {
            Future<?> result = executor.submit(r);
            results.add(result);
          }
        }

        if (executor != null) {
          for (Future<?> result : results) {
            boolean executorDone = false;
            do {
              try {
                result.get();
                executorDone = true;
              } catch (InterruptedException e) {
                LOG.info("Interrupted when waiting threads: ", e);
                Thread.currentThread().interrupt();
                break;
              } catch (ExecutionException e) {
                throw new IOException(e);
              }
            } while (!executorDone);
          }
          executor.shutdown();
        }
        HiveInterruptUtils.checkInterrupted();
        for (Map.Entry<String, ContentSummary> entry : resultMap.entrySet()) {
          ContentSummary cs = entry.getValue();

          summary[0] += cs.getLength();
          summary[1] += cs.getFileCount();
          summary[2] += cs.getDirectoryCount();

          ctx.addCS(entry.getKey(), cs);
          LOG.info("Cache Content Summary for " + entry.getKey() + " length: " + cs.getLength()
              + " file count: "
              + cs.getFileCount() + " directory count: " + cs.getDirectoryCount());
        }

        perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.INPUT_SUMMARY);
        return new ContentSummary(summary[0], summary[1], summary[2]);
      } finally {
        HiveInterruptUtils.remove(interrup);
      }
    }
  }

  public static long sumOf(Map<String, Long> aliasToSize, Set<String> aliases) {
    return sumOfExcept(aliasToSize, aliases, null);
  }

  // return sum of lengths except some aliases. returns -1 if any of other alias is unknown
  public static long sumOfExcept(Map<String, Long> aliasToSize,
      Set<String> aliases, Set<String> excepts) {
    long total = 0;
    for (String alias : aliases) {
      if (excepts != null && excepts.contains(alias)) {
        continue;
      }
      Long size = aliasToSize.get(alias);
      if (size == null) {
        return -1;
      }
      total += size;
    }
    return total;
  }

  public static boolean isEmptyPath(JobConf job, Path dirPath, Context ctx)
      throws Exception {
    if (ctx != null) {
      ContentSummary cs = ctx.getCS(dirPath);
      if (cs != null) {
        LOG.info("Content Summary " + dirPath + "length: " + cs.getLength() + " num files: "
            + cs.getFileCount() + " num directories: " + cs.getDirectoryCount());
        return (cs.getLength() == 0 && cs.getFileCount() == 0 && cs.getDirectoryCount() <= 1);
      } else {
        LOG.info("Content Summary not cached for " + dirPath);
      }
    }
    return isEmptyPath(job, dirPath);
  }

  public static boolean isEmptyPath(Configuration job, Path dirPath) throws IOException {
    FileSystem inpFs = dirPath.getFileSystem(job);
    try {
      FileStatus[] fStats = inpFs.listStatus(dirPath, FileUtils.HIDDEN_FILES_PATH_FILTER);
      if (fStats.length > 0) {
        return false;
      }
    } catch(FileNotFoundException fnf) {
      return true;
    }
    return true;
  }

  public static List<TezTask> getTezTasks(List<Task<? extends Serializable>> tasks) {
    List<TezTask> tezTasks = new ArrayList<TezTask>();
    if (tasks != null) {
      getTezTasks(tasks, tezTasks);
    }
    return tezTasks;
  }

  private static void getTezTasks(List<Task<? extends Serializable>> tasks, List<TezTask> tezTasks) {
    for (Task<? extends Serializable> task : tasks) {
      if (task instanceof TezTask && !tezTasks.contains(task)) {
        tezTasks.add((TezTask) task);
      }

      if (task.getDependentTasks() != null) {
        getTezTasks(task.getDependentTasks(), tezTasks);
      }
    }
  }

  public static List<SparkTask> getSparkTasks(List<Task<? extends Serializable>> tasks) {
    List<SparkTask> sparkTasks = new ArrayList<SparkTask>();
    if (tasks != null) {
      getSparkTasks(tasks, sparkTasks);
    }
    return sparkTasks;
  }

  private static void getSparkTasks(List<Task<? extends Serializable>> tasks,
    List<SparkTask> sparkTasks) {
    for (Task<? extends Serializable> task : tasks) {
      if (task instanceof SparkTask && !sparkTasks.contains(task)) {
        sparkTasks.add((SparkTask) task);
      }

      if (task.getDependentTasks() != null) {
        getSparkTasks(task.getDependentTasks(), sparkTasks);
      }
    }
  }

  public static List<ExecDriver> getMRTasks(List<Task<? extends Serializable>> tasks) {
    List<ExecDriver> mrTasks = new ArrayList<ExecDriver>();
    if (tasks != null) {
      getMRTasks(tasks, mrTasks);
    }
    return mrTasks;
  }

  private static void getMRTasks(List<Task<? extends Serializable>> tasks, List<ExecDriver> mrTasks) {
    for (Task<? extends Serializable> task : tasks) {
      if (task instanceof ExecDriver && !mrTasks.contains(task)) {
        mrTasks.add((ExecDriver) task);
      }

      if (task.getDependentTasks() != null) {
        getMRTasks(task.getDependentTasks(), mrTasks);
      }
    }
  }

  /**
   * Construct a list of full partition spec from Dynamic Partition Context and the directory names
   * corresponding to these dynamic partitions.
   */
  public static List<LinkedHashMap<String, String>> getFullDPSpecs(Configuration conf,
      DynamicPartitionCtx dpCtx) throws HiveException {

    try {
      Path loadPath = dpCtx.getRootPath();
      FileSystem fs = loadPath.getFileSystem(conf);
      int numDPCols = dpCtx.getNumDPCols();
      FileStatus[] status = HiveStatsUtils.getFileStatusRecurse(loadPath, numDPCols, fs);

      if (status.length == 0) {
        LOG.warn("No partition is generated by dynamic partitioning");
        return null;
      }

      // partial partition specification
      Map<String, String> partSpec = dpCtx.getPartSpec();

      // list of full partition specification
      List<LinkedHashMap<String, String>> fullPartSpecs = new ArrayList<LinkedHashMap<String, String>>();

      // for each dynamically created DP directory, construct a full partition spec
      // and load the partition based on that
      for (int i = 0; i < status.length; ++i) {
        // get the dynamically created directory
        Path partPath = status[i].getPath();
        assert fs.getFileStatus(partPath).isDir() : "partitions " + partPath
            + " is not a directory !";

        // generate a full partition specification
        LinkedHashMap<String, String> fullPartSpec = new LinkedHashMap<String, String>(partSpec);
        Warehouse.makeSpecFromName(fullPartSpec, partPath);
        fullPartSpecs.add(fullPartSpec);
      }
      return fullPartSpecs;
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  public static StatsPublisher getStatsPublisher(JobConf jc) {
    StatsFactory factory = StatsFactory.newFactory(jc);
    return factory == null ? null : factory.getStatsPublisher();
  }

  public static String join(String... elements) {
    StringBuilder builder = new StringBuilder();
    for (String element : elements) {
      if (element == null || element.isEmpty()) {
        continue;
      }
      builder.append(element);
      if (!element.endsWith(Path.SEPARATOR)) {
        builder.append(Path.SEPARATOR);
      }
    }
    return builder.toString();
  }

  public static void setColumnNameList(JobConf jobConf, RowSchema rowSchema) {
    setColumnNameList(jobConf, rowSchema, false);
  }

  public static void setColumnNameList(JobConf jobConf, RowSchema rowSchema, boolean excludeVCs) {
    if (rowSchema == null) {
      return;
    }
    StringBuilder columnNames = new StringBuilder();
    for (ColumnInfo colInfo : rowSchema.getSignature()) {
      if (excludeVCs && colInfo.getIsVirtualCol()) {
        continue;
      }
      if (columnNames.length() > 0) {
        columnNames.append(",");
      }
      columnNames.append(colInfo.getInternalName());
    }
    String columnNamesString = columnNames.toString();
    jobConf.set(serdeConstants.LIST_COLUMNS, columnNamesString);
  }

  public static void setColumnNameList(JobConf jobConf, Operator op) {
    setColumnNameList(jobConf, op, false);
  }

  public static void setColumnNameList(JobConf jobConf, Operator op, boolean excludeVCs) {
    RowSchema rowSchema = op.getSchema();
    setColumnNameList(jobConf, rowSchema, excludeVCs);
  }

  public static void setColumnTypeList(JobConf jobConf, RowSchema rowSchema) {
    setColumnTypeList(jobConf, rowSchema, false);
  }

  public static void setColumnTypeList(JobConf jobConf, RowSchema rowSchema, boolean excludeVCs) {
    if (rowSchema == null) {
      return;
    }
    StringBuilder columnTypes = new StringBuilder();
    for (ColumnInfo colInfo : rowSchema.getSignature()) {
      if (excludeVCs && colInfo.getIsVirtualCol()) {
        continue;
      }
      if (columnTypes.length() > 0) {
        columnTypes.append(",");
      }
      columnTypes.append(colInfo.getTypeName());
    }
    String columnTypesString = columnTypes.toString();
    jobConf.set(serdeConstants.LIST_COLUMN_TYPES, columnTypesString);
  }

  public static void setColumnTypeList(JobConf jobConf, Operator op) {
    setColumnTypeList(jobConf, op, false);
  }

  public static void setColumnTypeList(JobConf jobConf, Operator op, boolean excludeVCs) {
    RowSchema rowSchema = op.getSchema();
    setColumnTypeList(jobConf, rowSchema, excludeVCs);
  }

  public static final String suffix = ".hashtable";

  public static Path generatePath(Path basePath, String dumpFilePrefix,
      Byte tag, String bigBucketFileName) {
    return new Path(basePath, "MapJoin-" + dumpFilePrefix + tag +
      "-" + bigBucketFileName + suffix);
  }

  public static String generateFileName(Byte tag, String bigBucketFileName) {
    String fileName = new String("MapJoin-" + tag + "-" + bigBucketFileName + suffix);
    return fileName;
  }

  public static Path generateTmpPath(Path basePath, String id) {
    return new Path(basePath, "HashTable-" + id);
  }

  public static Path generateTarPath(Path basePath, String filename) {
    return new Path(basePath, filename + ".tar.gz");
  }

  public static String generateTarFileName(String name) {
    return name + ".tar.gz";
  }

  public static String generatePath(Path baseURI, String filename) {
    String path = new String(baseURI + Path.SEPARATOR + filename);
    return path;
  }

  public static String now() {
    Calendar cal = Calendar.getInstance();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    return sdf.format(cal.getTime());
  }

  public static double showTime(long time) {
    double result = (double) time / (double) 1000;
    return result;
  }

  /**
   * The check here is kind of not clean. It first use a for loop to go through
   * all input formats, and choose the ones that extend ReworkMapredInputFormat
   * to a set. And finally go through the ReworkMapredInputFormat set, and call
   * rework for each one.
   *
   * Technically all these can be avoided if all Hive's input formats can share
   * a same interface. As in today's hive and Hadoop, it is not possible because
   * a lot of Hive's input formats are in Hadoop's code. And most of Hadoop's
   * input formats just extend InputFormat interface.
   *
   * @param task
   * @param reworkMapredWork
   * @param conf
   * @throws SemanticException
   */
  public static void reworkMapRedWork(Task<? extends Serializable> task,
      boolean reworkMapredWork, HiveConf conf) throws SemanticException {
    if (reworkMapredWork && (task instanceof MapRedTask)) {
      try {
        MapredWork mapredWork = ((MapRedTask) task).getWork();
        Set<Class<? extends InputFormat>> reworkInputFormats = new HashSet<Class<? extends InputFormat>>();
        for (PartitionDesc part : mapredWork.getMapWork().getPathToPartitionInfo().values()) {
          Class<? extends InputFormat> inputFormatCls = part
              .getInputFileFormatClass();
          if (ReworkMapredInputFormat.class.isAssignableFrom(inputFormatCls)) {
            reworkInputFormats.add(inputFormatCls);
          }
        }

        if (reworkInputFormats.size() > 0) {
          for (Class<? extends InputFormat> inputFormatCls : reworkInputFormats) {
            ReworkMapredInputFormat inst = (ReworkMapredInputFormat) ReflectionUtil
                .newInstance(inputFormatCls, null);
            inst.rework(conf, mapredWork);
          }
        }
      } catch (IOException e) {
        throw new SemanticException(e);
      }
    }
  }

  public static class SQLCommand<T> {
    public T run(PreparedStatement stmt) throws SQLException {
      return null;
    }
  }

  /**
   * Retry SQL execution with random backoff (same as the one implemented in HDFS-767).
   * This function only retries when the SQL query throws a SQLTransientException (which
   * might be able to succeed with a simple retry). It doesn't retry when the exception
   * is a SQLRecoverableException or SQLNonTransientException. For SQLRecoverableException
   * the caller needs to reconnect to the database and restart the whole transaction.
   *
   * @param cmd the SQL command
   * @param stmt the prepared statement of SQL.
   * @param baseWindow  The base time window (in milliseconds) before the next retry.
   * see {@link #getRandomWaitTime} for details.
   * @param maxRetries the maximum # of retries when getting a SQLTransientException.
   * @throws SQLException throws SQLRecoverableException or SQLNonTransientException the
   * first time it is caught, or SQLTransientException when the maxRetries has reached.
   */
  public static <T> T executeWithRetry(SQLCommand<T> cmd, PreparedStatement stmt,
      long baseWindow, int maxRetries)  throws SQLException {

    Random r = new Random();
    T result = null;

    // retry with # of maxRetries before throwing exception
    for (int failures = 0; ; failures++) {
      try {
        result = cmd.run(stmt);
        return result;
      } catch (SQLTransientException e) {
        LOG.warn("Failure and retry #" + failures +  " with exception " + e.getMessage());
        if (failures >= maxRetries) {
          throw e;
        }
        long waitTime = getRandomWaitTime(baseWindow, failures, r);
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException iex) {
         }
      } catch (SQLException e) {
        // throw other types of SQLExceptions (SQLNonTransientException / SQLRecoverableException)
        throw e;
      }
    }
  }

  /**
   * Retry connecting to a database with random backoff (same as the one implemented in HDFS-767).
   * This function only retries when the SQL query throws a SQLTransientException (which
   * might be able to succeed with a simple retry). It doesn't retry when the exception
   * is a SQLRecoverableException or SQLNonTransientException. For SQLRecoverableException
   * the caller needs to reconnect to the database and restart the whole transaction.
   *
   * @param connectionString the JDBC connection string.
   * @param waitWindow  The base time window (in milliseconds) before the next retry.
   * see {@link #getRandomWaitTime} for details.
   * @param maxRetries the maximum # of retries when getting a SQLTransientException.
   * @throws SQLException throws SQLRecoverableException or SQLNonTransientException the
   * first time it is caught, or SQLTransientException when the maxRetries has reached.
   */
  public static Connection connectWithRetry(String connectionString,
      long waitWindow, int maxRetries) throws SQLException {

    Random r = new Random();

    // retry with # of maxRetries before throwing exception
    for (int failures = 0; ; failures++) {
      try {
        Connection conn = DriverManager.getConnection(connectionString);
        return conn;
      } catch (SQLTransientException e) {
        if (failures >= maxRetries) {
          LOG.error("Error during JDBC connection. " + e);
          throw e;
        }
        long waitTime = Utilities.getRandomWaitTime(waitWindow, failures, r);
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException e1) {
        }
      } catch (SQLException e) {
        // just throw other types (SQLNonTransientException / SQLRecoverableException)
        throw e;
      }
    }
  }

  /**
   * Retry preparing a SQL statement with random backoff (same as the one implemented in HDFS-767).
   * This function only retries when the SQL query throws a SQLTransientException (which
   * might be able to succeed with a simple retry). It doesn't retry when the exception
   * is a SQLRecoverableException or SQLNonTransientException. For SQLRecoverableException
   * the caller needs to reconnect to the database and restart the whole transaction.
   *
   * @param conn a JDBC connection.
   * @param stmt the SQL statement to be prepared.
   * @param waitWindow  The base time window (in milliseconds) before the next retry.
   * see {@link #getRandomWaitTime} for details.
   * @param maxRetries the maximum # of retries when getting a SQLTransientException.
   * @throws SQLException throws SQLRecoverableException or SQLNonTransientException the
   * first time it is caught, or SQLTransientException when the maxRetries has reached.
   */
  public static PreparedStatement prepareWithRetry(Connection conn, String stmt,
      long waitWindow, int maxRetries) throws SQLException {

    Random r = new Random();

    // retry with # of maxRetries before throwing exception
    for (int failures = 0; ; failures++) {
      try {
        return conn.prepareStatement(stmt);
      } catch (SQLTransientException e) {
        if (failures >= maxRetries) {
          LOG.error("Error preparing JDBC Statement " + stmt + " :" + e);
          throw e;
        }
        long waitTime = Utilities.getRandomWaitTime(waitWindow, failures, r);
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException e1) {
        }
      } catch (SQLException e) {
        // just throw other types (SQLNonTransientException / SQLRecoverableException)
        throw e;
      }
    }
  }

  public static void setQueryTimeout(java.sql.Statement stmt, int timeout) throws SQLException {
    if (timeout < 0) {
      LOG.info("Invalid query timeout " + timeout);
      return;
    }
    try {
      stmt.setQueryTimeout(timeout);
    } catch (SQLException e) {
      String message = e.getMessage() == null ? null : e.getMessage().toLowerCase();
      if (e instanceof SQLFeatureNotSupportedException ||
         (message != null && (message.contains("implemented") || message.contains("supported")))) {
        LOG.info("setQueryTimeout is not supported");
        return;
      }
      throw e;
    }
  }

  /**
   * Introducing a random factor to the wait time before another retry.
   * The wait time is dependent on # of failures and a random factor.
   * At the first time of getting an exception , the wait time
   * is a random number between 0..baseWindow msec. If the first retry
   * still fails, we will wait baseWindow msec grace period before the 2nd retry.
   * Also at the second retry, the waiting window is expanded to 2*baseWindow msec
   * alleviating the request rate from the server. Similarly the 3rd retry
   * will wait 2*baseWindow msec. grace period before retry and the waiting window is
   * expanded to 3*baseWindow msec and so on.
   * @param baseWindow the base waiting window.
   * @param failures number of failures so far.
   * @param r a random generator.
   * @return number of milliseconds for the next wait time.
   */
  public static long getRandomWaitTime(long baseWindow, int failures, Random r) {
    return (long) (
          baseWindow * failures +     // grace period for the last round of attempt
          baseWindow * (failures + 1) * r.nextDouble()); // expanding time window for each failure
  }

  public static final char sqlEscapeChar = '\\';

  /**
   * Escape the '_', '%', as well as the escape characters inside the string key.
   * @param key the string that will be used for the SQL LIKE operator.
   * @return a string with escaped '_' and '%'.
   */
  public static String escapeSqlLike(String key) {
    StringBuilder sb = new StringBuilder(key.length());
    for (char c: key.toCharArray()) {
      switch(c) {
      case '_':
      case '%':
      case sqlEscapeChar:
        sb.append(sqlEscapeChar);
        // fall through
      default:
        sb.append(c);
        break;
      }
    }
    return sb.toString();
  }

  /**
   * Format number of milliseconds to strings
   *
   * @param msec milliseconds
   * @return a formatted string like "x days y hours z minutes a seconds b msec"
   */
  public static String formatMsecToStr(long msec) {
    long day = -1, hour = -1, minute = -1, second = -1;
    long ms = msec % 1000;
    long timeLeft = msec / 1000;
    if (timeLeft > 0) {
      second = timeLeft % 60;
      timeLeft /= 60;
      if (timeLeft > 0) {
        minute = timeLeft % 60;
        timeLeft /= 60;
        if (timeLeft > 0) {
          hour = timeLeft % 24;
          day = timeLeft / 24;
        }
      }
    }
    StringBuilder sb = new StringBuilder();
    if (day != -1) {
      sb.append(day + " days ");
    }
    if (hour != -1) {
      sb.append(hour + " hours ");
    }
    if (minute != -1) {
      sb.append(minute + " minutes ");
    }
    if (second != -1) {
      sb.append(second + " seconds ");
    }
    sb.append(ms + " msec");

    return sb.toString();
  }

  /**
   * Estimate the number of reducers needed for this job, based on job input,
   * and configuration parameters.
   *
   * The output of this method should only be used if the output of this
   * MapRedTask is not being used to populate a bucketed table and the user
   * has not specified the number of reducers to use.
   *
   * @return the number of reducers.
   */
  public static int estimateNumberOfReducers(HiveConf conf, ContentSummary inputSummary,
                                             MapWork work, boolean finalMapRed) throws IOException {
    long bytesPerReducer = conf.getLongVar(HiveConf.ConfVars.BYTESPERREDUCER);
    int maxReducers = conf.getIntVar(HiveConf.ConfVars.MAXREDUCERS);

    double samplePercentage = getHighestSamplePercentage(work);
    long totalInputFileSize = getTotalInputFileSize(inputSummary, work, samplePercentage);

    // if all inputs are sampled, we should shrink the size of reducers accordingly.
    if (totalInputFileSize != inputSummary.getLength()) {
      LOG.info("BytesPerReducer=" + bytesPerReducer + " maxReducers="
          + maxReducers + " estimated totalInputFileSize=" + totalInputFileSize);
    } else {
      LOG.info("BytesPerReducer=" + bytesPerReducer + " maxReducers="
        + maxReducers + " totalInputFileSize=" + totalInputFileSize);
    }

    // If this map reduce job writes final data to a table and bucketing is being inferred,
    // and the user has configured Hive to do this, make sure the number of reducers is a
    // power of two
    boolean powersOfTwo = conf.getBoolVar(HiveConf.ConfVars.HIVE_INFER_BUCKET_SORT_NUM_BUCKETS_POWER_TWO) &&
        finalMapRed && !work.getBucketedColsByDirectory().isEmpty();

    return estimateReducers(totalInputFileSize, bytesPerReducer, maxReducers, powersOfTwo);
  }

  public static int estimateReducers(long totalInputFileSize, long bytesPerReducer,
      int maxReducers, boolean powersOfTwo) {
    double bytes = Math.max(totalInputFileSize, bytesPerReducer);
    int reducers = (int) Math.ceil(bytes / bytesPerReducer);
    reducers = Math.max(1, reducers);
    reducers = Math.min(maxReducers, reducers);

    int reducersLog = (int)(Math.log(reducers) / Math.log(2)) + 1;
    int reducersPowerTwo = (int)Math.pow(2, reducersLog);

    if (powersOfTwo) {
      // If the original number of reducers was a power of two, use that
      if (reducersPowerTwo / 2 == reducers) {
        // nothing to do
      } else if (reducersPowerTwo > maxReducers) {
        // If the next power of two greater than the original number of reducers is greater
        // than the max number of reducers, use the preceding power of two, which is strictly
        // less than the original number of reducers and hence the max
        reducers = reducersPowerTwo / 2;
      } else {
        // Otherwise use the smallest power of two greater than the original number of reducers
        reducers = reducersPowerTwo;
      }
    }
    return reducers;
  }

  /**
   * Computes the total input file size. If block sampling was used it will scale this
   * value by the highest sample percentage (as an estimate for input).
   *
   * @param inputSummary
   * @param work
   * @param highestSamplePercentage
   * @return estimated total input size for job
   */
  public static long getTotalInputFileSize (ContentSummary inputSummary, MapWork work,
      double highestSamplePercentage) {
    long totalInputFileSize = inputSummary.getLength();
    if (work.getNameToSplitSample() == null || work.getNameToSplitSample().isEmpty()) {
      // If percentage block sampling wasn't used, we don't need to do any estimation
      return totalInputFileSize;
    }

    if (highestSamplePercentage >= 0) {
      totalInputFileSize = Math.min((long) (totalInputFileSize * (highestSamplePercentage / 100D))
          , totalInputFileSize);
    }
    return totalInputFileSize;
  }

  /**
   * Computes the total number of input files. If block sampling was used it will scale this
   * value by the highest sample percentage (as an estimate for # input files).
   *
   * @param inputSummary
   * @param work
   * @param highestSamplePercentage
   * @return
   */
  public static long getTotalInputNumFiles (ContentSummary inputSummary, MapWork work,
      double highestSamplePercentage) {
    long totalInputNumFiles = inputSummary.getFileCount();
    if (work.getNameToSplitSample() == null || work.getNameToSplitSample().isEmpty()) {
      // If percentage block sampling wasn't used, we don't need to do any estimation
      return totalInputNumFiles;
    }

    if (highestSamplePercentage >= 0) {
      totalInputNumFiles = Math.min((long) (totalInputNumFiles * (highestSamplePercentage / 100D))
          , totalInputNumFiles);
    }
    return totalInputNumFiles;
  }

  /**
   * Returns the highest sample percentage of any alias in the given MapWork
   */
  public static double getHighestSamplePercentage (MapWork work) {
    double highestSamplePercentage = 0;
    for (String alias : work.getAliasToWork().keySet()) {
      if (work.getNameToSplitSample().containsKey(alias)) {
        Double rate = work.getNameToSplitSample().get(alias).getPercent();
        if (rate != null && rate > highestSamplePercentage) {
          highestSamplePercentage = rate;
        }
      } else {
        highestSamplePercentage = -1;
        break;
      }
    }

    return highestSamplePercentage;
  }

  /**
   * On Tez we're not creating dummy files when getting/setting input paths.
   * We let Tez handle the situation. We're also setting the paths in the AM
   * so we don't want to depend on scratch dir and context.
   */
  public static List<Path> getInputPathsTez(JobConf job, MapWork work) throws Exception {
    String scratchDir = job.get(DagUtils.TEZ_TMP_DIR_KEY);

    List<Path> paths = getInputPaths(job, work, new Path(scratchDir), null, true);

    return paths;
  }

  /**
   * Computes a list of all input paths needed to compute the given MapWork. All aliases
   * are considered and a merged list of input paths is returned. If any input path points
   * to an empty table or partition a dummy file in the scratch dir is instead created and
   * added to the list. This is needed to avoid special casing the operator pipeline for
   * these cases.
   *
   * @param job JobConf used to run the job
   * @param work MapWork encapsulating the info about the task
   * @param hiveScratchDir The tmp dir used to create dummy files if needed
   * @param ctx Context object
   * @return List of paths to process for the given MapWork
   * @throws Exception
   */
  public static List<Path> getInputPaths(JobConf job, MapWork work, Path hiveScratchDir,
      Context ctx, boolean skipDummy) throws Exception {

    Set<Path> pathsProcessed = new HashSet<Path>();
    List<Path> pathsToAdd = new LinkedList<Path>();
    // AliasToWork contains all the aliases
    for (String alias : work.getAliasToWork().keySet()) {
      LOG.info("Processing alias " + alias);

      // The alias may not have any path
      boolean isEmptyTable = true;
      boolean hasLogged = false;
      // Note: this copies the list because createDummyFileForEmptyPartition may modify the map.
      for (Path file : new LinkedList<Path>(work.getPathToAliases().keySet())) {
        List<String> aliases = work.getPathToAliases().get(file);
        if (aliases.contains(alias)) {
          if (file != null) {
            isEmptyTable = false;
          } else {
            LOG.warn("Found a null path for alias " + alias);
            continue;
          }

          // Multiple aliases can point to the same path - it should be
          // processed only once
          if (pathsProcessed.contains(file)) {
            continue;
          }

          StringInternUtils.internUriStringsInPath(file);
          pathsProcessed.add(file);

          if (LOG.isDebugEnabled()) {
            LOG.debug("Adding input file " + file);
          } else if (!hasLogged) {
            hasLogged = true;
            LOG.info("Adding " + work.getPathToAliases().size()
                + " inputs; the first input is " + file);
          }
          pathsToAdd.add(file);
        }
      }

      // If the query references non-existent partitions
      // We need to add a empty file, it is not acceptable to change the
      // operator tree
      // Consider the query:
      // select * from (select count(1) from T union all select count(1) from
      // T2) x;
      // If T is empty and T2 contains 100 rows, the user expects: 0, 100 (2
      // rows)
      if (isEmptyTable && !skipDummy) {
        pathsToAdd.add(createDummyFileForEmptyTable(job, work, hiveScratchDir, alias));
      }
    }

    ExecutorService pool = null;
    int numExecutors = getMaxExecutorsForInputListing(job, pathsToAdd.size());
    if (numExecutors > 1) {
      pool = Executors.newFixedThreadPool(numExecutors,
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Get-Input-Paths-%d").build());
    }

    List<Path> finalPathsToAdd = new LinkedList<>();
    List<Future<Path>> futures = new LinkedList<>();
    for (final Path path : pathsToAdd) {
      if (pool == null) {
        finalPathsToAdd.add(new GetInputPathsCallable(path, job, work, hiveScratchDir, ctx, skipDummy).call());
      } else {
        futures.add(pool.submit(new GetInputPathsCallable(path, job, work, hiveScratchDir, ctx, skipDummy)));
      }
    }

    if (pool != null) {
      for (Future<Path> future : futures) {
        finalPathsToAdd.add(future.get());
      }
    }

    return finalPathsToAdd;
  }

  private static class GetInputPathsCallable implements Callable<Path> {

    private final Path path;
    private final JobConf job;
    private final MapWork work;
    private final Path hiveScratchDir;
    private final Context ctx;
    private final boolean skipDummy;

    private GetInputPathsCallable(Path path, JobConf job, MapWork work, Path hiveScratchDir,
      Context ctx, boolean skipDummy) {
      this.path = path;
      this.job = job;
      this.work = work;
      this.hiveScratchDir = hiveScratchDir;
      this.ctx = ctx;
      this.skipDummy = skipDummy;
    }

    @Override
    public Path call() throws Exception {
      if (!this.skipDummy && isEmptyPath(this.job, this.path, this.ctx)) {
        return createDummyFileForEmptyPartition(this.path, this.job, this.work, this.hiveScratchDir);
      }
      return this.path;
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Path createEmptyFile(Path hiveScratchDir,
      HiveOutputFormat outFileFormat, JobConf job,
      Properties props, boolean dummyRow)
          throws IOException, InstantiationException, IllegalAccessException {

    // create a dummy empty file in a new directory
    String newDir = hiveScratchDir + Path.SEPARATOR + UUID.randomUUID().toString();
    Path newPath = new Path(newDir);
    FileSystem fs = newPath.getFileSystem(job);
    fs.mkdirs(newPath);
    //Qualify the path against the file system. The user configured path might contain default port which is skipped
    //in the file status. This makes sure that all paths which goes into PathToPartitionInfo are always listed status
    //file path.
    newPath = fs.makeQualified(newPath);
    String newFile = newDir + Path.SEPARATOR + "emptyFile";
    Path newFilePath = new Path(newFile);

    RecordWriter recWriter = outFileFormat.getHiveRecordWriter(job, newFilePath,
        Text.class, false, props, null);
    if (dummyRow) {
      // empty files are omitted at CombineHiveInputFormat.
      // for meta-data only query, it effectively makes partition columns disappear..
      // this could be fixed by other methods, but this seemed to be the most easy (HIVEV-2955)
      recWriter.write(new Text("empty"));  // written via HiveIgnoreKeyTextOutputFormat
    }
    recWriter.close(false);

    return StringInternUtils.internUriStringsInPath(newPath);
  }

  @SuppressWarnings("rawtypes")
  private static Path createDummyFileForEmptyPartition(Path path, JobConf job, MapWork work,
      Path hiveScratchDir)
          throws Exception {

    String strPath = path.toString();

    // The input file does not exist, replace it by a empty file
    PartitionDesc partDesc = work.getPathToPartitionInfo().get(path);
    if (partDesc.getTableDesc().isNonNative()) {
      // if this isn't a hive table we can't create an empty file for it.
      return path;
    }

    Properties props = SerDeUtils.createOverlayedProperties(
        partDesc.getTableDesc().getProperties(), partDesc.getProperties());
    HiveOutputFormat outFileFormat = HiveFileFormatUtils.getHiveOutputFormat(job, partDesc);

    boolean oneRow = partDesc.getInputFileFormatClass() == OneNullRowInputFormat.class;

    Path newPath = createEmptyFile(hiveScratchDir, outFileFormat, job, props, oneRow);

    if (LOG.isInfoEnabled()) {
      LOG.info("Changed input file " + strPath + " to empty file " + newPath + " (" + oneRow + ")");
    }

    // update the work

    work.addPathToAlias(newPath, work.getPathToAliases().get(path));
    work.removePathToAlias(path);

    work.removePathToPartitionInfo(path);
    work.addPathToPartitionInfo(newPath, partDesc);

    return newPath;
  }

  @SuppressWarnings("rawtypes")
  private static Path createDummyFileForEmptyTable(JobConf job, MapWork work,
      Path hiveScratchDir, String alias)
          throws Exception {

    TableDesc tableDesc = work.getAliasToPartnInfo().get(alias).getTableDesc();
    if (tableDesc.isNonNative()) {
      // if it does not need native storage, we can't create an empty file for it.
      return null;
    }

    Properties props = tableDesc.getProperties();
    HiveOutputFormat outFileFormat = HiveFileFormatUtils.getHiveOutputFormat(job, tableDesc);

    Path newPath = createEmptyFile(hiveScratchDir, outFileFormat, job, props, false);

    if (LOG.isInfoEnabled()) {
      LOG.info("Changed input file for alias " + alias + " to " + newPath);
    }

    // update the work

    LinkedHashMap<Path, ArrayList<String>> pathToAliases = work.getPathToAliases();
    ArrayList<String> newList = new ArrayList<String>();
    newList.add(alias);
    pathToAliases.put(newPath, newList);

    work.setPathToAliases(pathToAliases);

    PartitionDesc pDesc = work.getAliasToPartnInfo().get(alias).clone();
    work.addPathToPartitionInfo(newPath, pDesc);

    return newPath;
  }

  /**
   * setInputPaths add all the paths in the provided list to the Job conf object
   * as input paths for the job.
   *
   * @param job
   * @param pathsToAdd
   */
  public static void setInputPaths(JobConf job, List<Path> pathsToAdd) {

    Path[] addedPaths = FileInputFormat.getInputPaths(job);
    if (addedPaths == null) {
      addedPaths = new Path[0];
    }

    Path[] combined = new Path[addedPaths.length + pathsToAdd.size()];
    System.arraycopy(addedPaths, 0, combined, 0, addedPaths.length);

    int i = 0;
    for(Path p: pathsToAdd) {
      combined[addedPaths.length + (i++)] = p;
    }
    FileInputFormat.setInputPaths(job, combined);
  }

  /**
   * Set hive input format, and input format file if necessary.
   */
  public static void setInputAttributes(Configuration conf, MapWork mWork) {
    HiveConf.ConfVars var = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez") ?
      HiveConf.ConfVars.HIVETEZINPUTFORMAT : HiveConf.ConfVars.HIVEINPUTFORMAT;
    if (mWork.getInputformat() != null) {
      HiveConf.setVar(conf, var, mWork.getInputformat());
    }
    if (mWork.getIndexIntermediateFile() != null) {
      conf.set(ConfVars.HIVE_INDEX_COMPACT_FILE.varname, mWork.getIndexIntermediateFile());
      conf.set(ConfVars.HIVE_INDEX_BLOCKFILTER_FILE.varname, mWork.getIndexIntermediateFile());
    }

    // Intentionally overwrites anything the user may have put here
    conf.setBoolean("hive.input.format.sorted", mWork.isInputFormatSorted());
  }

  /**
   * Hive uses tmp directories to capture the output of each FileSinkOperator.
   * This method creates all necessary tmp directories for FileSinks in the Mapwork.
   *
   * @param conf Used to get the right FileSystem
   * @param mWork Used to find FileSinkOperators
   * @throws IOException
   */
  public static void createTmpDirs(Configuration conf, MapWork mWork)
      throws IOException {

    Map<Path, ArrayList<String>> pa = mWork.getPathToAliases();
    if (pa != null) {
      // common case: 1 table scan per map-work
      // rare case: smb joins
      HashSet<String> aliases = new HashSet<String>(1);
      List<Operator<? extends OperatorDesc>> ops =
          new ArrayList<Operator<? extends OperatorDesc>>();
      for (List<String> ls : pa.values()) {
        for (String a : ls) {
          aliases.add(a);
        }
      }
      for (String a : aliases) {
        ops.add(mWork.getAliasToWork().get(a));
      }
      createTmpDirs(conf, ops);
    }
  }

  /**
   * Hive uses tmp directories to capture the output of each FileSinkOperator.
   * This method creates all necessary tmp directories for FileSinks in the ReduceWork.
   *
   * @param conf Used to get the right FileSystem
   * @param rWork Used to find FileSinkOperators
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static void createTmpDirs(Configuration conf, ReduceWork rWork)
      throws IOException {
    if (rWork == null) {
      return;
    }
    List<Operator<? extends OperatorDesc>> ops
      = new LinkedList<Operator<? extends OperatorDesc>>();
    ops.add(rWork.getReducer());
    createTmpDirs(conf, ops);
  }

  private static void createTmpDirs(Configuration conf,
      List<Operator<? extends OperatorDesc>> ops) throws IOException {

    while (!ops.isEmpty()) {
      Operator<? extends OperatorDesc> op = ops.remove(0);

      if (op instanceof FileSinkOperator) {
        FileSinkDesc fdesc = ((FileSinkOperator) op).getConf();
        Path tempDir = fdesc.getDirName();

        if (tempDir != null) {
          Path tempPath = Utilities.toTempPath(tempDir);
          FileSystem fs = tempPath.getFileSystem(conf);
          fs.mkdirs(tempPath);
        }
      }

      if (op.getChildOperators() != null) {
        ops.addAll(op.getChildOperators());
      }
    }
  }

  public static boolean createDirsWithPermission(Configuration conf, Path mkdirPath,
      FsPermission fsPermission, boolean recursive) throws IOException {
    String origUmask = null;
    LOG.debug("Create dirs " + mkdirPath + " with permission " + fsPermission + " recursive "
        + recursive);
    if (recursive) {
      origUmask = conf.get(FsPermission.UMASK_LABEL);
      // this umask is required because by default the hdfs mask is 022 resulting in
      // all parents getting the fsPermission & !(022) permission instead of fsPermission
      conf.set(FsPermission.UMASK_LABEL, "000");
    }
    FileSystem fs = ShimLoader.getHadoopShims().getNonCachedFileSystem(mkdirPath.toUri(), conf);
    boolean retval = false;
    try {
      retval = fs.mkdirs(mkdirPath, fsPermission);
      resetUmaskInConf(conf, recursive, origUmask);
    } catch (IOException ioe) {
      resetUmaskInConf(conf, recursive, origUmask);
      throw ioe;
    } finally {
      IOUtils.closeStream(fs);
    }
    return retval;
  }

  private static void resetUmaskInConf(Configuration conf, boolean unsetUmask, String origUmask) {
    if (unsetUmask) {
      if (origUmask != null) {
        conf.set(FsPermission.UMASK_LABEL, origUmask);
      } else {
        conf.unset(FsPermission.UMASK_LABEL);
      }
    }
  }

  /**
   * Returns true if a plan is both configured for vectorized execution
   * and the node is vectorized and the Input File Format is marked VectorizedInputFileFormat.
   *
   * The plan may be configured for vectorization
   * but vectorization disallowed eg. for FetchOperator execution.
   */
  public static boolean getUseVectorizedInputFileFormat(Configuration conf) {
    if (conf.get(VECTOR_MODE) != null) {
      // this code path is necessary, because with HS2 and client
      // side split generation we end up not finding the map work.
      // This is because of thread local madness (tez split
      // generation is multi-threaded - HS2 plan cache uses thread
      // locals).
      return
          conf.getBoolean(VECTOR_MODE, false) &&
          conf.getBoolean(USE_VECTORIZED_INPUT_FILE_FORMAT, false);
    } else {
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED) &&
        Utilities.getPlanPath(conf) != null) {
        MapWork mapWork = Utilities.getMapWork(conf);
        return (mapWork.getVectorMode() && mapWork.getUseVectorizedInputFileFormat());
      } else {
        return false;
      }
    }
  }


  public static boolean getUseVectorizedInputFileFormat(Configuration conf, MapWork mapWork) {
    return HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED) &&
        mapWork.getVectorMode() &&
        mapWork.getUseVectorizedInputFileFormat();
  }

  /**
   * @param conf
   * @return the configured VectorizedRowBatchCtx for a MapWork task.
   */
  public static VectorizedRowBatchCtx getVectorizedRowBatchCtx(Configuration conf) {
    VectorizedRowBatchCtx result = null;
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED) &&
        Utilities.getPlanPath(conf) != null) {
      MapWork mapWork = Utilities.getMapWork(conf);
      if (mapWork != null && mapWork.getVectorMode()) {
        result = mapWork.getVectorizedRowBatchCtx();
      }
    }
    return result;
  }

  public static void clearWorkMapForConf(Configuration conf) {
    // Remove cached query plans for the current query only
    Path mapPath = getPlanPath(conf, MAP_PLAN_NAME);
    Path reducePath = getPlanPath(conf, REDUCE_PLAN_NAME);
    if (mapPath != null) {
      gWorkMap.get(conf).remove(mapPath);
    }
    if (reducePath != null) {
      gWorkMap.get(conf).remove(reducePath);
    }
    // TODO: should this also clean merge work?
  }

  public static void clearWorkMap(Configuration conf) {
    gWorkMap.get(conf).clear();
  }

  /**
   * Create a temp dir in specified baseDir
   * This can go away once hive moves to support only JDK 7
   *  and can use Files.createTempDirectory
   *  Guava Files.createTempDir() does not take a base dir
   * @param baseDir - directory under which new temp dir will be created
   * @return File object for new temp dir
   */
  public static File createTempDir(String baseDir){
    //try creating the temp dir MAX_ATTEMPTS times
    final int MAX_ATTEMPS = 30;
    for(int i = 0; i < MAX_ATTEMPS; i++){
      //pick a random file name
      String tempDirName = "tmp_" + ((int)(100000 * Math.random()));

      //return if dir could successfully be created with that file name
      File tempDir = new File(baseDir, tempDirName);
      if(tempDir.mkdir()){
        return tempDir;
      }
    }
    throw new IllegalStateException("Failed to create a temp dir under "
    + baseDir + " Giving up after " + MAX_ATTEMPS + " attempts");

  }

  /**
   * Skip header lines in the table file when reading the record.
   *
   * @param currRecReader
   *          Record reader.
   *
   * @param headerCount
   *          Header line number of the table files.
   *
   * @param key
   *          Key of current reading record.
   *
   * @param value
   *          Value of current reading record.
   *
   * @return Return true if there are 0 or more records left in the file
   *         after skipping all headers, otherwise return false.
   */
  public static boolean skipHeader(RecordReader<WritableComparable, Writable> currRecReader,
      int headerCount, WritableComparable key, Writable value) throws IOException {
    while (headerCount > 0) {
      if (!currRecReader.next(key, value))
        return false;
      headerCount--;
    }
    return true;
  }

  /**
   * Get header line count for a table.
   *
   * @param table
   *          Table description for target table.
   *
   */
  public static int getHeaderCount(TableDesc table) throws IOException {
    int headerCount;
    try {
      headerCount = Integer.parseInt(table.getProperties().getProperty(serdeConstants.HEADER_COUNT, "0"));
    } catch (NumberFormatException nfe) {
      throw new IOException(nfe);
    }
    return headerCount;
  }

  /**
   * Get footer line count for a table.
   *
   * @param table
   *          Table description for target table.
   *
   * @param job
   *          Job configuration for current job.
   */
  public static int getFooterCount(TableDesc table, JobConf job) throws IOException {
    int footerCount;
    try {
      footerCount = Integer.parseInt(table.getProperties().getProperty(serdeConstants.FOOTER_COUNT, "0"));
      if (footerCount > HiveConf.getIntVar(job, HiveConf.ConfVars.HIVE_FILE_MAX_FOOTER)) {
        throw new IOException("footer number exceeds the limit defined in hive.file.max.footer");
      }
    } catch (NumberFormatException nfe) {

      // Footer line number must be set as an integer.
      throw new IOException(nfe);
    }
    return footerCount;
  }

  /**
   * Convert path to qualified path.
   *
   * @param conf
   *          Hive configuration.
   * @param path
   *          Path to convert.
   * @return Qualified path
   */
  public static String getQualifiedPath(HiveConf conf, Path path) throws HiveException {
    FileSystem fs;
    if (path == null) {
      return null;
    }

    try {
      fs = path.getFileSystem(conf);
      return fs.makeQualified(path).toString();
    }
    catch (IOException e) {
      throw new HiveException(e);
    }
  }

  /**
   * Checks if current hive script was executed with non-default namenode
   *
   * @return True/False
   */
  public static boolean isDefaultNameNode(HiveConf conf) {
    return !conf.getChangedProperties().containsKey(
        CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
  }

  /**
   * Checks if the current HiveServer2 logging operation level is >= PERFORMANCE.
   * @param conf Hive configuration.
   * @return true if current HiveServer2 logging operation level is >= PERFORMANCE.
   * Else, false.
   */
  public static boolean isPerfOrAboveLogging(HiveConf conf) {
    String loggingLevel = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LEVEL);
    return conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED) &&
      (loggingLevel.equalsIgnoreCase("PERFORMANCE") || loggingLevel.equalsIgnoreCase("VERBOSE"));
  }

  /**
   * Returns the full path to the Jar containing the class. It always return a JAR.
   *
   * @param klass
   *          class.
   *
   * @return path to the Jar containing the class.
   */
  @SuppressWarnings("rawtypes")
  public static String jarFinderGetJar(Class klass) {
    Preconditions.checkNotNull(klass, "klass");
    ClassLoader loader = klass.getClassLoader();
    if (loader != null) {
      String class_file = klass.getName().replaceAll("\\.", "/") + ".class";
      try {
        for (Enumeration itr = loader.getResources(class_file); itr.hasMoreElements();) {
          URL url = (URL) itr.nextElement();
          String path = url.getPath();
          if (path.startsWith("file:")) {
            path = path.substring("file:".length());
          }
          path = URLDecoder.decode(path, "UTF-8");
          if ("jar".equals(url.getProtocol())) {
            path = URLDecoder.decode(path, "UTF-8");
            return path.replaceAll("!.*$", "");
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  public static int getDPColOffset(FileSinkDesc conf) {

    if (conf.getWriteType() == AcidUtils.Operation.DELETE) {
      // For deletes, there is only ROW__ID in non-partitioning, non-bucketing columns.
      //See : UpdateDeleteSemanticAnalyzer::reparseAndSuperAnalyze() for details.
      return 1;
    } else if (conf.getWriteType() == AcidUtils.Operation.UPDATE) {
      // For updates, ROW__ID is an extra column at index 0.
      //See : UpdateDeleteSemanticAnalyzer::reparseAndSuperAnalyze() for details.
      return getColumnNames(conf.getTableInfo().getProperties()).size() + 1;
    } else {
      return getColumnNames(conf.getTableInfo().getProperties()).size();
    }

  }

  public static List<String> getStatsTmpDirs(BaseWork work, Configuration conf) {

    List<String> statsTmpDirs = new ArrayList<>();
    if (!StatsSetupConst.StatDB.fs.name().equalsIgnoreCase(HiveConf.getVar(conf, ConfVars.HIVESTATSDBCLASS))) {
      // no-op for non-fs stats collection
      return statsTmpDirs;
    }
    // if its auto-stats gather for inserts or CTAS, stats dir will be in FileSink
    Set<Operator<? extends OperatorDesc>> ops = work.getAllLeafOperators();
    if (work instanceof MapWork) {
      // if its an anlayze statement, stats dir will be in TableScan
      ops.addAll(work.getAllRootOperators());
    }
    for (Operator<? extends OperatorDesc> op : ops) {
      OperatorDesc desc = op.getConf();
      String statsTmpDir = null;
      if (desc instanceof FileSinkDesc) {
         statsTmpDir = ((FileSinkDesc)desc).getStatsTmpDir();
      } else if (desc instanceof TableScanDesc) {
        statsTmpDir = ((TableScanDesc) desc).getTmpStatsDir();
      }
      if (statsTmpDir != null && !statsTmpDir.isEmpty()) {
        statsTmpDirs.add(statsTmpDir);
      }
    }
    return statsTmpDirs;
  }

  public static boolean isSchemaEvolutionEnabled(Configuration conf, boolean isAcid) {
    return isAcid || HiveConf.getBoolVar(conf, ConfVars.HIVE_SCHEMA_EVOLUTION);
  }

  public static boolean isInputFileFormatSelfDescribing(PartitionDesc pd) {
    Class<?> inputFormatClass = pd.getInputFileFormatClass();
    return SelfDescribingInputFormatInterface.class.isAssignableFrom(inputFormatClass);
  }

  public static boolean isInputFileFormatVectorized(PartitionDesc pd) {
    Class<?> inputFormatClass = pd.getInputFileFormatClass();
    return VectorizedInputFormatInterface.class.isAssignableFrom(inputFormatClass);
  }

  public static void addSchemaEvolutionToTableScanOperator(Table table,
      TableScanOperator tableScanOp) {
    String colNames = MetaStoreUtils.getColumnNamesFromFieldSchema(table.getSd().getCols());
    String colTypes = MetaStoreUtils.getColumnTypesFromFieldSchema(table.getSd().getCols());
    tableScanOp.setSchemaEvolution(colNames, colTypes);
  }

  public static void addSchemaEvolutionToTableScanOperator(StructObjectInspector structOI,
      TableScanOperator tableScanOp) {
    String colNames = ObjectInspectorUtils.getFieldNames(structOI);
    String colTypes = ObjectInspectorUtils.getFieldTypes(structOI);
    tableScanOp.setSchemaEvolution(colNames, colTypes);
  }

  public static void unsetSchemaEvolution(Configuration conf) {
    conf.unset(IOConstants.SCHEMA_EVOLUTION_COLUMNS);
    conf.unset(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES);
  }

  public static void addTableSchemaToConf(Configuration conf,
      TableScanOperator tableScanOp) {
    String schemaEvolutionColumns = tableScanOp.getSchemaEvolutionColumns();
    if (schemaEvolutionColumns != null) {
      conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, tableScanOp.getSchemaEvolutionColumns());
      conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, tableScanOp.getSchemaEvolutionColumnsTypes());
    } else {
      LOG.info("schema.evolution.columns and schema.evolution.columns.types not available");
    }
  }

  /**
   * Create row key and value object inspectors for reduce vectorization.
   * The row object inspector used by ReduceWork needs to be a **standard**
   * struct object inspector, not just any struct object inspector.
   * @param keyInspector
   * @param valueInspector
   * @return OI
   * @throws HiveException
   */
  public static StandardStructObjectInspector constructVectorizedReduceRowOI(
      StructObjectInspector keyInspector, StructObjectInspector valueInspector)
          throws HiveException {

    ArrayList<String> colNames = new ArrayList<String>();
    ArrayList<ObjectInspector> ois = new ArrayList<ObjectInspector>();
    List<? extends StructField> fields = keyInspector.getAllStructFieldRefs();
    for (StructField field: fields) {
      colNames.add(Utilities.ReduceField.KEY.toString() + "." + field.getFieldName());
      ois.add(field.getFieldObjectInspector());
    }
    fields = valueInspector.getAllStructFieldRefs();
    for (StructField field: fields) {
      colNames.add(Utilities.ReduceField.VALUE.toString() + "." + field.getFieldName());
      ois.add(field.getFieldObjectInspector());
    }
    StandardStructObjectInspector rowObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(colNames, ois);

    return rowObjectInspector;
  }

  /**
   * Check if LLAP IO supports the column type that is being read
   * @param conf - configuration
   * @return false for types not supported by vectorization, true otherwise
   */
  public static boolean checkVectorizerSupportedTypes(final Configuration conf) {
    final String[] readColumnNames = ColumnProjectionUtils.getReadColumnNames(conf);
    final String columnNames = conf.get(serdeConstants.LIST_COLUMNS);
    final String columnTypes = conf.get(serdeConstants.LIST_COLUMN_TYPES);
    if (columnNames == null || columnTypes == null || columnNames.isEmpty() ||
        columnTypes.isEmpty()) {
      LOG.warn("Column names ({}) or types ({}) is null. Skipping type checking for LLAP IO.",
          columnNames, columnTypes);
      return true;
    }
    final List<String> allColumnNames = Lists.newArrayList(columnNames.split(","));
    final List<TypeInfo> typeInfos = TypeInfoUtils.getTypeInfosFromTypeString(columnTypes);
    final List<String> allColumnTypes = TypeInfoUtils.getTypeStringsFromTypeInfo(typeInfos);
    return checkVectorizerSupportedTypes(Lists.newArrayList(readColumnNames), allColumnNames,
        allColumnTypes);
  }

  /**
   * Check if LLAP IO supports the column type that is being read
   * @param readColumnNames - columns that will be read from the table/partition
   * @param allColumnNames - all columns
   * @param allColumnTypes - all column types
   * @return false for types not supported by vectorization, true otherwise
   */
  public static boolean checkVectorizerSupportedTypes(final List<String> readColumnNames,
      final List<String> allColumnNames, final List<String> allColumnTypes) {
    final String[] readColumnTypes = getReadColumnTypes(readColumnNames, allColumnNames,
        allColumnTypes);

    if (readColumnTypes != null) {
      for (String readColumnType : readColumnTypes) {
        if (readColumnType != null) {
          if (!Vectorizer.validateDataType(readColumnType,
              VectorExpressionDescriptor.Mode.PROJECTION)) {
            LOG.warn("Unsupported column type encountered ({}). Disabling LLAP IO.",
                readColumnType);
            return false;
          }
        }
      }
    } else {
      LOG.warn("readColumnTypes is null. Skipping type checking for LLAP IO. " +
          "readColumnNames: {} allColumnNames: {} allColumnTypes: {} readColumnTypes: {}",
          readColumnNames, allColumnNames, allColumnTypes, readColumnTypes);
    }
    return true;
  }

  private static String[] getReadColumnTypes(final List<String> readColumnNames,
      final List<String> allColumnNames, final List<String> allColumnTypes) {
    if (readColumnNames == null || allColumnNames == null || allColumnTypes == null ||
        readColumnNames.isEmpty() || allColumnNames.isEmpty() || allColumnTypes.isEmpty()) {
      return null;
    }
    Map<String, String> columnNameToType = new HashMap<>();
    List<TypeInfo> types = TypeInfoUtils.typeInfosFromTypeNames(allColumnTypes);
    if (allColumnNames.size() != types.size()) {
      LOG.warn("Column names count does not match column types count." +
              " ColumnNames: {} [{}] ColumnTypes: {} [{}]", allColumnNames, allColumnNames.size(),
          allColumnTypes, types.size());
      return null;
    }

    for (int i = 0; i < allColumnNames.size(); i++) {
      columnNameToType.put(allColumnNames.get(i), types.get(i).toString());
    }

    String[] result = new String[readColumnNames.size()];
    for (int i = 0; i < readColumnNames.size(); i++) {
      result[i] = columnNameToType.get(readColumnNames.get(i));
    }
    return result;
  }

  public static String humanReadableByteCount(long bytes) {
    int unit = 1000; // use binary units instead?
    if (bytes < unit) {
      return bytes + "B";
    }
    int exp = (int) (Math.log(bytes) / Math.log(unit));
    String suffix = "KMGTPE".charAt(exp-1) + "";
    return String.format("%.2f%sB", bytes / Math.pow(unit, exp), suffix);
  }


  public static String getAclStringWithHiveModification(Configuration tezConf,
                                                        String propertyName,
                                                        boolean addHs2User,
                                                        String user,
                                                        String hs2User) throws
      IOException {

    // Start with initial ACLs
    ACLConfigurationParser aclConf =
        new ACLConfigurationParser(tezConf, propertyName);

    // Always give access to the user
    aclConf.addAllowedUser(user);

    // Give access to the process user if the config is set.
    if (addHs2User && hs2User != null) {
      aclConf.addAllowedUser(hs2User);
    }
    return aclConf.toAclString();
  }
}
