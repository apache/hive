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

import java.beans.DefaultPersistenceDelegate;
import java.beans.Encoder;
import java.beans.ExceptionListener;
import java.beans.Expression;
import java.beans.Statement;
import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.HiveInterruptCallback;
import org.apache.hadoop.hive.common.HiveInterruptUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.ContentSummaryInputFormat;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.ReworkMapredInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils.ExpressionTypes;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Shell;

/**
 * Utilities.
 *
 */
@SuppressWarnings("nls")
public final class Utilities {

  /**
   * The object in the reducer are composed of these top level fields.
   */

  public static String HADOOP_LOCAL_FS = "file:///";

  /**
   * ReduceField.
   *
   */
  public static enum ReduceField {
    KEY, VALUE, ALIAS
  };

  private Utilities() {
    // prevent instantiation
  }

  private static Map<String, MapredWork> gWorkMap = Collections
      .synchronizedMap(new HashMap<String, MapredWork>());
  private static final Log LOG = LogFactory.getLog(Utilities.class.getName());

  public static void clearMapRedWork(Configuration job) {
    try {
      Path planPath = new Path(HiveConf.getVar(job, HiveConf.ConfVars.PLAN));
      FileSystem fs = planPath.getFileSystem(job);
      if (fs.exists(planPath)) {
        try {
          fs.delete(planPath, true);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    } catch (Exception e) {
    } finally {
      // where a single process works with multiple plans - we must clear
      // the cache before working with the next plan.
      String jobID = getHiveJobID(job);
      if (jobID != null) {
        gWorkMap.remove(jobID);
      }
    }
  }

  public static MapredWork getMapRedWork(Configuration job) {
    MapredWork gWork = null;
    try {
      String jobID = getHiveJobID(job);
      assert jobID != null;
      gWork = gWorkMap.get(jobID);
      if (gWork == null) {
        String jtConf = ShimLoader.getHadoopShims().getJobLauncherRpcAddress(job);
        String path;
        if (jtConf.equals("local")) {
          String planPath = HiveConf.getVar(job, HiveConf.ConfVars.PLAN);
          path = new Path(planPath).toUri().getPath();
        } else {
          path = "HIVE_PLAN" + jobID;
        }
        InputStream in = new FileInputStream(path);
        MapredWork ret = deserializeMapRedWork(in, job);
        gWork = ret;
        gWork.initialize();
        gWorkMap.put(jobID, gWork);
      }
      return (gWork);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
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

  /**
   * Java 1.5 workaround. From http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=5015403
   */
  public static class EnumDelegate extends DefaultPersistenceDelegate {
    @Override
    protected Expression instantiate(Object oldInstance, Encoder out) {
      return new Expression(Enum.class, "valueOf", new Object[] {oldInstance.getClass(),
          ((Enum<?>) oldInstance).name()});
    }

    @Override
    protected boolean mutatesTo(Object oldInstance, Object newInstance) {
      return oldInstance == newInstance;
    }
  }

  public static class MapDelegate extends DefaultPersistenceDelegate {
    @Override
    protected Expression instantiate(Object oldInstance, Encoder out) {
      Map oldMap = (Map) oldInstance;
      HashMap newMap = new HashMap(oldMap);
      return new Expression(newMap, HashMap.class, "new", new Object[] {});
    }

    @Override
    protected boolean mutatesTo(Object oldInstance, Object newInstance) {
      return false;
    }

    @Override
    protected void initialize(Class<?> type, Object oldInstance, Object newInstance, Encoder out) {
      java.util.Collection oldO = (java.util.Collection) oldInstance;
      java.util.Collection newO = (java.util.Collection) newInstance;

      if (newO.size() != 0) {
        out.writeStatement(new Statement(oldInstance, "clear", new Object[] {}));
      }
      for (Iterator i = oldO.iterator(); i.hasNext();) {
        out.writeStatement(new Statement(oldInstance, "add", new Object[] {i.next()}));
      }
    }
  }

  public static class SetDelegate extends DefaultPersistenceDelegate {
    @Override
    protected Expression instantiate(Object oldInstance, Encoder out) {
      Set oldSet = (Set) oldInstance;
      HashSet newSet = new HashSet(oldSet);
      return new Expression(newSet, HashSet.class, "new", new Object[] {});
    }

    @Override
    protected boolean mutatesTo(Object oldInstance, Object newInstance) {
      return false;
    }

    @Override
    protected void initialize(Class<?> type, Object oldInstance, Object newInstance, Encoder out) {
      java.util.Collection oldO = (java.util.Collection) oldInstance;
      java.util.Collection newO = (java.util.Collection) newInstance;

      if (newO.size() != 0) {
        out.writeStatement(new Statement(oldInstance, "clear", new Object[] {}));
      }
      for (Iterator i = oldO.iterator(); i.hasNext();) {
        out.writeStatement(new Statement(oldInstance, "add", new Object[] {i.next()}));
      }
    }

  }

  public static class ListDelegate extends DefaultPersistenceDelegate {
    @Override
    protected Expression instantiate(Object oldInstance, Encoder out) {
      List oldList = (List) oldInstance;
      ArrayList newList = new ArrayList(oldList);
      return new Expression(newList, ArrayList.class, "new", new Object[] {});
    }

    @Override
    protected boolean mutatesTo(Object oldInstance, Object newInstance) {
      return false;
    }

    @Override
    protected void initialize(Class<?> type, Object oldInstance, Object newInstance, Encoder out) {
      java.util.Collection oldO = (java.util.Collection) oldInstance;
      java.util.Collection newO = (java.util.Collection) newInstance;

      if (newO.size() != 0) {
        out.writeStatement(new Statement(oldInstance, "clear", new Object[] {}));
      }
      for (Iterator i = oldO.iterator(); i.hasNext();) {
        out.writeStatement(new Statement(oldInstance, "add", new Object[] {i.next()}));
      }
    }

  }

  public static void setMapRedWork(Configuration job, MapredWork w, String hiveScratchDir) {
    try {

      // this is the unique job ID, which is kept in JobConf as part of the plan file name
      String jobID = UUID.randomUUID().toString();
      Path planPath = new Path(hiveScratchDir, jobID);
      HiveConf.setVar(job, HiveConf.ConfVars.PLAN, planPath.toUri().toString());

      // use the default file system of the job
      FileSystem fs = planPath.getFileSystem(job);
      FSDataOutputStream out = fs.create(planPath);
      serializeMapRedWork(w, out);

      // Serialize the plan to the default hdfs instance
      // Except for hadoop local mode execution where we should be
      // able to get the plan directly from the cache
      if (!ShimLoader.getHadoopShims().isLocalMode(job)) {
        // Set up distributed cache
        DistributedCache.createSymlink(job);
        String uriWithLink = planPath.toUri().toString() + "#HIVE_PLAN" + jobID;
        DistributedCache.addCacheFile(new URI(uriWithLink), job);

        // set replication of the plan file to a high number. we use the same
        // replication factor as used by the hadoop jobclient for job.xml etc.
        short replication = (short) job.getInt("mapred.submit.replication", 10);
        fs.setReplication(planPath, replication);
      }

      // Cache the plan in this process
      w.initialize();
      gWorkMap.put(jobID, w);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static String getHiveJobID(Configuration job) {
    String planPath = HiveConf.getVar(job, HiveConf.ConfVars.PLAN);
    if (planPath != null) {
      return (new Path(planPath)).getName();
    }
    return null;
  }

  public static String serializeExpression(ExprNodeDesc expr) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    XMLEncoder encoder = new XMLEncoder(baos);
    try {
      encoder.writeObject(expr);
    } finally {
      encoder.close();
    }
    try {
      return baos.toString("UTF-8");
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException("UTF-8 support required", ex);
    }
  }

  public static ExprNodeDesc deserializeExpression(String s, Configuration conf) {
    byte[] bytes;
    try {
      bytes = s.getBytes("UTF-8");
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException("UTF-8 support required", ex);
    }

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

    XMLDecoder decoder = new XMLDecoder(bais, null, null);
    try {
      ExprNodeDesc expr = (ExprNodeDesc) decoder.readObject();
      return expr;
    } finally {
      decoder.close();
    }
  }

  /**
   * Serialize a single Task.
   */
  public static void serializeTasks(Task<? extends Serializable> t, OutputStream out) {
    XMLEncoder e = null;
    try {
      e = new XMLEncoder(out);
      // workaround for java 1.5
      e.setPersistenceDelegate(ExpressionTypes.class, new EnumDelegate());
      e.setPersistenceDelegate(GroupByDesc.Mode.class, new EnumDelegate());
      e.setPersistenceDelegate(Operator.ProgressCounter.class, new EnumDelegate());

      e.writeObject(t);
    } finally {
      if (null != e) {
        e.close();
      }
    }
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

  /**
   * Serialize the whole query plan.
   */
  public static void serializeQueryPlan(QueryPlan plan, OutputStream out) {
    XMLEncoder e = new XMLEncoder(out);
    e.setExceptionListener(new ExceptionListener() {
      public void exceptionThrown(Exception e) {
        LOG.warn(org.apache.hadoop.util.StringUtils.stringifyException(e));
        throw new RuntimeException("Cannot serialize the query plan", e);
      }
    });
    // workaround for java 1.5
    e.setPersistenceDelegate(ExpressionTypes.class, new EnumDelegate());
    e.setPersistenceDelegate(GroupByDesc.Mode.class, new EnumDelegate());
    e.setPersistenceDelegate(Operator.ProgressCounter.class, new EnumDelegate());

    e.setPersistenceDelegate(org.datanucleus.sco.backed.Map.class, new MapDelegate());
    e.setPersistenceDelegate(org.datanucleus.sco.backed.List.class, new ListDelegate());

    e.writeObject(plan);
    e.close();
  }

  /**
   * Deserialize the whole query plan.
   */
  public static QueryPlan deserializeQueryPlan(InputStream in, Configuration conf) {
    XMLDecoder d = null;
    try {
      d = new XMLDecoder(in, null, null);
      QueryPlan ret = (QueryPlan) d.readObject();
      return (ret);
    } finally {
      if (null != d) {
        d.close();
      }
    }
  }

  /**
   * Serialize the mapredWork object to an output stream. DO NOT use this to write to standard
   * output since it closes the output stream. DO USE mapredWork.toXML() instead.
   */
  public static void serializeMapRedWork(MapredWork w, OutputStream out) {
    XMLEncoder e = null;
    try {
      e = new XMLEncoder(out);
      // workaround for java 1.5
      e.setPersistenceDelegate(ExpressionTypes.class, new EnumDelegate());
      e.setPersistenceDelegate(GroupByDesc.Mode.class, new EnumDelegate());
      e.writeObject(w);
    } finally {
      if (null != e) {
        e.close();
      }
    }

  }

  public static MapredWork deserializeMapRedWork(InputStream in, Configuration conf) {
    XMLDecoder d = null;
    try {
      d = new XMLDecoder(in, null, null);
      MapredWork ret = (MapredWork) d.readObject();
      return (ret);
    } finally {
      if (null != d) {
        d.close();
      }
    }
  }

  /**
   * Serialize the mapredLocalWork object to an output stream. DO NOT use this to write to standard
   * output since it closes the output stream. DO USE mapredWork.toXML() instead.
   */
  public static void serializeMapRedLocalWork(MapredLocalWork w, OutputStream out) {
    XMLEncoder e = null;
    try {
      e = new XMLEncoder(out);
      // workaround for java 1.5
      e.setPersistenceDelegate(ExpressionTypes.class, new EnumDelegate());
      e.setPersistenceDelegate(GroupByDesc.Mode.class, new EnumDelegate());
      e.writeObject(w);
    } finally {
      if (null != e) {
        e.close();
      }
    }
  }

  public static MapredLocalWork deserializeMapRedLocalWork(InputStream in, Configuration conf) {
    XMLDecoder d = null;
    try {
      d = new XMLDecoder(in, null, null);
      MapredLocalWork ret = (MapredLocalWork) d.readObject();
      return (ret);
    } finally {
      if (null != d) {
        d.close();
      }
    }
  }

  /**
   * Tuple.
   *
   * @param <T>
   * @param <V>
   */
  public static class Tuple<T, V> {
    private final T one;
    private final V two;

    public Tuple(T one, V two) {
      this.one = one;
      this.two = two;
    }

    public T getOne() {
      return this.one;
    }

    public V getTwo() {
      return this.two;
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
  public static String nullStringStorage = "\\N";
  public static String nullStringOutput = "NULL";

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

  /**
   * StreamPrinter.
   *
   */
  public static class StreamPrinter extends Thread {
    InputStream is;
    String type;
    PrintStream os;

    public StreamPrinter(InputStream is, String type, PrintStream os) {
      this.is = is;
      this.type = type;
      this.os = os;
    }

    @Override
    public void run() {
      BufferedReader br = null;
      try {
        InputStreamReader isr = new InputStreamReader(is);
        br = new BufferedReader(isr);
        String line = null;
        if (type != null) {
          while ((line = br.readLine()) != null) {
            os.println(type + ">" + line);
          }
        } else {
          while ((line = br.readLine()) != null) {
            os.println(line);
          }
        }
        br.close();
        br=null;
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }finally{
        IOUtils.closeStream(br);
      }
    }
  }

  public static TableDesc getTableDesc(Table tbl) {
    return (new TableDesc(tbl.getDeserializer().getClass(), tbl.getInputFormatClass(), tbl
        .getOutputFormatClass(), tbl.getSchema()));
  }

  // column names and column types are all delimited by comma
  public static TableDesc getTableDesc(String cols, String colTypes) {
    return (new TableDesc(LazySimpleSerDe.class, SequenceFileInputFormat.class,
        HiveSequenceFileOutputFormat.class, Utilities.makeProperties(
        org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "" + Utilities.ctrlaCode,
        org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS, cols,
        org.apache.hadoop.hive.serde.Constants.LIST_COLUMN_TYPES, colTypes)));
  }

  public static PartitionDesc getPartitionDesc(Partition part) throws HiveException {
    return (new PartitionDesc(part));
  }

  public static PartitionDesc getPartitionDescFromTableDesc(TableDesc tblDesc, Partition part)
      throws HiveException {
    return new PartitionDesc(part, tblDesc);
  }

  public static void addMapWork(MapredWork mr, Table tbl, String alias, Operator<?> work) {
    mr.addMapWork(tbl.getDataLocation().getPath(), alias, work, new PartitionDesc(
        getTableDesc(tbl), (LinkedHashMap<String, String>) null));
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

      // Default new line characters on windows are "CRLF" so detect if there are any windows
      // native newline characters and handle them.
      if (Shell.WINDOWS) {
        // if the CR is not followed by the LF on windows then add it back to the stream and
        // proceed with next characters in the input stream.
        if (foundCrChar && b != Utilities.newLineCode) {
          out.write(Utilities.carriageReturnCode);
          foundCrChar = false;
        }

        if (b == Utilities.carriageReturnCode) {
          foundCrChar = true;
          continue;
        }
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
      CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, jc);
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
      CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, jc);
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
      Class<?> keyClass, Class<?> valClass) throws IOException {
    boolean isCompressed = FileOutputFormat.getCompressOutput(jc);
    return createSequenceWriter(jc, fs, file, keyClass, valClass, isCompressed);
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
      Class<?> keyClass, Class<?> valClass, boolean isCompressed) throws IOException {
    CompressionCodec codec = null;
    CompressionType compressionType = CompressionType.NONE;
    Class codecClass = null;
    if (isCompressed) {
      compressionType = SequenceFileOutputFormat.getOutputCompressionType(jc);
      codecClass = FileOutputFormat.getOutputCompressorClass(jc, DefaultCodec.class);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, jc);
    }
    return (SequenceFile.createWriter(fs, jc, file, keyClass, valClass, compressionType, codec));

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
      boolean isCompressed) throws IOException {
    CompressionCodec codec = null;
    Class<?> codecClass = null;
    if (isCompressed) {
      codecClass = FileOutputFormat.getOutputCompressorClass(jc, DefaultCodec.class);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, jc);
    }
    return new RCFile.Writer(fs, jc, file, null, codec);
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
    // For compatibility with hadoop 0.17, change file:/a/b/c to file:///a/b/c
    if (StringUtils.startsWith(file, "file:/") && !StringUtils.startsWith(file, "file:///")) {
      file = "file:///" + file.substring("file:/".length());
    }
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

  public static Path toTaskTempPath(String orig) {
    return toTaskTempPath(new Path(orig));
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
   * return an integer only - this should match a pure integer as well. {1,3} is used to limit
   * matching for attempts #'s 0-999.
   */
  private static final Pattern FILE_NAME_TO_TASK_ID_REGEX =
      Pattern.compile("^.*?([0-9]+)(_[0-9]{1,3})?(\\..*)?$");

  /**
   * This retruns prefix part + taskID for bucket join for partitioned table
   */
  private static final Pattern FILE_NAME_PREFIXED_TASK_ID_REGEX =
      Pattern.compile("^.*?((\\(.*\\))?[0-9]+)(_[0-9]{1,3})?(\\..*)?$");

  /**
   * This breaks a prefixed bucket number into the prefix and the taskID
   */
  private static final Pattern PREFIXED_TASK_ID_REGEX =
      Pattern.compile("^(.*?\\(.*\\))?([0-9]+)$");

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

  private static String replaceTaskId(String taskId, int bucketNum) {
    return replaceTaskId(taskId, String.valueOf(bucketNum));
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
    StringBuffer s = new StringBuffer();
    for (int i = 0; i < taskIdLen - bucketNumLen; i++) {
      s.append("0");
    }
    return s.toString() + bucketNum;
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

    StringBuffer snew = new StringBuffer();
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
   * Get all file status from a root path and recursively go deep into certain levels.
   *
   * @param path
   *          the root path
   * @param level
   *          the depth of directory should explore
   * @param fs
   *          the file system
   * @return array of FileStatus
   * @throws IOException
   */
  public static FileStatus[] getFileStatusRecurse(Path path, int level, FileSystem fs)
      throws IOException {

    // construct a path pattern (e.g., /*/*) to find all dynamically generated paths
    StringBuilder sb = new StringBuilder(path.toUri().getPath());
    for (int i = 0; i < level; ++i) {
      sb.append(Path.SEPARATOR).append("*");
    }
    Path pathPattern = new Path(path, sb.toString());
    return fs.globStatus(pathPattern);
  }

  public static void mvFileToFinalPath(String specPath, Configuration hconf,
      boolean success, Log log, DynamicPartitionCtx dpCtx, FileSinkDesc conf) throws IOException,
      HiveException {

    FileSystem fs = (new Path(specPath)).getFileSystem(hconf);
    Path tmpPath = Utilities.toTempPath(specPath);
    Path taskTmpPath = Utilities.toTaskTempPath(specPath);
    Path intermediatePath = new Path(tmpPath.getParent(), tmpPath.getName()
        + ".intermediate");
    Path finalPath = new Path(specPath);
    if (success) {
      if (fs.exists(tmpPath)) {
        // Step1: rename tmp output folder to intermediate path. After this
        // point, updates from speculative tasks still writing to tmpPath
        // will not appear in finalPath.
        log.info("Moving tmp dir: " + tmpPath + " to: " + intermediatePath);
        Utilities.rename(fs, tmpPath, intermediatePath);
        // Step2: remove any tmp file or double-committed output files
        ArrayList<String> emptyBuckets =
            Utilities.removeTempOrDuplicateFiles(fs, intermediatePath, dpCtx);
        // create empty buckets if necessary
        if (emptyBuckets.size() > 0) {
          createEmptyBuckets(hconf, emptyBuckets, conf);
        }

        // Step3: move to the file destination
        log.info("Moving tmp dir: " + intermediatePath + " to: " + finalPath);
        Utilities.renameOrMoveFiles(fs, intermediatePath, finalPath);
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
   * @param specPath
   *          The final path where the dynamic partitions should be in.
   * @param conf
   *          FileSinkDesc.
   * @param dpCtx
   *          dynamic partition context.
   * @throws HiveException
   * @throws IOException
   */
  private static void createEmptyBuckets(Configuration hconf, ArrayList<String> paths,
      FileSinkDesc conf)
      throws HiveException, IOException {

    JobConf jc;
    if (hconf instanceof JobConf) {
      jc = new JobConf(hconf);
    } else {
      // test code path
      jc = new JobConf(hconf, ExecDriver.class);
    }
    HiveOutputFormat<?, ?> hiveOutputFormat = null;
    Class<? extends Writable> outputClass = null;
    boolean isCompressed = conf.getCompressed();
    TableDesc tableInfo = conf.getTableInfo();
    try {
      Serializer serializer = (Serializer) tableInfo.getDeserializerClass().newInstance();
      serializer.initialize(null, tableInfo.getProperties());
      outputClass = serializer.getSerializedClass();
      hiveOutputFormat = conf.getTableInfo().getOutputFileFormatClass().newInstance();
    } catch (SerDeException e) {
      throw new HiveException(e);
    } catch (InstantiationException e) {
      throw new HiveException(e);
    } catch (IllegalAccessException e) {
      throw new HiveException(e);
    }

    for (String p : paths) {
      Path path = new Path(p);
      RecordWriter writer = HiveFileFormatUtils.getRecordWriter(
          jc, hiveOutputFormat, outputClass, isCompressed, tableInfo.getProperties(), path);
      writer.close(false);
      LOG.info("created empty bucket for enforcing bucketing at " + path);
    }
  }

  /**
   * Remove all temporary files and duplicate (double-committed) files from a given directory.
   */
  public static void removeTempOrDuplicateFiles(FileSystem fs, Path path) throws IOException {
    removeTempOrDuplicateFiles(fs, path, null);
  }

  /**
   * Remove all temporary files and duplicate (double-committed) files from a given directory.
   *
   * @return a list of path names corresponding to should-be-created empty buckets.
   */
  public static ArrayList<String> removeTempOrDuplicateFiles(FileSystem fs, Path path,
      DynamicPartitionCtx dpCtx) throws IOException {
    if (path == null) {
      return null;
    }

    ArrayList<String> result = new ArrayList<String>();
    if (dpCtx != null) {
      FileStatus parts[] = getFileStatusRecurse(path, dpCtx.getNumDPCols(), fs);
      HashMap<String, FileStatus> taskIDToFile = null;

      for (int i = 0; i < parts.length; ++i) {
        assert parts[i].isDir() : "dynamic partition " + parts[i].getPath()
            + " is not a direcgtory";
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
        if (dpCtx.getNumBuckets() > 0 && taskIDToFile != null) {
          // refresh the file list
          items = fs.listStatus(parts[i].getPath());
          // get the missing buckets and generate empty buckets
          String taskID1 = taskIDToFile.keySet().iterator().next();
          Path bucketPath = taskIDToFile.values().iterator().next().getPath();
          for (int j = 0; j < dpCtx.getNumBuckets(); ++j) {
            String taskID2 = replaceTaskId(taskID1, j);
            if (!taskIDToFile.containsKey(taskID2)) {
              // create empty bucket, file name should be derived from taskID2
              String path2 = replaceTaskIdFromFilename(bucketPath.toUri().getPath().toString(), j);
              result.add(path2);
            }
          }
        }
      }
    } else {
      FileStatus[] items = fs.listStatus(path);
      removeTempOrDuplicateFiles(items, fs);
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
          if (otherFile.getLen() >= one.getLen()) {
            toDelete = one;
          } else {
            toDelete = otherFile;
            taskIdToFile.put(taskId, one);
          }
          long len1 = toDelete.getLen();
          long len2 = taskIdToFile.get(taskId).getLen();
          if (!fs.delete(toDelete.getPath(), true)) {
            throw new IOException("Unable to delete duplicate file: " + toDelete.getPath()
                + ". Existing file: " + taskIdToFile.get(taskId).getPath());
          } else {
            LOG.warn("Duplicate taskid file removed: " + toDelete.getPath() + " with length "
                + len1 + ". Existing file: " + taskIdToFile.get(taskId).getPath() + " with length "
                + len2);
          }
        }
      }
    }
    return taskIdToFile;
  }

  public static String getNameMessage(Exception e) {
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
          realFiles.add(realFile(one, conf));
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
   * Add new elements to the classpath.
   *
   * @param newPaths
   *          Array of classpath elements
   */
  public static ClassLoader addToClassPath(ClassLoader cloader, String[] newPaths) throws Exception {
    URLClassLoader loader = (URLClassLoader) cloader;
    List<URL> curPath = Arrays.asList(loader.getURLs());
    ArrayList<URL> newPath = new ArrayList<URL>();

    // get a list with the current classpath components
    for (URL onePath : curPath) {
      newPath.add(onePath);
    }
    curPath = newPath;

    for (String onestr : newPaths) {
      // special processing for hadoop-17. file:// needs to be removed
      if (StringUtils.indexOf(onestr, "file://") == 0) {
        onestr = StringUtils.substring(onestr, 7);
      }

      URL oneurl = (new File(onestr)).toURL();
      if (!curPath.contains(oneurl)) {
        curPath.add(oneurl);
      }
    }

    return new URLClassLoader(curPath.toArray(new URL[0]), loader);
  }

  /**
   * remove elements from the classpath.
   *
   * @param pathsToRemove
   *          Array of classpath elements
   */
  public static void removeFromClassPath(String[] pathsToRemove) throws Exception {
    Thread curThread = Thread.currentThread();
    URLClassLoader loader = (URLClassLoader) curThread.getContextClassLoader();
    Set<URL> newPath = new HashSet<URL>(Arrays.asList(loader.getURLs()));

    for (String onestr : pathsToRemove) {
      // special processing for hadoop-17. file:// needs to be removed
      if (StringUtils.indexOf(onestr, "file://") == 0) {
        onestr = StringUtils.substring(onestr, 7);
      }

      URL oneurl = (new File(onestr)).toURL();
      newPath.remove(oneurl);
    }

    loader = new URLClassLoader(newPath.toArray(new URL[0]));
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

  public static List<String> getColumnNames(Properties props) {
    List<String> names = new ArrayList<String>();
    String colNames = props.getProperty(Constants.LIST_COLUMNS);
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
    String colNames = props.getProperty(Constants.LIST_COLUMN_TYPES);
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
  public static void copyTableJobPropertiesToConf(TableDesc tbl, JobConf job) {
    Map<String, String> jobProperties = tbl.getJobProperties();
    if (jobProperties == null) {
      return;
    }
    for (Map.Entry<String, String> entry : jobProperties.entrySet()) {
      job.set(entry.getKey(), entry.getValue());
    }
  }

  public static Object getInputSummaryLock = new Object();

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
  public static ContentSummary getInputSummary(Context ctx, MapredWork work, PathFilter filter)
      throws IOException {

    long[] summary = {0, 0, 0};

    List<String> pathNeedProcess = new ArrayList<String>();

    // Since multiple threads could call this method concurrently, locking
    // this method will avoid number of threads out of control.
    synchronized (getInputSummaryLock) {
      // For each input path, calculate the total size.
      for (String path : work.getPathToAliases().keySet()) {
        Path p = new Path(path);

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
      final ThreadPoolExecutor executor;
      int maxThreads = ctx.getConf().getInt("mapred.dfsclient.parallelism.max", 0);
      if (pathNeedProcess.size() > 1 && maxThreads > 1) {
        int numExecutors = Math.min(pathNeedProcess.size(), maxThreads);
        LOG.info("Using " + numExecutors + " threads for getContentSummary");
        executor = new ThreadPoolExecutor(numExecutors, numExecutors, 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>());
      } else {
        executor = null;
      }

      HiveInterruptCallback interrup = HiveInterruptUtils.add(new HiveInterruptCallback() {
        @Override
        public void interrupt() {
          if (executor != null) {
            executor.shutdownNow();
          }
        }
      });
      try {
        Configuration conf = ctx.getConf();
        JobConf jobConf = new JobConf(conf);
        for (String path : pathNeedProcess) {
          final Path p = new Path(path);
          final String pathStr = path;
          // All threads share the same Configuration and JobConf based on the
          // assumption that they are thread safe if only read operations are
          // executed. It is not stated in Hadoop's javadoc, the sourcce codes
          // clearly showed that they made efforts for it and we believe it is
          // thread safe. Will revisit this piece of codes if we find the assumption
          // is not correct.
          final Configuration myConf = conf;
          final JobConf myJobConf = jobConf;
          final PartitionDesc partDesc = work.getPathToPartitionInfo().get(
              p.toString());
          Runnable r = new Runnable() {
            public void run() {
              try {
                ContentSummary resultCs;

                Class<? extends InputFormat> inputFormatCls = partDesc
                    .getInputFileFormatClass();
                InputFormat inputFormatObj = HiveInputFormat.getInputFormatFromCache(
                    inputFormatCls, myJobConf);
                if (inputFormatObj instanceof ContentSummaryInputFormat) {
                  resultCs = ((ContentSummaryInputFormat) inputFormatObj).getContentSummary(p,
                      myJobConf);
                } else {
                  FileSystem fs = p.getFileSystem(myConf);
                  resultCs = fs.getContentSummary(p);
                }
                resultMap.put(pathStr, resultCs);
              } catch (IOException e) {
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

        return new ContentSummary(summary[0], summary[1], summary[2]);
      } finally {
        HiveInterruptUtils.remove(interrup);
      }
    }
  }

  public static boolean isEmptyPath(JobConf job, String dirPath, Context ctx)
      throws Exception {
    ContentSummary cs = ctx.getCS(dirPath);
    if (cs != null) {
      LOG.info("Content Summary " + dirPath + "length: " + cs.getLength() + " num files: "
          + cs.getFileCount() + " num directories: " + cs.getDirectoryCount());
      return (cs.getLength() == 0 && cs.getFileCount() == 0 && cs.getDirectoryCount() <= 1);
    } else {
      LOG.info("Content Summary not cached for " + dirPath);
    }
    Path p = new Path(dirPath);
    return isEmptyPath(job, p);
  }

  public static boolean isEmptyPath(JobConf job, Path dirPath) throws Exception {
    FileSystem inpFs = dirPath.getFileSystem(job);

    if (inpFs.exists(dirPath)) {
      FileStatus[] fStats = inpFs.listStatus(dirPath);
      if (fStats.length > 0) {
        return false;
      }
    }
    return true;
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
      if (task instanceof ExecDriver && !mrTasks.contains((ExecDriver) task)) {
        mrTasks.add((ExecDriver) task);
      }

      if (task.getDependentTasks() != null) {
        getMRTasks(task.getDependentTasks(), mrTasks);
      }
    }
  }

  public static boolean supportCombineFileInputFormat() {
    return ShimLoader.getHadoopShims().getCombineFileInputFormat() != null;
  }

  /**
   * Construct a list of full partition spec from Dynamic Partition Context and the directory names
   * corresponding to these dynamic partitions.
   */
  public static List<LinkedHashMap<String, String>> getFullDPSpecs(Configuration conf,
      DynamicPartitionCtx dpCtx) throws HiveException {

    try {
      Path loadPath = new Path(dpCtx.getRootPath());
      FileSystem fs = loadPath.getFileSystem(conf);
      int numDPCols = dpCtx.getNumDPCols();
      FileStatus[] status = Utilities.getFileStatusRecurse(loadPath, numDPCols, fs);

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
    String statsImplementationClass = HiveConf.getVar(jc, HiveConf.ConfVars.HIVESTATSDBCLASS);
    if (StatsFactory.setImplementation(statsImplementationClass, jc)) {
      return StatsFactory.getStatsPublisher();
    } else {
      return null;
    }
  }

  public static void setColumnNameList(JobConf jobConf, Operator op) {
    RowSchema rowSchema = op.getSchema();
    if (rowSchema == null) {
      return;
    }
    StringBuilder columnNames = new StringBuilder();
    for (ColumnInfo colInfo : rowSchema.getSignature()) {
      if (columnNames.length() > 0) {
        columnNames.append(",");
      }
      columnNames.append(colInfo.getInternalName());
    }
    String columnNamesString = columnNames.toString();
    jobConf.set(Constants.LIST_COLUMNS, columnNamesString);
  }

  public static void setColumnTypeList(JobConf jobConf, Operator op) {
    RowSchema rowSchema = op.getSchema();
    if (rowSchema == null) {
      return;
    }
    StringBuilder columnTypes = new StringBuilder();
    for (ColumnInfo colInfo : rowSchema.getSignature()) {
      if (columnTypes.length() > 0) {
        columnTypes.append(",");
      }
      columnTypes.append(colInfo.getType().getTypeName());
    }
    String columnTypesString = columnTypes.toString();
    jobConf.set(Constants.LIST_COLUMN_TYPES, columnTypesString);
  }

  public static void validatePartSpec(Table tbl, Map<String, String> partSpec)
      throws SemanticException {

    List<FieldSchema> parts = tbl.getPartitionKeys();
    Set<String> partCols = new HashSet<String>(parts.size());
    for (FieldSchema col : parts) {
      partCols.add(col.getName());
    }
    for (String col : partSpec.keySet()) {
      if (!partCols.contains(col)) {
        throw new SemanticException(ErrorMsg.NONEXISTPARTCOL.getMsg(col));
      }
    }
  }

  public static String suffix = ".hashtable";

  public static String generatePath(String baseURI, String dumpFilePrefix,
      Byte tag, String bigBucketFileName) {
    String path = new String(baseURI + Path.SEPARATOR + "MapJoin-" + dumpFilePrefix + tag +
      "-" + bigBucketFileName + suffix);
    return path;
  }

  public static String generateFileName(Byte tag, String bigBucketFileName) {
    String fileName = new String("MapJoin-" + tag + "-" + bigBucketFileName + suffix);
    return fileName;
  }

  public static String generateTmpURI(String baseURI, String id) {
    String tmpFileURI = new String(baseURI + Path.SEPARATOR + "HashTable-" + id);
    return tmpFileURI;
  }

  public static String generateTarURI(String baseURI, String filename) {
    String tmpFileURI = new String(baseURI + Path.SEPARATOR + filename + ".tar.gz");
    return tmpFileURI;
  }

  public static String generateTarURI(Path baseURI, String filename) {
    String tmpFileURI = new String(baseURI + Path.SEPARATOR + filename + ".tar.gz");
    return tmpFileURI;
  }

  public static String generateTarFileName(String name) {
    String tmpFileURI = new String(name + ".tar.gz");
    return tmpFileURI;
  }

  public static String generatePath(Path baseURI, String filename) {
    String path = new String(baseURI + Path.SEPARATOR + filename);
    return path;
  }

  public static String now() {
    Calendar cal = Calendar.getInstance();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    return sdf.format(cal.getTime());
  }

  public static double showTime(long time) {
    double result = (double) time / (double) 1000;
    return result;
  }

  /**
   * Check if a function can be pushed down to JDO.
   * Now only {=, AND, OR} are supported.
   * @param func a generic function.
   * @return true if this function can be pushed down to JDO filter.
   */
  private static boolean supportedJDOFuncs(GenericUDF func) {
    return func instanceof GenericUDFOPEqual ||
           func instanceof GenericUDFOPAnd ||
           func instanceof GenericUDFOPOr;
  }

  /**
   * Check if the partition pruning expression can be pushed down to JDO filtering.
   * The partition expression contains only partition columns.
   * The criteria that an expression can be pushed down are that:
   *  1) the expression only contains function specified in supportedJDOFuncs().
   *     Now only {=, AND, OR} can be pushed down.
   *  2) the partition column type and the constant type have to be String. This is
   *     restriction by the current JDO filtering implementation.
   * @param tab The table that contains the partition columns.
   * @param expr the partition pruning expression
   * @return null if the partition pruning expression can be pushed down to JDO filtering.
   */
  public static String checkJDOPushDown(Table tab, ExprNodeDesc expr) {
    if (expr instanceof ExprNodeConstantDesc) {
      // JDO filter now only support String typed literal -- see Filter.g and ExpressionTree.java
      Object value = ((ExprNodeConstantDesc)expr).getValue();
      if (value instanceof String) {
        return null;
      }
      return "Constant " + value + " is not string type";
    } else if (expr instanceof ExprNodeColumnDesc) {
      // JDO filter now only support String typed literal -- see Filter.g and ExpressionTree.java
      TypeInfo type = expr.getTypeInfo();
      if (type.getTypeName().equals(Constants.STRING_TYPE_NAME)) {
        String colName = ((ExprNodeColumnDesc)expr).getColumn();
        for (FieldSchema fs: tab.getPartCols()) {
          if (fs.getName().equals(colName)) {
            if (fs.getType().equals(Constants.STRING_TYPE_NAME)) {
              return null;
            }
            return "Partition column " + fs.getName() + " is not string type";
          }
        }
        assert(false); // cannot find the partition column!
     } else {
        return "Column " + expr.getExprString() + " is not string type";
     }
    } else if (expr instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc) expr;
      GenericUDF func = funcDesc.getGenericUDF();
      if (!supportedJDOFuncs(func)) {
        return "Expression " + expr.getExprString() + " cannot be evaluated";
      }
      List<ExprNodeDesc> children = funcDesc.getChildExprs();
      for (ExprNodeDesc child: children) {
        String message = checkJDOPushDown(tab, child);
        if (message != null) {
          return message;
        }
      }
      return null;
    }
    return "Expression " + expr.getExprString() + " cannot be evaluated";
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
        for (PartitionDesc part : mapredWork.getPathToPartitionInfo().values()) {
          Class<? extends InputFormat> inputFormatCls = part
              .getInputFileFormatClass();
          if (ReworkMapredInputFormat.class.isAssignableFrom(inputFormatCls)) {
            reworkInputFormats.add(inputFormatCls);
          }
        }

        if (reworkInputFormats.size() > 0) {
          for (Class<? extends InputFormat> inputFormatCls : reworkInputFormats) {
            ReworkMapredInputFormat inst = (ReworkMapredInputFormat) ReflectionUtils
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
      int baseWindow, int maxRetries)  throws SQLException {

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
      int waitWindow, int maxRetries) throws SQLException {

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
      int waitWindow, int maxRetries) throws SQLException {

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
  public static long getRandomWaitTime(int baseWindow, int failures, Random r) {
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
    StringBuffer sb = new StringBuffer(key.length());
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

  public static Class getBuiltinUtilsClass() throws ClassNotFoundException {
    return Class.forName("org.apache.hive.builtins.BuiltinUtils");
  }
}
