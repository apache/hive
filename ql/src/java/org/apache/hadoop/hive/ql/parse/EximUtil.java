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

package org.apache.hadoop.hive.ql.parse;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.DBSerializer;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.JsonWriter;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.ReplicationSpecSerializer;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.TableSerializer;
import org.apache.hadoop.hive.ql.parse.repl.load.MetaData;
import org.apache.hadoop.hive.ql.parse.repl.load.MetadataJson;
import org.apache.thrift.TException;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 *
 * EximUtil. Utility methods for the export/import semantic
 * analyzers.
 *
 */
public class EximUtil {

  public static final String METADATA_NAME = "_metadata";
  public static final String FILES_NAME = "_files";
  public static final String DATA_PATH_NAME = "data";

  private static final Logger LOG = LoggerFactory.getLogger(EximUtil.class);

  /**
   * Wrapper class for common BaseSemanticAnalyzer non-static members
   * into static generic methods without having the fn signatures
   * becoming overwhelming, with passing each of these into every function.
   *
   * Note, however, that since this is constructed with args passed in,
   * parts of the context, such as the tasks or inputs, might have been
   * overridden with temporary context values, rather than being exactly
   * 1:1 equivalent to BaseSemanticAnalyzer.getRootTasks() or BSA.getInputs().
   */
  public static class SemanticAnalyzerWrapperContext {
    private HiveConf conf;
    private Hive db;
    private HashSet<ReadEntity> inputs;
    private HashSet<WriteEntity> outputs;
    private List<Task<? extends Serializable>> tasks;
    private Logger LOG;
    private Context ctx;
    private DumpType eventType = DumpType.EVENT_UNKNOWN;

    public HiveConf getConf() {
      return conf;
    }

    public Hive getHive() {
      return db;
    }

    public HashSet<ReadEntity> getInputs() {
      return inputs;
    }

    public HashSet<WriteEntity> getOutputs() {
      return outputs;
    }

    public List<Task<? extends Serializable>> getTasks() {
      return tasks;
    }

    public Logger getLOG() {
      return LOG;
    }

    public Context getCtx() {
      return ctx;
    }

    public void setEventType(DumpType eventType) {
      this.eventType = eventType;
    }

    public DumpType getEventType() {
      return eventType;
    }

    public SemanticAnalyzerWrapperContext(HiveConf conf, Hive db,
                                          HashSet<ReadEntity> inputs,
                                          HashSet<WriteEntity> outputs,
                                          List<Task<? extends Serializable>> tasks,
                                          Logger LOG, Context ctx){
      this.conf = conf;
      this.db = db;
      this.inputs = inputs;
      this.outputs = outputs;
      this.tasks = tasks;
      this.LOG = LOG;
      this.ctx = ctx;
    }
  }


  private EximUtil() {
  }

  /**
   * Initialize the URI where the exported data collection is
   * to created for export, or is present for import
   */
  public static URI getValidatedURI(HiveConf conf, String dcPath) throws SemanticException {
    try {
      boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODE)
          || conf.getBoolVar(HiveConf.ConfVars.HIVEEXIMTESTMODE);
      URI uri = new Path(dcPath).toUri();
      FileSystem fs = FileSystem.get(uri, conf);
      // Get scheme from FileSystem
      String scheme = fs.getScheme();
      String authority = uri.getAuthority();
      String path = uri.getPath();

      LOG.info("Path before norm :" + path);
      // generate absolute path relative to home directory
      if (!path.startsWith("/")) {
        if (testMode) {
          path = (new Path(System.getProperty("test.tmp.dir"), path)).toUri().getPath();
        } else {
          path =
              (new Path(new Path("/user/" + System.getProperty("user.name")), path)).toUri()
                  .getPath();
        }
      }


      // if scheme is specified but not authority then use the default authority
      if (StringUtils.isEmpty(authority)) {
        URI defaultURI = FileSystem.get(conf).getUri();
        authority = defaultURI.getAuthority();
      }

      LOG.info("Scheme:" + scheme + ", authority:" + authority + ", path:" + path);
      Collection<String> eximSchemes =
          conf.getStringCollection(HiveConf.ConfVars.HIVE_EXIM_URI_SCHEME_WL.varname);
      if (!eximSchemes.contains(scheme)) {
        throw new SemanticException(
            ErrorMsg.INVALID_PATH
                .getMsg("only the following file systems accepted for export/import : "
                    + conf.get(HiveConf.ConfVars.HIVE_EXIM_URI_SCHEME_WL.varname)));
      }

      try {
        return new URI(scheme, authority, path, null, null);
      } catch (URISyntaxException e) {
        throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(), e);
      }
    } catch (IOException e) {
      throw new SemanticException(ErrorMsg.IO_ERROR.getMsg() + ": " + e.getMessage(), e);
    }
  }

  static void validateTable(org.apache.hadoop.hive.ql.metadata.Table table) throws SemanticException {
    if (table.isNonNative()) {
      throw new SemanticException(ErrorMsg.EXIM_FOR_NON_NATIVE.getMsg());
    }
  }

  public static String relativeToAbsolutePath(HiveConf conf, String location)
      throws SemanticException {
    try {
      boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODE)
        || conf.getBoolVar(HiveConf.ConfVars.HIVEEXIMTESTMODE);;
      if (testMode) {
        URI uri = new Path(location).toUri();
        FileSystem fs = FileSystem.get(uri, conf);
        String scheme = fs.getScheme();
        String authority = uri.getAuthority();
        String path = uri.getPath();
        if (!path.startsWith("/")) {
          path = (new Path(System.getProperty("test.tmp.dir"), path)).toUri().getPath();
        }
        if (StringUtils.isEmpty(scheme)) {
          scheme = "pfile";
        }
        try {
          uri = new URI(scheme, authority, path, null, null);
        } catch (URISyntaxException e) {
          throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(), e);
        }
        return uri.toString();
      } else {
        Path path = new Path(location);
        if (path.isAbsolute()) {
          return location;
        }
        return path.getFileSystem(conf).makeQualified(path).toString();
      }
    } catch (IOException e) {
      throw new SemanticException(ErrorMsg.IO_ERROR.getMsg() + ": " + e.getMessage(), e);
    }
  }

  /* major version number should match for backward compatibility */
  public static final String METADATA_FORMAT_VERSION = "0.2";

  /* If null, then the major version number should match */
  public static final String METADATA_FORMAT_FORWARD_COMPATIBLE_VERSION = null;

  public static void createDbExportDump(FileSystem fs, Path metadataPath, Database dbObj,
      ReplicationSpec replicationSpec) throws IOException, SemanticException {

    // WARNING NOTE : at this point, createDbExportDump lives only in a world where ReplicationSpec is in replication scope
    // If we later make this work for non-repl cases, analysis of this logic might become necessary. Also, this is using
    // Replv2 semantics, i.e. with listFiles laziness (no copy at export time)

    // Remove all the entries from the parameters which are added by repl tasks internally.
    Map<String, String> parameters = dbObj.getParameters();
    if (parameters != null) {
      Map<String, String> tmpParameters = new HashMap<>(parameters);
      tmpParameters.entrySet()
                .removeIf(e -> e.getKey().startsWith(Utils.BOOTSTRAP_DUMP_STATE_KEY_PREFIX)
                            || e.getKey().equals(ReplUtils.REPL_CHECKPOINT_KEY)
                            || e.getKey().equals(ReplChangeManager.SOURCE_OF_REPLICATION));
      dbObj.setParameters(tmpParameters);
    }
    try (JsonWriter jsonWriter = new JsonWriter(fs, metadataPath)) {
      new DBSerializer(dbObj).writeTo(jsonWriter, replicationSpec);
    }
    if (parameters != null) {
      dbObj.setParameters(parameters);
    }
  }

  public static void createExportDump(FileSystem fs, Path metadataPath, Table tableHandle,
      Iterable<Partition> partitions, ReplicationSpec replicationSpec, HiveConf hiveConf)
      throws SemanticException, IOException {

    if (replicationSpec == null) {
      replicationSpec = new ReplicationSpec(); // instantiate default values if not specified
    }

    if (tableHandle == null) {
      replicationSpec.setNoop(true);
    }

    try (JsonWriter writer = new JsonWriter(fs, metadataPath)) {
      if (replicationSpec.isInReplicationScope()) {
        new ReplicationSpecSerializer().writeTo(writer, replicationSpec);
      }
      new TableSerializer(tableHandle, partitions, hiveConf).writeTo(writer, replicationSpec);
    }
  }

  public static MetaData readMetaData(FileSystem fs, Path metadataPath)
      throws IOException, SemanticException {
    String message = readAsString(fs, metadataPath);
    try {
      return new MetadataJson(message).getMetaData();
    } catch (TException | JSONException e) {
      throw new SemanticException(ErrorMsg.ERROR_SERIALIZE_METADATA.getMsg(), e);
    }
  }

  public static String readAsString(final FileSystem fs, final Path fromMetadataPath)
      throws IOException {
    try (FSDataInputStream stream = fs.open(fromMetadataPath)) {
      byte[] buffer = new byte[1024];
      ByteArrayOutputStream sb = new ByteArrayOutputStream();
      int read = stream.read(buffer);
      while (read != -1) {
        sb.write(buffer, 0, read);
        read = stream.read(buffer);
      }
      return new String(sb.toByteArray(), "UTF-8");
    }
  }

  /* check the forward and backward compatibility */
  public static void doCheckCompatibility(String currVersion,
      String version, String fcVersion) throws SemanticException {
    if (version == null) {
      throw new SemanticException(ErrorMsg.INVALID_METADATA.getMsg("Version number missing"));
    }
    StringTokenizer st = new StringTokenizer(version, ".");
    int data_major = Integer.parseInt(st.nextToken());

    StringTokenizer st2 = new StringTokenizer(currVersion, ".");
    int code_major = Integer.parseInt(st2.nextToken());
    int code_minor = Integer.parseInt(st2.nextToken());

    if (code_major > data_major) {
      throw new SemanticException(ErrorMsg.INVALID_METADATA.getMsg("Not backward compatible."
          + " Producer version " + version + ", Consumer version " +
          currVersion));
    } else {
      if ((fcVersion == null) || fcVersion.isEmpty()) {
        if (code_major < data_major) {
          throw new SemanticException(ErrorMsg.INVALID_METADATA.getMsg("Not forward compatible."
              + "Producer version " + version + ", Consumer version " +
              currVersion));
        }
      } else {
        StringTokenizer st3 = new StringTokenizer(fcVersion, ".");
        int fc_major = Integer.parseInt(st3.nextToken());
        int fc_minor = Integer.parseInt(st3.nextToken());
        if ((fc_major > code_major) || ((fc_major == code_major) && (fc_minor > code_minor))) {
          throw new SemanticException(ErrorMsg.INVALID_METADATA.getMsg("Not forward compatible."
              + "Minimum version " + fcVersion + ", Consumer version " +
              currVersion));
        }
      }
    }
  }

  /**
   * Return the partition specification from the specified keys and values
   *
   * @param partCols
   *          the names of the partition keys
   * @param partVals
   *          the values of the partition keys
   *
   * @return the partition specification as a map
   */
  public static Map<String, String> makePartSpec(List<FieldSchema> partCols, List<String> partVals) {
    Map<String, String> partSpec = new LinkedHashMap<String, String>();
    for (int i = 0; i < partCols.size(); ++i) {
      partSpec.put(partCols.get(i).getName(), partVals.get(i));
    }
    return partSpec;
  }

  /**
   * Compares the schemas - names, types and order, but ignoring comments
   *
   * @param newSchema
   *          the new schema
   * @param oldSchema
   *          the old schema
   * @return a boolean indicating match
   */
  public static boolean schemaCompare(List<FieldSchema> newSchema, List<FieldSchema> oldSchema) {
    Iterator<FieldSchema> newColIter = newSchema.iterator();
    for (FieldSchema oldCol : oldSchema) {
      FieldSchema newCol = null;
      if (newColIter.hasNext()) {
        newCol = newColIter.next();
      } else {
        return false;
      }
      // not using FieldSchema.equals as comments can be different
      if (!oldCol.getName().equals(newCol.getName())
          || !oldCol.getType().equals(newCol.getType())) {
        return false;
      }
    }
    if (newColIter.hasNext()) {
      return false;
    }
    return true;
  }

  public static PathFilter getDirectoryFilter(final FileSystem fs) {
    // TODO : isn't there a prior impl of an isDirectory utility PathFilter so users don't have to write their own?
    return new PathFilter() {
      @Override
      public boolean accept(Path p) {
        try {
          return fs.isDirectory(p);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
}
