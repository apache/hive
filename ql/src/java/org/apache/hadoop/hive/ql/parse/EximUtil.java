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

package org.apache.hadoop.hive.ql.parse;

import com.google.common.base.Function;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

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
  static URI getValidatedURI(HiveConf conf, String dcPath) throws SemanticException {
    try {
      boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODE);
      URI uri = new Path(dcPath).toUri();
      String scheme = uri.getScheme();
      String authority = uri.getAuthority();
      String path = uri.getPath();
      FileSystem fs = FileSystem.get(uri, conf);

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

      // Get scheme from FileSystem
      scheme = fs.getScheme();

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
      boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODE);
      if (testMode) {
        URI uri = new Path(location).toUri();
        FileSystem fs = FileSystem.get(uri, conf);
        String scheme = fs.getScheme();
        String authority = uri.getAuthority();
        String path = uri.getPath();
        if (!path.startsWith("/")) {
          path = (new Path(System.getProperty("test.tmp.dir"), path)).toUri().getPath();
        }
        try {
          uri = new URI(scheme, authority, path, null, null);
        } catch (URISyntaxException e) {
          throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(), e);
        }
        return uri.toString();
      } else {
        // no-op for non-test mode for now
        return location;
      }
    } catch (IOException e) {
      throw new SemanticException(ErrorMsg.IO_ERROR.getMsg() + ": " + e.getMessage(), e);
    }
  }

  /* major version number should match for backward compatibility */
  public static final String METADATA_FORMAT_VERSION = "0.2";

  /* If null, then the major version number should match */
  public static final String METADATA_FORMAT_FORWARD_COMPATIBLE_VERSION = null;

  public static void createDbExportDump(
      FileSystem fs, Path metadataPath, Database dbObj,
      ReplicationSpec replicationSpec) throws IOException, SemanticException {

    // WARNING NOTE : at this point, createDbExportDump lives only in a world where ReplicationSpec is in replication scope
    // If we later make this work for non-repl cases, analysis of this logic might become necessary. Also, this is using
    // Replv2 semantics, i.e. with listFiles laziness (no copy at export time)

    OutputStream out = fs.create(metadataPath);
    JsonGenerator jgen = (new JsonFactory()).createJsonGenerator(out);
    jgen.writeStartObject();
    jgen.writeStringField("version",METADATA_FORMAT_VERSION);
    dbObj.putToParameters(ReplicationSpec.KEY.CURR_STATE_ID.toString(), replicationSpec.getCurrentReplicationState());

    if (METADATA_FORMAT_FORWARD_COMPATIBLE_VERSION != null) {
      jgen.writeStringField("fcversion",METADATA_FORMAT_FORWARD_COMPATIBLE_VERSION);
    }
    TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
    try {
      jgen.writeStringField("db", serializer.toString(dbObj, "UTF-8"));
    } catch (TException e) {
      throw new SemanticException(
          ErrorMsg.ERROR_SERIALIZE_METASTORE
              .getMsg(), e);
    }

    jgen.writeEndObject();
    jgen.close(); // JsonGenerator owns the OutputStream, so it closes it when we call close.
  }

  public static void createExportDump(FileSystem fs, Path metadataPath,
      org.apache.hadoop.hive.ql.metadata.Table tableHandle,
      Iterable<org.apache.hadoop.hive.ql.metadata.Partition> partitions,
      ReplicationSpec replicationSpec) throws SemanticException, IOException {

    if (replicationSpec == null){
      replicationSpec = new ReplicationSpec(); // instantiate default values if not specified
    }

    if (tableHandle == null){
      replicationSpec.setNoop(true);
    }

    OutputStream out = fs.create(metadataPath);
    JsonGenerator jgen = (new JsonFactory()).createJsonGenerator(out);
    jgen.writeStartObject();
    jgen.writeStringField("version",METADATA_FORMAT_VERSION);
    if (METADATA_FORMAT_FORWARD_COMPATIBLE_VERSION != null) {
      jgen.writeStringField("fcversion",METADATA_FORMAT_FORWARD_COMPATIBLE_VERSION);
    }

    if (replicationSpec.isInReplicationScope()){
      for (ReplicationSpec.KEY key : ReplicationSpec.KEY.values()){
        String value = replicationSpec.get(key);
        if (value != null){
          jgen.writeStringField(key.toString(), value);
        }
      }
      if (tableHandle != null){
        Table ttable = tableHandle.getTTable();
        ttable.putToParameters(
            ReplicationSpec.KEY.CURR_STATE_ID.toString(), replicationSpec.getCurrentReplicationState());
        if ((ttable.getParameters().containsKey("EXTERNAL")) &&
            (ttable.getParameters().get("EXTERNAL").equalsIgnoreCase("TRUE"))){
          // Replication destination will not be external - override if set
          ttable.putToParameters("EXTERNAL","FALSE");
        }
        if (ttable.isSetTableType() && ttable.getTableType().equalsIgnoreCase(TableType.EXTERNAL_TABLE.toString())){
          // Replication dest will not be external - override if set
          ttable.setTableType(TableType.MANAGED_TABLE.toString());
        }
      }
    } else {
      // ReplicationSpec.KEY scopeKey = ReplicationSpec.KEY.REPL_SCOPE;
      // write(out, ",\""+ scopeKey.toString() +"\":\"" + replicationSpec.get(scopeKey) + "\"");
      // TODO: if we want to be explicit about this dump not being a replication dump, we can
      // uncomment this else section, but currently unnneeded. Will require a lot of golden file
      // regen if we do so.
    }
    if ((tableHandle != null) && (!replicationSpec.isNoop())){
      TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
      try {
        jgen.writeStringField("table", serializer.toString(tableHandle.getTTable(), "UTF-8"));
        jgen.writeFieldName("partitions");
        jgen.writeStartArray();
        if (partitions != null) {
          for (org.apache.hadoop.hive.ql.metadata.Partition partition : partitions) {
            Partition tptn = partition.getTPartition();
            if (replicationSpec.isInReplicationScope()){
              tptn.putToParameters(
                  ReplicationSpec.KEY.CURR_STATE_ID.toString(), replicationSpec.getCurrentReplicationState());
              if ((tptn.getParameters().containsKey("EXTERNAL")) &&
                  (tptn.getParameters().get("EXTERNAL").equalsIgnoreCase("TRUE"))){
                // Replication destination will not be external
                tptn.putToParameters("EXTERNAL", "FALSE");
              }
            }
            jgen.writeString(serializer.toString(tptn, "UTF-8"));
            jgen.flush();
          }
        }
        jgen.writeEndArray();
      } catch (TException e) {
        throw new SemanticException(
            ErrorMsg.ERROR_SERIALIZE_METASTORE
                .getMsg(), e);
      }
    }
    jgen.writeEndObject();
    jgen.close(); // JsonGenerator owns the OutputStream, so it closes it when we call close.
  }

  /**
   * Utility class to help return complex value from readMetaData function
   */
  public static class ReadMetaData {
    private final Database db;
    private final Table table;
    private final Iterable<Partition> partitions;
    private final ReplicationSpec replicationSpec;

    public ReadMetaData(){
      this(null,null,null,new ReplicationSpec());
    }
    public ReadMetaData(Database db, Table table, Iterable<Partition> partitions, ReplicationSpec replicationSpec){
      this.db = db;
      this.table = table;
      this.partitions = partitions;
      this.replicationSpec = replicationSpec;
    }

    public Database getDatabase(){
      return db;
    }

    public Table getTable() {
      return table;
    }

    public Iterable<Partition> getPartitions() {
      return partitions;
    }

    public ReplicationSpec getReplicationSpec() {
      return replicationSpec;
    }
  };

  public static ReadMetaData readMetaData(FileSystem fs, Path metadataPath)
      throws IOException, SemanticException {
    FSDataInputStream mdstream = null;
    try {
      mdstream = fs.open(metadataPath);
      byte[] buffer = new byte[1024];
      ByteArrayOutputStream sb = new ByteArrayOutputStream();
      int read = mdstream.read(buffer);
      while (read != -1) {
        sb.write(buffer, 0, read);
        read = mdstream.read(buffer);
      }
      String md = new String(sb.toByteArray(), "UTF-8");
      JSONObject jsonContainer = new JSONObject(md);
      String version = jsonContainer.getString("version");
      String fcversion = getJSONStringEntry(jsonContainer, "fcversion");
      checkCompatibility(version, fcversion);

      String dbDesc = getJSONStringEntry(jsonContainer, "db");
      String tableDesc = getJSONStringEntry(jsonContainer,"table");
      TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());

      Database db = null;
      if (dbDesc != null){
        db = new Database();
        deserializer.deserialize(db, dbDesc, "UTF-8");
      }

      Table table = null;
      List<Partition> partitionsList = null;
      if (tableDesc != null){
        table = new Table();
        deserializer.deserialize(table, tableDesc, "UTF-8");
        // TODO : jackson-streaming-iterable-redo this
        JSONArray jsonPartitions = new JSONArray(jsonContainer.getString("partitions"));
        partitionsList = new ArrayList<Partition>(jsonPartitions.length());
        for (int i = 0; i < jsonPartitions.length(); ++i) {
          String partDesc = jsonPartitions.getString(i);
          Partition partition = new Partition();
          deserializer.deserialize(partition, partDesc, "UTF-8");
          partitionsList.add(partition);
        }
      }

      return new ReadMetaData(db, table, partitionsList,readReplicationSpec(jsonContainer));
    } catch (JSONException e) {
      throw new SemanticException(ErrorMsg.ERROR_SERIALIZE_METADATA.getMsg(), e);
    } catch (TException e) {
      throw new SemanticException(ErrorMsg.ERROR_SERIALIZE_METADATA.getMsg(), e);
    } finally {
      if (mdstream != null) {
        mdstream.close();
      }
    }
  }

  private static ReplicationSpec readReplicationSpec(final JSONObject jsonContainer){
    Function<String,String> keyFetcher = new Function<String, String>() {
      @Override
      public String apply(@Nullable String s) {
        return getJSONStringEntry(jsonContainer,s);
      }
    };
    return new ReplicationSpec(keyFetcher);
  }

  private static String getJSONStringEntry(JSONObject jsonContainer, String name) {
    String retval = null;
    try {
      retval = jsonContainer.getString(name);
    } catch (JSONException ignored) {}
    return retval;
  }

  /* check the forward and backward compatibility */
  private static void checkCompatibility(String version, String fcVersion) throws SemanticException {
    doCheckCompatibility(
        METADATA_FORMAT_VERSION,
        version,
        fcVersion);
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
    Map<String, String> partSpec = new TreeMap<String, String>();
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
