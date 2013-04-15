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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * EximUtil. Utility methods for the export/import semantic
 * analyzers.
 *
 */
public class EximUtil {

  private static Log LOG = LogFactory.getLog(EximUtil.class);

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
      LOG.debug("Path before norm :" + path);
      // generate absolute path relative to home directory
      if (!path.startsWith("/")) {
        if (testMode) {
          path = (new Path(System.getProperty("build.dir.hive"),
              path)).toUri().getPath();
        } else {
          path = (new Path(new Path("/user/" + System.getProperty("user.name")),
              path)).toUri().getPath();
        }
      }
      // set correct scheme and authority
      if (StringUtils.isEmpty(scheme)) {
        if (testMode) {
          scheme = "pfile";
        } else {
          scheme = "hdfs";
        }
      }

      // if scheme is specified but not authority then use the default
      // authority
      if (StringUtils.isEmpty(authority)) {
        URI defaultURI = FileSystem.get(conf).getUri();
        authority = defaultURI.getAuthority();
      }

      LOG.debug("Scheme:" + scheme + ", authority:" + authority + ", path:" + path);
      Collection<String> eximSchemes = conf.getStringCollection(
          HiveConf.ConfVars.HIVE_EXIM_URI_SCHEME_WL.varname);
      if (!eximSchemes.contains(scheme)) {
        throw new SemanticException(
            ErrorMsg.INVALID_PATH.getMsg(
                "only the following file systems accepted for export/import : "
                    + conf.get(HiveConf.ConfVars.HIVE_EXIM_URI_SCHEME_WL.varname)));
      }

      try {
        return new URI(scheme, authority, path, null, null);
      } catch (URISyntaxException e) {
        throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(), e);
      }
    } catch (IOException e) {
      throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg(), e);
    }
  }

  static void validateTable(org.apache.hadoop.hive.ql.metadata.Table table) throws SemanticException {
    if (table.isOffline()) {
      throw new SemanticException(
          ErrorMsg.OFFLINE_TABLE_OR_PARTITION.getMsg(":Table "
              + table.getTableName()));
    }
    if (table.isView()) {
      throw new SemanticException(ErrorMsg.DML_AGAINST_VIEW.getMsg());
    }
    if (table.isNonNative()) {
      throw new SemanticException(ErrorMsg.EXIM_FOR_NON_NATIVE.getMsg());
    }
  }

  public static String relativeToAbsolutePath(HiveConf conf, String location) throws SemanticException {
    boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODE);
    if (testMode) {
      URI uri = new Path(location).toUri();
      String scheme = uri.getScheme();
      String authority = uri.getAuthority();
      String path = uri.getPath();
      if (!path.startsWith("/")) {
          path = (new Path(System.getProperty("build.dir.hive"),
              path)).toUri().getPath();
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
      //no-op for non-test mode for now
      return location;
    }
  }

  /* major version number should match for backward compatibility */
  public static final String METADATA_FORMAT_VERSION = "0.1";
  /* If null, then the major version number should match */
  public static final String METADATA_FORMAT_FORWARD_COMPATIBLE_VERSION = null;

  public static void createExportDump(FileSystem fs, Path metadataPath, org.apache.hadoop.hive.ql.metadata.Table tableHandle,
      List<org.apache.hadoop.hive.ql.metadata.Partition> partitions) throws SemanticException, IOException {
    try {
      JSONObject jsonContainer = new JSONObject();
      jsonContainer.put("version", METADATA_FORMAT_VERSION);
      if (METADATA_FORMAT_FORWARD_COMPATIBLE_VERSION != null) {
        jsonContainer.put("fcversion", METADATA_FORMAT_FORWARD_COMPATIBLE_VERSION);
      }
      TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
      try {
        String tableDesc = serializer.toString(tableHandle.getTTable(), "UTF-8");
        jsonContainer.put("table", tableDesc);
        JSONArray jsonPartitions = new JSONArray();
        if (partitions != null) {
          for (org.apache.hadoop.hive.ql.metadata.Partition partition : partitions) {
            String partDesc = serializer.toString(partition.getTPartition(), "UTF-8");
            jsonPartitions.put(partDesc);
          }
        }
        jsonContainer.put("partitions", jsonPartitions);
      } catch (TException e) {
        throw new SemanticException(
            ErrorMsg.GENERIC_ERROR
                .getMsg("Exception while serializing the metastore objects"), e);
      }
      OutputStream out = fs.create(metadataPath);
      out.write(jsonContainer.toString().getBytes("UTF-8"));
      out.close();

    } catch (JSONException e) {
      throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg("Error in serializing metadata"), e);
    }
  }

  public static Map.Entry<Table, List<Partition>>
      readMetaData(FileSystem fs, Path metadataPath)
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
      String fcversion = null;
      try {
        fcversion = jsonContainer.getString("fcversion");
      } catch (JSONException ignored) {}
      checkCompatibility(version, fcversion);
      String tableDesc = jsonContainer.getString("table");
      Table table = new Table();
      TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());
      deserializer.deserialize(table, tableDesc, "UTF-8");
      JSONArray jsonPartitions = new JSONArray(jsonContainer.getString("partitions"));
      List<Partition> partitionsList = new ArrayList<Partition>(jsonPartitions.length());
      for (int i = 0; i < jsonPartitions.length(); ++i) {
        String partDesc = jsonPartitions.getString(i);
        Partition partition = new Partition();
        deserializer.deserialize(partition, partDesc, "UTF-8");
        partitionsList.add(partition);
      }
      return new AbstractMap.SimpleEntry<Table, List<Partition>>(table, partitionsList);
    } catch (JSONException e) {
      throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg("Error in serializing metadata"), e);
    } catch (TException e) {
      throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg("Error in serializing metadata"), e);
    } finally {
      if (mdstream != null) {
        mdstream.close();
      }
    }
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
}
