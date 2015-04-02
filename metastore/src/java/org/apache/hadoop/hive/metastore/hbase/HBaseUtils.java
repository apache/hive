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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData._Fields;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TFieldIdEnum;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Utility functions
 */
class HBaseUtils {

  final static Charset ENCODING = StandardCharsets.UTF_8;
  final static char KEY_SEPARATOR = ':';
  final static String KEY_SEPARATOR_STR = new String(new char[] {KEY_SEPARATOR});

  static final private Log LOG = LogFactory.getLog(HBaseUtils.class.getName());

  /**
   * Build a key for an object in hbase
   * @param components
   * @return
   */
  static byte[] buildKey(String... components) {
    return buildKey(false, components);
  }

  static byte[] buildKeyWithTrailingSeparator(String... components) {
    return buildKey(true, components);
  }

  private static byte[] buildKey(boolean trailingSeparator, String... components) {
    String protoKey = StringUtils.join(components, KEY_SEPARATOR);
    if (trailingSeparator) protoKey += KEY_SEPARATOR;
    return protoKey.getBytes(ENCODING);
  }

  private static HbaseMetastoreProto.Parameters buildParameters(Map<String, String> params) {
    List<HbaseMetastoreProto.ParameterEntry> entries =
        new ArrayList<HbaseMetastoreProto.ParameterEntry>();
    for (Map.Entry<String, String> e : params.entrySet()) {
      entries.add(
          HbaseMetastoreProto.ParameterEntry.newBuilder()
              .setKey(e.getKey())
              .setValue(e.getValue())
              .build());
    }
    return HbaseMetastoreProto.Parameters.newBuilder()
        .addAllParameter(entries)
        .build();
  }

  private static Map<String, String> buildParameters(HbaseMetastoreProto.Parameters protoParams) {
    Map<String, String> params = new HashMap<String, String>();
    for (HbaseMetastoreProto.ParameterEntry pe : protoParams.getParameterList()) {
      params.put(pe.getKey(), pe.getValue());
    }
    return params;
  }


  private static List<HbaseMetastoreProto.PrincipalPrivilegeSetEntry>
  buildPrincipalPrivilegeSetEntry(Map<String, List<PrivilegeGrantInfo>> entries) {
    List<HbaseMetastoreProto.PrincipalPrivilegeSetEntry> results =
        new ArrayList<HbaseMetastoreProto.PrincipalPrivilegeSetEntry>();
    for (Map.Entry<String, List<PrivilegeGrantInfo>> entry : entries.entrySet()) {
      results.add(HbaseMetastoreProto.PrincipalPrivilegeSetEntry.newBuilder()
          .setPrincipalName(entry.getKey())
          .addAllPrivileges(buildPrivilegeGrantInfo(entry.getValue()))
          .build());
    }
    return results;
  }

  private static List<HbaseMetastoreProto.PrivilegeGrantInfo> buildPrivilegeGrantInfo(
      List<PrivilegeGrantInfo> privileges) {
    List<HbaseMetastoreProto.PrivilegeGrantInfo> results =
        new ArrayList<HbaseMetastoreProto.PrivilegeGrantInfo>();
    for (PrivilegeGrantInfo privilege : privileges) {
      HbaseMetastoreProto.PrivilegeGrantInfo.Builder builder =
          HbaseMetastoreProto.PrivilegeGrantInfo.newBuilder();
      if (privilege.getPrivilege() != null) builder.setPrivilege(privilege.getPrivilege());
      builder.setCreateTime(privilege.getCreateTime());
      if (privilege.getGrantor() != null) builder.setGrantor(privilege.getGrantor());
      if (privilege.getGrantorType() != null) {
        builder.setGrantorType(convertPrincipalTypes(privilege.getGrantorType()));
      }
      builder.setGrantOption(privilege.isGrantOption());
      results.add(builder.build());
    }
    return results;
  }

  /**
   * Convert Thrift.PrincipalType to HbaseMetastoreProto.principalType
   * @param type
   * @return
   */
  static HbaseMetastoreProto.PrincipalType convertPrincipalTypes(PrincipalType type) {
    switch (type) {
      case USER: return HbaseMetastoreProto.PrincipalType.USER;
      case ROLE: return HbaseMetastoreProto.PrincipalType.ROLE;
      default: throw new RuntimeException("Unknown principal type " + type.toString());
    }
  }

  /**
   * Convert principalType from HbaseMetastoreProto to Thrift.PrincipalType
   * @param type
   * @return
   */
  static PrincipalType convertPrincipalTypes(HbaseMetastoreProto.PrincipalType type) {
    switch (type) {
      case USER: return PrincipalType.USER;
      case ROLE: return PrincipalType.ROLE;
      default: throw new RuntimeException("Unknown principal type " + type.toString());
    }
  }

  private static Map<String, List<PrivilegeGrantInfo>> convertPrincipalPrivilegeSetEntries(
      List<HbaseMetastoreProto.PrincipalPrivilegeSetEntry> entries) {
    Map<String, List<PrivilegeGrantInfo>> map =
        new HashMap<String, List<PrivilegeGrantInfo>>();
    for (HbaseMetastoreProto.PrincipalPrivilegeSetEntry entry : entries) {
      map.put(entry.getPrincipalName(), convertPrivilegeGrantInfos(entry.getPrivilegesList()));
    }
    return map;
  }

  private static List<PrivilegeGrantInfo> convertPrivilegeGrantInfos(
      List<HbaseMetastoreProto.PrivilegeGrantInfo> privileges) {
    List<PrivilegeGrantInfo> results = new ArrayList<PrivilegeGrantInfo>();
    for (HbaseMetastoreProto.PrivilegeGrantInfo proto : privileges) {
      PrivilegeGrantInfo pgi = new PrivilegeGrantInfo();
      if (proto.hasPrivilege()) pgi.setPrivilege(proto.getPrivilege());
      pgi.setCreateTime((int)proto.getCreateTime());
      if (proto.hasGrantor()) pgi.setGrantor(proto.getGrantor());
      if (proto.hasGrantorType()) {
        pgi.setGrantorType(convertPrincipalTypes(proto.getGrantorType()));
      }
      if (proto.hasGrantOption()) pgi.setGrantOption(proto.getGrantOption());
      results.add(pgi);
    }
    return results;
  }

  private static HbaseMetastoreProto.PrincipalPrivilegeSet
  buildPrincipalPrivilegeSet(PrincipalPrivilegeSet pps) {
    HbaseMetastoreProto.PrincipalPrivilegeSet.Builder builder =
        HbaseMetastoreProto.PrincipalPrivilegeSet.newBuilder();
    if (pps.getUserPrivileges() != null) {
      builder.addAllUsers(buildPrincipalPrivilegeSetEntry(pps.getUserPrivileges()));
    }
    if (pps.getRolePrivileges() != null) {
      builder.addAllRoles(buildPrincipalPrivilegeSetEntry(pps.getRolePrivileges()));
    }
    return builder.build();
  }

  private static PrincipalPrivilegeSet buildPrincipalPrivilegeSet(
      HbaseMetastoreProto.PrincipalPrivilegeSet proto) throws InvalidProtocolBufferException {
    PrincipalPrivilegeSet pps = new PrincipalPrivilegeSet();
    pps.setUserPrivileges(convertPrincipalPrivilegeSetEntries(proto.getUsersList()));
    pps.setRolePrivileges(convertPrincipalPrivilegeSetEntries(proto.getRolesList()));
    return pps;
  }
  /**
   * Serialize a PrincipalPrivilegeSet
   * @param pps
   * @return
   */
  static byte[] serializePrincipalPrivilegeSet(PrincipalPrivilegeSet pps) {
    return buildPrincipalPrivilegeSet(pps).toByteArray();
  }

  /**
   * Deserialize a PrincipalPrivilegeSet
   * @param serialized
   * @return
   * @throws InvalidProtocolBufferException
   */
  static PrincipalPrivilegeSet deserializePrincipalPrivilegeSet(byte[] serialized)
      throws InvalidProtocolBufferException {
    HbaseMetastoreProto.PrincipalPrivilegeSet proto =
        HbaseMetastoreProto.PrincipalPrivilegeSet.parseFrom(serialized);
    return buildPrincipalPrivilegeSet(proto);
  }

  /**
   * Serialize a role
   * @param role
   * @return two byte arrays, first contains the key, the second the serialized value.
   */
  static byte[][] serializeRole(Role role) {
    byte[][] result = new byte[2][];
    result[0] = buildKey(role.getRoleName());
    HbaseMetastoreProto.Role.Builder builder = HbaseMetastoreProto.Role.newBuilder();
    builder.setCreateTime(role.getCreateTime());
    if (role.getOwnerName() != null) builder.setOwnerName(role.getOwnerName());
    result[1] = builder.build().toByteArray();
    return result;
  }

  /**
   * Deserialize a role.  This method should be used when the rolename is already known as it
   * doesn't have to re-deserialize it.
   * @param roleName name of the role
   * @param value value fetched from hbase
   * @return A role
   * @throws InvalidProtocolBufferException
   */
  static Role deserializeRole(String roleName, byte[] value)
      throws InvalidProtocolBufferException {
    Role role = new Role();
    role.setRoleName(roleName);
    HbaseMetastoreProto.Role protoRole =
        HbaseMetastoreProto.Role.parseFrom(value);
    role.setCreateTime((int)protoRole.getCreateTime());
    if (protoRole.hasOwnerName()) role.setOwnerName(protoRole.getOwnerName());
    return role;
  }

  /**
   * Deserialize a role.  This method should be used when the rolename is not already known (eg
   * when doing a scan).
   * @param key key from hbase
   * @param value value from hbase
   * @return a role
   * @throws InvalidProtocolBufferException
   */
  static Role deserializeRole(byte[] key, byte[] value)
      throws InvalidProtocolBufferException {
    String roleName = new String(key, ENCODING);
    return deserializeRole(roleName, value);
  }

  /**
   * Serialize a list of role names
   * @param roles
   * @return
   */
  static byte[] serializeRoleList(List<String> roles) {
    return HbaseMetastoreProto.RoleList.newBuilder()
        .addAllRole(roles)
        .build()
        .toByteArray();
  }

  static List<String> deserializeRoleList(byte[] value) throws InvalidProtocolBufferException {
    HbaseMetastoreProto.RoleList proto = HbaseMetastoreProto.RoleList.parseFrom(value);
    return new ArrayList<String>(proto.getRoleList());
  }

  /**
   * Serialize a database
   * @param db
   * @return two byte arrays, first contains the key, the second the serialized value.
   */
  static byte[][] serializeDatabase(Database db) {
    byte[][] result = new byte[2][];
    result[0] = buildKey(db.getName());
    HbaseMetastoreProto.Database.Builder builder = HbaseMetastoreProto.Database.newBuilder();

    if (db.getDescription() != null) builder.setDescription(db.getDescription());
    if (db.getLocationUri() != null) builder.setUri(db.getLocationUri());
    if (db.getParameters() != null) builder.setParameters(buildParameters(db.getParameters()));
    if (db.getPrivileges() != null) {
      builder.setPrivileges(buildPrincipalPrivilegeSet(db.getPrivileges()));
    }
    if (db.getOwnerName() != null) builder.setOwnerName(db.getOwnerName());
    if (db.getOwnerType() != null) builder.setOwnerType(convertPrincipalTypes(db.getOwnerType()));

    result[1] = builder.build().toByteArray();
    return result;
  }

  /**
   * Deserialize a database.  This method should be used when the db anme is already known as it
   * doesn't have to re-deserialize it.
   * @param dbName name of the role
   * @param value value fetched from hbase
   * @return A database
   * @throws InvalidProtocolBufferException
   */
  static Database deserializeDatabase(String dbName, byte[] value)
      throws InvalidProtocolBufferException {
    Database db = new Database();
    db.setName(dbName);
    HbaseMetastoreProto.Database protoDb = HbaseMetastoreProto.Database.parseFrom(value);
    if (protoDb.hasDescription()) db.setDescription(protoDb.getDescription());
    if (protoDb.hasUri()) db.setLocationUri(protoDb.getUri());
    if (protoDb.hasParameters()) db.setParameters(buildParameters(protoDb.getParameters()));
    if (protoDb.hasPrivileges()) {
      db.setPrivileges(buildPrincipalPrivilegeSet(protoDb.getPrivileges()));
    }
    if (protoDb.hasOwnerName()) db.setOwnerName(protoDb.getOwnerName());
    if (protoDb.hasOwnerType()) db.setOwnerType(convertPrincipalTypes(protoDb.getOwnerType()));

    return db;
  }

  /**
   * Deserialize a database.  This method should be used when the db name is not already known (eg
   * when doing a scan).
   * @param key key from hbase
   * @param value value from hbase
   * @return a role
   * @throws InvalidProtocolBufferException
   */
  static Database deserializeDatabase(byte[] key, byte[] value)
      throws InvalidProtocolBufferException {
    String dbName = new String(key, ENCODING);
    return deserializeDatabase(dbName, value);
  }

  /**
   * Serialize a function
   * @param func function to serialize
   * @return two byte arrays, first contains the key, the second the value.
   */
  static byte[][] serializeFunction(Function func) {
    byte[][] result = new byte[2][];
    result[0] = buildKey(func.getDbName(), func.getFunctionName());
    HbaseMetastoreProto.Function.Builder builder = HbaseMetastoreProto.Function.newBuilder();
    if (func.getClassName() != null) builder.setClassName(func.getClassName());
    if (func.getOwnerName() != null) builder.setOwnerName(func.getOwnerName());
    if (func.getOwnerType() != null) {
      builder.setOwnerType(convertPrincipalTypes(func.getOwnerType()));
    }
    builder.setCreateTime(func.getCreateTime());
    if (func.getFunctionType() != null) {
      builder.setFunctionType(convertFunctionTypes(func.getFunctionType()));
    }
    if (func.getResourceUris() != null) {
      for (ResourceUri uri : func.getResourceUris()) {
        builder.addResourceUris(HbaseMetastoreProto.Function.ResourceUri.newBuilder()
            .setResourceType(convertResourceTypes(uri.getResourceType()))
            .setUri(uri.getUri()));
      }
    }
    result[1] = builder.build().toByteArray();
    return result;
  }

  /**
   * Deserialize a function.  This method should be used when the function and db name are
   * already known.
   * @param dbName name of the database the function is in
   * @param functionName name of the function
   * @param value serialized value of the function
   * @return function as an object
   * @throws InvalidProtocolBufferException
   */
  static Function deserializeFunction(String dbName, String functionName, byte[] value)
      throws InvalidProtocolBufferException {
    Function func = new Function();
    func.setDbName(dbName);
    func.setFunctionName(functionName);
    HbaseMetastoreProto.Function protoFunc = HbaseMetastoreProto.Function.parseFrom(value);
    if (protoFunc.hasClassName()) func.setClassName(protoFunc.getClassName());
    if (protoFunc.hasOwnerName()) func.setOwnerName(protoFunc.getOwnerName());
    if (protoFunc.hasOwnerType()) {
      func.setOwnerType(convertPrincipalTypes(protoFunc.getOwnerType()));
    }
    func.setCreateTime((int)protoFunc.getCreateTime());
    if (protoFunc.hasFunctionType()) {
      func.setFunctionType(convertFunctionTypes(protoFunc.getFunctionType()));
    }
    for (HbaseMetastoreProto.Function.ResourceUri protoUri : protoFunc.getResourceUrisList()) {
      func.addToResourceUris(new ResourceUri(convertResourceTypes(protoUri.getResourceType()),
                                             protoUri.getUri()));
    }
    return func;
  }

  /**
   * Deserialize a function.  This method should be used when the dbname and function name are
   * not already known, such as in a scan.
   * @param key key from hbase
   * @param value value from hbase
   * @return function object
   * @throws InvalidProtocolBufferException
   */
  static Function deserializeFunction(byte[] key, byte[] value)
      throws  InvalidProtocolBufferException {
    String[] keys = deserializeKey(key);
    return deserializeFunction(keys[0], keys[1], value);
  }

  private static HbaseMetastoreProto.Function.FunctionType convertFunctionTypes(FunctionType type) {
    switch (type) {
    case JAVA: return HbaseMetastoreProto.Function.FunctionType.JAVA;
    default: throw new RuntimeException("Unknown function type " + type.toString());
    }
  }

  private static FunctionType convertFunctionTypes(HbaseMetastoreProto.Function.FunctionType type) {
    switch (type) {
    case JAVA: return FunctionType.JAVA;
    default: throw new RuntimeException("Unknown function type " + type.toString());
    }
  }

  private static HbaseMetastoreProto.Function.ResourceUri.ResourceType
  convertResourceTypes(ResourceType type) {
    switch (type) {
    case JAR: return HbaseMetastoreProto.Function.ResourceUri.ResourceType.JAR;
    case FILE: return HbaseMetastoreProto.Function.ResourceUri.ResourceType.FILE;
    case ARCHIVE: return HbaseMetastoreProto.Function.ResourceUri.ResourceType.ARCHIVE;
    default: throw new RuntimeException("Unknown resource type " + type.toString());
    }
  }

  private static ResourceType convertResourceTypes(
      HbaseMetastoreProto.Function.ResourceUri.ResourceType type) {
    switch (type) {
    case JAR: return ResourceType.JAR;
    case FILE: return ResourceType.FILE;
    case ARCHIVE: return ResourceType.ARCHIVE;
    default: throw new RuntimeException("Unknown resource type " + type.toString());
    }
  }

  private static List<FieldSchema>
  convertFieldSchemaListFromProto(List<HbaseMetastoreProto.FieldSchema> protoList) {
    List<FieldSchema> schemas = new ArrayList<FieldSchema>(protoList.size());
    for (HbaseMetastoreProto.FieldSchema proto : protoList) {
      schemas.add(new FieldSchema(proto.getName(), proto.getType(),
          proto.hasComment() ? proto.getComment() : null));
    }
    return schemas;
  }

  private static List<HbaseMetastoreProto.FieldSchema>
  convertFieldSchemaListToProto(List<FieldSchema> schemas) {
    List<HbaseMetastoreProto.FieldSchema> protoList =
        new ArrayList<HbaseMetastoreProto.FieldSchema>(schemas.size());
    for (FieldSchema fs : schemas) {
      HbaseMetastoreProto.FieldSchema.Builder builder =
          HbaseMetastoreProto.FieldSchema.newBuilder();
      builder
          .setName(fs.getName())
          .setType(fs.getType());
      if (fs.getComment() != null) builder.setComment(fs.getComment());
      protoList.add(builder.build());
    }
    return protoList;
  }

  /**
   * Serialize a storage descriptor.
   * @param sd storage descriptor to serialize
   * @return serialized storage descriptor.
   */
  static byte[] serializeStorageDescriptor(StorageDescriptor sd)  {
    HbaseMetastoreProto.StorageDescriptor.Builder builder =
        HbaseMetastoreProto.StorageDescriptor.newBuilder();
    builder.addAllCols(convertFieldSchemaListToProto(sd.getCols()));
    if (sd.getInputFormat() != null) {
      builder.setInputFormat(sd.getInputFormat());
    }
    if (sd.getOutputFormat() != null) {
      builder.setOutputFormat(sd.getOutputFormat());
    }
    builder.setIsCompressed(sd.isCompressed());
    builder.setNumBuckets(sd.getNumBuckets());
    if (sd.getSerdeInfo() != null) {
      HbaseMetastoreProto.StorageDescriptor.SerDeInfo.Builder serdeBuilder =
          HbaseMetastoreProto.StorageDescriptor.SerDeInfo.newBuilder();
      SerDeInfo serde = sd.getSerdeInfo();
      if (serde.getName() != null) {
        serdeBuilder.setName(serde.getName());
      }
      if (serde.getSerializationLib() != null) {
        serdeBuilder.setSerializationLib(serde.getSerializationLib());
      }
      if (serde.getParameters() != null) {
        serdeBuilder.setParameters(buildParameters(serde.getParameters()));
      }
      builder.setSerdeInfo(serdeBuilder);
    }
    if (sd.getBucketCols() != null) {
      builder.addAllBucketCols(sd.getBucketCols());
    }
    if (sd.getSortCols() != null) {
      List<Order> orders = sd.getSortCols();
      List<HbaseMetastoreProto.StorageDescriptor.Order> protoList =
          new ArrayList<HbaseMetastoreProto.StorageDescriptor.Order>(orders.size());
      for (Order order : orders) {
        protoList.add(HbaseMetastoreProto.StorageDescriptor.Order.newBuilder()
            .setColumnName(order.getCol())
            .setOrder(order.getOrder())
            .build());
      }
      builder.addAllSortCols(protoList);
    }
    if (sd.getSkewedInfo() != null) {
      HbaseMetastoreProto.StorageDescriptor.SkewedInfo.Builder skewBuilder =
          HbaseMetastoreProto.StorageDescriptor.SkewedInfo.newBuilder();
      SkewedInfo skewed = sd.getSkewedInfo();
      if (skewed.getSkewedColNames() != null) {
        skewBuilder.addAllSkewedColNames(skewed.getSkewedColNames());
      }
      if (skewed.getSkewedColValues() != null) {
        for (List<String> innerList : skewed.getSkewedColValues()) {
          HbaseMetastoreProto.StorageDescriptor.SkewedInfo.SkewedColValueList.Builder listBuilder =
              HbaseMetastoreProto.StorageDescriptor.SkewedInfo.SkewedColValueList.newBuilder();
          listBuilder.addAllSkewedColValue(innerList);
          skewBuilder.addSkewedColValues(listBuilder);
        }
      }
      if (skewed.getSkewedColValueLocationMaps() != null) {
        for (Map.Entry<List<String>, String> e : skewed.getSkewedColValueLocationMaps().entrySet()) {
          HbaseMetastoreProto.StorageDescriptor.SkewedInfo.SkewedColValueLocationMap.Builder mapBuilder =
              HbaseMetastoreProto.StorageDescriptor.SkewedInfo.SkewedColValueLocationMap.newBuilder();
          mapBuilder.addAllKey(e.getKey());
          mapBuilder.setValue(e.getValue());
          skewBuilder.addSkewedColValueLocationMaps(mapBuilder);
        }
      }
      builder.setSkewedInfo(skewBuilder);
    }
    builder.setStoredAsSubDirectories(sd.isStoredAsSubDirectories());

    return builder.build().toByteArray();
  }

  /**
   * Produce a hash for the storage descriptor
   * @param sd storage descriptor to hash
   * @param md message descriptor to use to generate the hash
   * @return the hash as a byte array
   */
  static byte[] hashStorageDescriptor(StorageDescriptor sd, MessageDigest md)  {
    // Note all maps and lists have to be absolutely sorted.  Otherwise we'll produce different
    // results for hashes based on the OS or JVM being used.
    md.reset();
    for (FieldSchema fs : sd.getCols()) {
      md.update(fs.getName().getBytes(ENCODING));
      md.update(fs.getType().getBytes(ENCODING));
      if (fs.getComment() != null) md.update(fs.getComment().getBytes(ENCODING));
    }
    if (sd.getInputFormat() != null) {
      md.update(sd.getInputFormat().getBytes(ENCODING));
    }
    if (sd.getOutputFormat() != null) {
      md.update(sd.getOutputFormat().getBytes(ENCODING));
    }
    md.update(sd.isCompressed() ? "true".getBytes(ENCODING) : "false".getBytes(ENCODING));
    md.update(Integer.toString(sd.getNumBuckets()).getBytes(ENCODING));
    if (sd.getSerdeInfo() != null) {
      SerDeInfo serde = sd.getSerdeInfo();
      if (serde.getName() != null) {
        md.update(serde.getName().getBytes(ENCODING));
      }
      if (serde.getSerializationLib() != null) {
        md.update(serde.getSerializationLib().getBytes(ENCODING));
      }
      if (serde.getParameters() != null) {
        SortedMap<String, String> params = new TreeMap<String, String>(serde.getParameters());
        for (Map.Entry<String, String> param : params.entrySet()) {
          md.update(param.getKey().getBytes(ENCODING));
          md.update(param.getValue().getBytes(ENCODING));
        }
      }
    }
    if (sd.getBucketCols() != null) {
      SortedSet<String> bucketCols = new TreeSet<String>(sd.getBucketCols());
      for (String bucket : bucketCols) md.update(bucket.getBytes(ENCODING));
    }
    if (sd.getSortCols() != null) {
      SortedSet<Order> orders = new TreeSet<Order>(sd.getSortCols());
      for (Order order : orders) {
        md.update(order.getCol().getBytes(ENCODING));
        md.update(Integer.toString(order.getOrder()).getBytes(ENCODING));
      }
    }
    if (sd.getSkewedInfo() != null) {
      SkewedInfo skewed = sd.getSkewedInfo();
      if (skewed.getSkewedColNames() != null) {
        SortedSet<String> colnames = new TreeSet<String>(skewed.getSkewedColNames());
        for (String colname : colnames) md.update(colname.getBytes(ENCODING));
      }
      if (skewed.getSkewedColValues() != null) {
        SortedSet<String> sortedOuterList = new TreeSet<String>();
        for (List<String> innerList : skewed.getSkewedColValues()) {
          SortedSet<String> sortedInnerList = new TreeSet<String>(innerList);
          sortedOuterList.add(StringUtils.join(sortedInnerList, "."));
        }
        for (String colval : sortedOuterList) md.update(colval.getBytes(ENCODING));
      }
      if (skewed.getSkewedColValueLocationMaps() != null) {
        SortedMap<String, String> sortedMap = new TreeMap<String, String>();
        for (Map.Entry<List<String>, String> smap : skewed.getSkewedColValueLocationMaps().entrySet()) {
          SortedSet<String> sortedKey = new TreeSet<String>(smap.getKey());
          sortedMap.put(StringUtils.join(sortedKey, "."), smap.getValue());
        }
        for (Map.Entry<String, String> e : sortedMap.entrySet()) {
          md.update(e.getKey().getBytes(ENCODING));
          md.update(e.getValue().getBytes(ENCODING));
        }
      }
    }

    return md.digest();
  }

  static StorageDescriptor deserializeStorageDescriptor(byte[] serialized)
      throws InvalidProtocolBufferException {
    HbaseMetastoreProto.StorageDescriptor proto =
        HbaseMetastoreProto.StorageDescriptor.parseFrom(serialized);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(convertFieldSchemaListFromProto(proto.getColsList()));
    if (proto.hasInputFormat()) sd.setInputFormat(proto.getInputFormat());
    if (proto.hasOutputFormat()) sd.setOutputFormat(proto.getOutputFormat());
    sd.setCompressed(proto.getIsCompressed());
    sd.setNumBuckets(proto.getNumBuckets());
    if (proto.hasSerdeInfo()) {
      SerDeInfo serde = new SerDeInfo();
      serde.setName(proto.getSerdeInfo().getName());
      serde.setSerializationLib(proto.getSerdeInfo().getSerializationLib());
      serde.setParameters(buildParameters(proto.getSerdeInfo().getParameters()));
      sd.setSerdeInfo(serde);
    }
    sd.setBucketCols(new ArrayList<String>(proto.getBucketColsList()));
    List<Order> sortCols = new ArrayList<Order>();
    for (HbaseMetastoreProto.StorageDescriptor.Order protoOrder : proto.getSortColsList()) {
      sortCols.add(new Order(protoOrder.getColumnName(), protoOrder.getOrder()));
    }
    sd.setSortCols(sortCols);
    if (proto.hasSkewedInfo()) {
      SkewedInfo skewed = new SkewedInfo();
      skewed
          .setSkewedColNames(new ArrayList<String>(proto.getSkewedInfo().getSkewedColNamesList()));
      for (HbaseMetastoreProto.StorageDescriptor.SkewedInfo.SkewedColValueList innerList :
          proto.getSkewedInfo().getSkewedColValuesList()) {
        skewed.addToSkewedColValues(new ArrayList<String>(innerList.getSkewedColValueList()));
      }
      Map<List<String>, String> colMaps = new HashMap<List<String>, String>();
      for (HbaseMetastoreProto.StorageDescriptor.SkewedInfo.SkewedColValueLocationMap map :
          proto.getSkewedInfo().getSkewedColValueLocationMapsList()) {
        colMaps.put(new ArrayList<String>(map.getKeyList()), map.getValue());
      }
      skewed.setSkewedColValueLocationMaps(colMaps);
      sd.setSkewedInfo(skewed);
    }
    if (proto.hasStoredAsSubDirectories()) {
      sd.setStoredAsSubDirectories(proto.getStoredAsSubDirectories());
    }
    return sd;
  }

  /**
   * Serialize a partition
   * @param part partition object
   * @param sdHash hash that is being used as a key for the enclosed storage descriptor
   * @return First element is the key, second is the serialized partition
   */
  static byte[][] serializePartition(Partition part, byte[] sdHash) {
    byte[][] result = new byte[2][];
    result[0] = buildPartitionKey(part.getDbName(), part.getTableName(), part.getValues());
    HbaseMetastoreProto.Partition.Builder builder = HbaseMetastoreProto.Partition.newBuilder();
    builder
        .setCreateTime(part.getCreateTime())
        .setLastAccessTime(part.getLastAccessTime());
    if (part.getSd().getLocation() != null) builder.setLocation(part.getSd().getLocation());
    if (part.getSd().getParameters() != null) {
      builder.setSdParameters(buildParameters(part.getSd().getParameters()));
    }
    builder.setSdHash(ByteString.copyFrom(sdHash));
    if (part.getParameters() != null) builder.setParameters(buildParameters(part.getParameters()));
    result[1] = builder.build().toByteArray();
    return result;
  }

  static byte[] buildPartitionKey(String dbName, String tableName, List<String> partVals) {
    Deque<String> keyParts = new ArrayDeque<String>(partVals);
    keyParts.addFirst(tableName);
    keyParts.addFirst(dbName);
    return buildKey(keyParts.toArray(new String[keyParts.size()]));
  }

  static class StorageDescriptorParts {
    byte[] sdHash;
    String location;
    Map<String, String> parameters;
    Partition containingPartition;
    Table containingTable;
  }

  static void assembleStorageDescriptor(StorageDescriptor sd, StorageDescriptorParts parts) {
    SharedStorageDescriptor ssd = new SharedStorageDescriptor();
    ssd.setLocation(parts.location);
    ssd.setParameters(parts.parameters);
    ssd.setShared(sd);
    if (parts.containingPartition != null) {
      parts.containingPartition.setSd(ssd);
    } else if (parts.containingTable != null) {
      parts.containingTable.setSd(ssd);
    } else {
      throw new RuntimeException("Need either a partition or a table");
    }
  }

  /**
   * Deserialize a partition.  This version should be used when the partition key is not already
   * known (eg a scan).
   * @param key the key fetched from HBase
   * @param serialized the value fetched from HBase
   * @return A struct that contains the partition plus parts of the storage descriptor
   */
  static StorageDescriptorParts deserializePartition(byte[] key, byte[] serialized)
      throws InvalidProtocolBufferException {
    String[] keys = deserializeKey(key);
    return deserializePartition(keys[0], keys[1],
        Arrays.asList(Arrays.copyOfRange(keys, 2, keys.length)), serialized);
  }

  /**
   * Deserialize a partition.  This version should be used when the partition key is
   * known (eg a get).
   * @param dbName database name
   * @param tableName table name
   * @param partVals partition values
   * @param serialized the value fetched from HBase
   * @return A struct that contains the partition plus parts of the storage descriptor
   */
  static StorageDescriptorParts deserializePartition(String dbName, String tableName,
                                                     List<String> partVals, byte[] serialized)
      throws InvalidProtocolBufferException {
    HbaseMetastoreProto.Partition proto = HbaseMetastoreProto.Partition.parseFrom(serialized);
    Partition part = new Partition();
    StorageDescriptorParts sdParts = new StorageDescriptorParts();
    sdParts.containingPartition = part;
    part.setDbName(dbName);
    part.setTableName(tableName);
    part.setValues(partVals);
    part.setCreateTime((int)proto.getCreateTime());
    part.setLastAccessTime((int)proto.getLastAccessTime());
    if (proto.hasLocation()) sdParts.location = proto.getLocation();
    if (proto.hasSdParameters()) sdParts.parameters = buildParameters(proto.getSdParameters());
    sdParts.sdHash = proto.getSdHash().toByteArray();
    if (proto.hasParameters()) part.setParameters(buildParameters(proto.getParameters()));
    return sdParts;
  }

  private static String[] deserializeKey(byte[] key) {
    String k = new String(key, ENCODING);
    return k.split(":");
  }

  /**
   * Serialize a table
   * @param table table object
   * @param sdHash hash that is being used as a key for the enclosed storage descriptor
   * @return First element is the key, second is the serialized table
   */
  static byte[][] serializeTable(Table table, byte[] sdHash) {
    byte[][] result = new byte[2][];
    result[0] = buildKey(table.getDbName(), table.getTableName());
    HbaseMetastoreProto.Table.Builder builder = HbaseMetastoreProto.Table.newBuilder();
    if (table.getOwner() != null) builder.setOwner(table.getOwner());
    builder
        .setCreateTime(table.getCreateTime())
        .setLastAccessTime(table.getLastAccessTime())
        .setRetention(table.getRetention());
    if (table.getSd().getLocation() != null) builder.setLocation(table.getSd().getLocation());
    if (table.getSd().getParameters() != null) {
      builder.setSdParameters(buildParameters(table.getSd().getParameters()));
    }
    builder.setSdHash(ByteString.copyFrom(sdHash));
    if (table.getPartitionKeys() != null) {
      builder.addAllPartitionKeys(convertFieldSchemaListToProto(table.getPartitionKeys()));
    }
    if (table.getParameters() != null) {
      builder.setParameters(buildParameters(table.getParameters()));
    }
    if (table.getViewOriginalText() != null) {
      builder.setViewOriginalText(table.getViewOriginalText());
    }
    if (table.getViewExpandedText() != null) {
      builder.setViewExpandedText(table.getViewExpandedText());
    }
    if (table.getTableType() != null) builder.setTableType(table.getTableType());
    if (table.getPrivileges() != null) {
      builder.setPrivileges(buildPrincipalPrivilegeSet(table.getPrivileges()));
    }
    builder.setIsTemporary(table.isTemporary());
    result[1] = builder.build().toByteArray();
    return result;
  }

  /**
   * Deserialize a table.  This version should be used when the table key is not already
   * known (eg a scan).
   * @param key the key fetched from HBase
   * @param serialized the value fetched from HBase
   * @return A struct that contains the table plus parts of the storage descriptor
   */
  static StorageDescriptorParts deserializeTable(byte[] key, byte[] serialized)
      throws InvalidProtocolBufferException {
    String[] keys = deserializeKey(key);
    return deserializeTable(keys[0], keys[1], serialized);
  }

  /**
   * Deserialize a table.  This version should be used when the table key is
   * known (eg a get).
   * @param dbName database name
   * @param tableName table name
   * @param serialized the value fetched from HBase
   * @return A struct that contains the partition plus parts of the storage descriptor
   */
  static StorageDescriptorParts deserializeTable(String dbName, String tableName,
                                                 byte[] serialized)
      throws InvalidProtocolBufferException {
    HbaseMetastoreProto.Table proto = HbaseMetastoreProto.Table.parseFrom(serialized);
    Table table = new Table();
    StorageDescriptorParts sdParts = new StorageDescriptorParts();
    sdParts.containingTable = table;
    table.setDbName(dbName);
    table.setTableName(tableName);
    table.setOwner(proto.getOwner());
    table.setCreateTime((int)proto.getCreateTime());
    table.setLastAccessTime((int)proto.getLastAccessTime());
    table.setRetention((int)proto.getRetention());
    if (proto.hasLocation()) sdParts.location = proto.getLocation();
    if (proto.hasSdParameters()) sdParts.parameters = buildParameters(proto.getSdParameters());
    sdParts.sdHash = proto.getSdHash().toByteArray();
    table.setPartitionKeys(convertFieldSchemaListFromProto(proto.getPartitionKeysList()));
    table.setParameters(buildParameters(proto.getParameters()));
    if (proto.hasViewOriginalText()) table.setViewOriginalText(proto.getViewOriginalText());
    if (proto.hasViewExpandedText()) table.setViewExpandedText(proto.getViewExpandedText());
    table.setTableType(proto.getTableType());
    if (proto.hasPrivileges()) {
      table.setPrivileges(buildPrincipalPrivilegeSet(proto.getPrivileges()));
    }
    if (proto.hasIsTemporary()) table.setTemporary(proto.getIsTemporary());
    return sdParts;
  }

  static byte[] serializeStatsForOneColumn(ColumnStatistics partitionColumnStats, ColumnStatisticsObj colStats)
      throws IOException {
    HbaseMetastoreProto.ColumnStats.Builder builder = HbaseMetastoreProto.ColumnStats.newBuilder();
    builder.setLastAnalyzed(partitionColumnStats.getStatsDesc().getLastAnalyzed());
    if (colStats.getColType() == null) {
      throw new RuntimeException("Column type must be set");
    }
    builder.setColumnType(colStats.getColType());
    ColumnStatisticsData colData = colStats.getStatsData();
    switch (colData.getSetField()) {
      case BOOLEAN_STATS:
        BooleanColumnStatsData boolData = colData.getBooleanStats();
        builder.setNumNulls(boolData.getNumNulls());
        builder.setBoolStats(
            HbaseMetastoreProto.ColumnStats.BooleanStats.newBuilder()
                .setNumTrues(boolData.getNumTrues())
                .setNumFalses(boolData.getNumFalses())
                .build());
        break;

      case LONG_STATS:
        LongColumnStatsData longData = colData.getLongStats();
        builder.setNumNulls(longData.getNumNulls());
        builder.setNumDistinctValues(longData.getNumDVs());
        builder.setLongStats(
            HbaseMetastoreProto.ColumnStats.LongStats.newBuilder()
                .setLowValue(longData.getLowValue())
                .setHighValue(longData.getHighValue())
                .build());
        break;

      case DOUBLE_STATS:
        DoubleColumnStatsData doubleData = colData.getDoubleStats();
        builder.setNumNulls(doubleData.getNumNulls());
        builder.setNumDistinctValues(doubleData.getNumDVs());
        builder.setDoubleStats(
            HbaseMetastoreProto.ColumnStats.DoubleStats.newBuilder()
                .setLowValue(doubleData.getLowValue())
                .setHighValue(doubleData.getHighValue())
                .build());
        break;

      case STRING_STATS:
        StringColumnStatsData stringData = colData.getStringStats();
        builder.setNumNulls(stringData.getNumNulls());
        builder.setNumDistinctValues(stringData.getNumDVs());
        builder.setStringStats(
            HbaseMetastoreProto.ColumnStats.StringStats.newBuilder()
                .setMaxColLength(stringData.getMaxColLen())
                .setAvgColLength(stringData.getAvgColLen())
                .build());
        break;

      case BINARY_STATS:
        BinaryColumnStatsData binaryData = colData.getBinaryStats();
        builder.setNumNulls(binaryData.getNumNulls());
        builder.setBinaryStats(
            HbaseMetastoreProto.ColumnStats.StringStats.newBuilder()
                .setMaxColLength(binaryData.getMaxColLen())
                .setAvgColLength(binaryData.getAvgColLen())
                .build());
        break;

      case DECIMAL_STATS:
        DecimalColumnStatsData decimalData = colData.getDecimalStats();
        builder.setNumNulls(decimalData.getNumNulls());
        builder.setNumDistinctValues(decimalData.getNumDVs());
        builder.setDecimalStats(
            HbaseMetastoreProto.ColumnStats.DecimalStats.newBuilder()
                .setLowValue(
                    HbaseMetastoreProto.ColumnStats.DecimalStats.Decimal.newBuilder()
                      .setUnscaled(ByteString.copyFrom(decimalData.getLowValue().getUnscaled()))
                      .setScale(decimalData.getLowValue().getScale())
                      .build())
                .setHighValue(
                    HbaseMetastoreProto.ColumnStats.DecimalStats.Decimal.newBuilder()
                    .setUnscaled(ByteString.copyFrom(decimalData.getHighValue().getUnscaled()))
                    .setScale(decimalData.getHighValue().getScale())
                    .build()))
                .build();
        break;

      default:
        throw new RuntimeException("Woh, bad.  Unknown stats type!");
    }
    return builder.build().toByteArray();
  }

  static ColumnStatisticsObj deserializeStatsForOneColumn(ColumnStatistics partitionColumnStats,
      byte[] bytes) throws IOException {
    HbaseMetastoreProto.ColumnStats proto = HbaseMetastoreProto.ColumnStats.parseFrom(bytes);
    ColumnStatisticsObj colStats = new ColumnStatisticsObj();
    long lastAnalyzed = proto.getLastAnalyzed();
    if (partitionColumnStats != null) {
      partitionColumnStats.getStatsDesc().setLastAnalyzed(
          Math.max(lastAnalyzed, partitionColumnStats.getStatsDesc().getLastAnalyzed()));
    }
    colStats.setColType(proto.getColumnType());

    ColumnStatisticsData colData = new ColumnStatisticsData();
    if (proto.hasBoolStats()) {
      BooleanColumnStatsData boolData = new BooleanColumnStatsData();
      boolData.setNumTrues(proto.getBoolStats().getNumTrues());
      boolData.setNumFalses(proto.getBoolStats().getNumFalses());
      boolData.setNumNulls(proto.getNumNulls());
      colData.setBooleanStats(boolData);
    } else if (proto.hasLongStats()) {
      LongColumnStatsData longData = new LongColumnStatsData();
      if (proto.getLongStats().hasLowValue()) {
        longData.setLowValue(proto.getLongStats().getLowValue());
      }
      if (proto.getLongStats().hasHighValue()) {
        longData.setHighValue(proto.getLongStats().getHighValue());
      }
      longData.setNumNulls(proto.getNumNulls());
      longData.setNumDVs(proto.getNumDistinctValues());
      colData.setLongStats(longData);
    } else if (proto.hasDoubleStats()) {
      DoubleColumnStatsData doubleData = new DoubleColumnStatsData();
      if (proto.getDoubleStats().hasLowValue()) {
        doubleData.setLowValue(proto.getDoubleStats().getLowValue());
      }
      if (proto.getDoubleStats().hasHighValue()) {
        doubleData.setHighValue(proto.getDoubleStats().getHighValue());
      }
      doubleData.setNumNulls(proto.getNumNulls());
      doubleData.setNumDVs(proto.getNumDistinctValues());
      colData.setDoubleStats(doubleData);
    } else if (proto.hasStringStats()) {
      StringColumnStatsData stringData = new StringColumnStatsData();
      stringData.setMaxColLen(proto.getStringStats().getMaxColLength());
      stringData.setAvgColLen(proto.getStringStats().getAvgColLength());
      stringData.setNumNulls(proto.getNumNulls());
      stringData.setNumDVs(proto.getNumDistinctValues());
      colData.setStringStats(stringData);
    } else if (proto.hasBinaryStats()) {
      BinaryColumnStatsData binaryData = new BinaryColumnStatsData();
      binaryData.setMaxColLen(proto.getBinaryStats().getMaxColLength());
      binaryData.setAvgColLen(proto.getBinaryStats().getAvgColLength());
      binaryData.setNumNulls(proto.getNumNulls());
      colData.setBinaryStats(binaryData);
    } else if (proto.hasDecimalStats()) {
      DecimalColumnStatsData decimalData = new DecimalColumnStatsData();
      if (proto.getDecimalStats().hasHighValue()) {
        Decimal hiVal = new Decimal();
        hiVal.setUnscaled(proto.getDecimalStats().getHighValue().getUnscaled().toByteArray());
        hiVal.setScale((short) proto.getDecimalStats().getHighValue().getScale());
        decimalData.setHighValue(hiVal);
      }
      if (proto.getDecimalStats().hasLowValue()) {
        Decimal loVal = new Decimal();
        loVal.setUnscaled(proto.getDecimalStats().getLowValue().getUnscaled().toByteArray());
        loVal.setScale((short) proto.getDecimalStats().getLowValue().getScale());
        decimalData.setLowValue(loVal);
      }
      decimalData.setNumNulls(proto.getNumNulls());
      decimalData.setNumDVs(proto.getNumDistinctValues());
      colData.setDecimalStats(decimalData);
    } else {
      throw new RuntimeException("Woh, bad.  Unknown stats type!");
    }
    colStats.setStatsData(colData);
    return colStats;
  }

  /**
   * @param keyStart byte array representing the start prefix
   * @return byte array corresponding to the next possible prefix
   */
  static byte[] getEndPrefix(byte[] keyStart) {
    if (keyStart == null) {
      return null;
    }
    // Since this is a prefix and not full key, the usual hbase technique of
    // appending 0 byte does not work. Instead of that, increment the last byte.
    byte[] keyEnd = Arrays.copyOf(keyStart, keyStart.length);
    keyEnd[keyEnd.length - 1]++;
    return keyEnd;
  }
}
