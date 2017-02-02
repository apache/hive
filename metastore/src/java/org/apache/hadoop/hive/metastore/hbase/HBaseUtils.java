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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDeWithEndPrefix;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.BloomFilter;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Utility functions
 */
public class HBaseUtils {

  final static Charset ENCODING = StandardCharsets.UTF_8;
  final static char KEY_SEPARATOR = '\u0001';
  final static String KEY_SEPARATOR_STR = new String(new char[] {KEY_SEPARATOR});

  static final private Logger LOG = LoggerFactory.getLogger(HBaseUtils.class.getName());

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
    List<HbaseMetastoreProto.ParameterEntry> entries = new ArrayList<>();
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
    Map<String, String> params = new HashMap<>();
    for (HbaseMetastoreProto.ParameterEntry pe : protoParams.getParameterList()) {
      params.put(pe.getKey(), pe.getValue());
    }
    return params;
  }


  private static List<HbaseMetastoreProto.PrincipalPrivilegeSetEntry>
  buildPrincipalPrivilegeSetEntry(Map<String, List<PrivilegeGrantInfo>> entries) {
    List<HbaseMetastoreProto.PrincipalPrivilegeSetEntry> results = new ArrayList<>();
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
    List<HbaseMetastoreProto.PrivilegeGrantInfo> results = new ArrayList<>();
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
    Map<String, List<PrivilegeGrantInfo>> map = new HashMap<>();
    for (HbaseMetastoreProto.PrincipalPrivilegeSetEntry entry : entries) {
      map.put(entry.getPrincipalName(), convertPrivilegeGrantInfos(entry.getPrivilegesList()));
    }
    return map;
  }

  private static List<PrivilegeGrantInfo> convertPrivilegeGrantInfos(
      List<HbaseMetastoreProto.PrivilegeGrantInfo> privileges) {
    List<PrivilegeGrantInfo> results = new ArrayList<>();
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
    PrincipalPrivilegeSet pps = null;
    if (!proto.getUsersList().isEmpty() || !proto.getRolesList().isEmpty()) {
      pps = new PrincipalPrivilegeSet();
      if (!proto.getUsersList().isEmpty()) {
        pps.setUserPrivileges(convertPrincipalPrivilegeSetEntries(proto.getUsersList()));
      }
      if (!proto.getRolesList().isEmpty()) {
        pps.setRolePrivileges(convertPrincipalPrivilegeSetEntries(proto.getRolesList()));
      }
    }
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
    return new ArrayList<>(proto.getRoleList());
  }

  /**
   * Serialize a database
   * @param db
   * @return two byte arrays, first contains the key, the second the serialized value.
   */
  static byte[][] serializeDatabase(Database db) {
    byte[][] result = new byte[2][];
    result[0] = buildKey(HiveStringUtils.normalizeIdentifier(db.getName()));
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
    List<FieldSchema> schemas = new ArrayList<>(protoList.size());
    for (HbaseMetastoreProto.FieldSchema proto : protoList) {
      schemas.add(new FieldSchema(proto.getName(), proto.getType(),
          proto.hasComment() ? proto.getComment() : null));
    }
    return schemas;
  }

  private static List<HbaseMetastoreProto.FieldSchema>
  convertFieldSchemaListToProto(List<FieldSchema> schemas) {
    List<HbaseMetastoreProto.FieldSchema> protoList = new ArrayList<>(schemas.size());
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
      List<HbaseMetastoreProto.StorageDescriptor.Order> protoList = new ArrayList<>(orders.size());
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
        SortedMap<String, String> params = new TreeMap<>(serde.getParameters());
        for (Map.Entry<String, String> param : params.entrySet()) {
          md.update(param.getKey().getBytes(ENCODING));
          md.update(param.getValue().getBytes(ENCODING));
        }
      }
    }
    if (sd.getBucketCols() != null) {
      SortedSet<String> bucketCols = new TreeSet<>(sd.getBucketCols());
      for (String bucket : bucketCols) md.update(bucket.getBytes(ENCODING));
    }
    if (sd.getSortCols() != null) {
      SortedSet<Order> orders = new TreeSet<>(sd.getSortCols());
      for (Order order : orders) {
        md.update(order.getCol().getBytes(ENCODING));
        md.update(Integer.toString(order.getOrder()).getBytes(ENCODING));
      }
    }
    if (sd.getSkewedInfo() != null) {
      SkewedInfo skewed = sd.getSkewedInfo();
      if (skewed.getSkewedColNames() != null) {
        SortedSet<String> colnames = new TreeSet<>(skewed.getSkewedColNames());
        for (String colname : colnames) md.update(colname.getBytes(ENCODING));
      }
      if (skewed.getSkewedColValues() != null) {
        SortedSet<String> sortedOuterList = new TreeSet<>();
        for (List<String> innerList : skewed.getSkewedColValues()) {
          SortedSet<String> sortedInnerList = new TreeSet<>(innerList);
          sortedOuterList.add(StringUtils.join(sortedInnerList, "."));
        }
        for (String colval : sortedOuterList) md.update(colval.getBytes(ENCODING));
      }
      if (skewed.getSkewedColValueLocationMaps() != null) {
        SortedMap<String, String> sortedMap = new TreeMap<>();
        for (Map.Entry<List<String>, String> smap : skewed.getSkewedColValueLocationMaps().entrySet()) {
          SortedSet<String> sortedKey = new TreeSet<>(smap.getKey());
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
      serde.setName(proto.getSerdeInfo().hasName()?
          proto.getSerdeInfo().getName():null);
      serde.setSerializationLib(proto.getSerdeInfo().hasSerializationLib()?
          proto.getSerdeInfo().getSerializationLib():null);
      serde.setParameters(buildParameters(proto.getSerdeInfo().getParameters()));
      sd.setSerdeInfo(serde);
    }
    sd.setBucketCols(new ArrayList<>(proto.getBucketColsList()));
    List<Order> sortCols = new ArrayList<>();
    for (HbaseMetastoreProto.StorageDescriptor.Order protoOrder : proto.getSortColsList()) {
      sortCols.add(new Order(protoOrder.getColumnName(), protoOrder.getOrder()));
    }
    sd.setSortCols(sortCols);
    if (proto.hasSkewedInfo()) {
      SkewedInfo skewed = new SkewedInfo();
      skewed
          .setSkewedColNames(new ArrayList<>(proto.getSkewedInfo().getSkewedColNamesList()));
      for (HbaseMetastoreProto.StorageDescriptor.SkewedInfo.SkewedColValueList innerList :
          proto.getSkewedInfo().getSkewedColValuesList()) {
        skewed.addToSkewedColValues(new ArrayList<>(innerList.getSkewedColValueList()));
      }
      Map<List<String>, String> colMaps = new HashMap<>();
      for (HbaseMetastoreProto.StorageDescriptor.SkewedInfo.SkewedColValueLocationMap map :
          proto.getSkewedInfo().getSkewedColValueLocationMapsList()) {
        colMaps.put(new ArrayList<>(map.getKeyList()), map.getValue());
      }
      skewed.setSkewedColValueLocationMaps(colMaps);
      sd.setSkewedInfo(skewed);
    }
    if (proto.hasStoredAsSubDirectories()) {
      sd.setStoredAsSubDirectories(proto.getStoredAsSubDirectories());
    }
    return sd;
  }

  static List<String> getPartitionKeyTypes(List<FieldSchema> parts) {
    com.google.common.base.Function<FieldSchema, String> fieldSchemaToType =
        new com.google.common.base.Function<FieldSchema, String>() {
      public String apply(FieldSchema fs) { return fs.getType(); }
    };
    return Lists.transform(parts, fieldSchemaToType);
  }

  static List<String> getPartitionNames(List<FieldSchema> parts) {
    com.google.common.base.Function<FieldSchema, String> fieldSchemaToName =
        new com.google.common.base.Function<FieldSchema, String>() {
      public String apply(FieldSchema fs) { return fs.getName(); }
    };
    return Lists.transform(parts, fieldSchemaToName);
  }

  /**
   * Serialize a partition
   * @param part partition object
   * @param sdHash hash that is being used as a key for the enclosed storage descriptor
   * @return First element is the key, second is the serialized partition
   */
  static byte[][] serializePartition(Partition part, List<String> partTypes, byte[] sdHash) {
    byte[][] result = new byte[2][];
    result[0] = buildPartitionKey(part.getDbName(), part.getTableName(), partTypes, part.getValues());
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

  static byte[] buildPartitionKey(String dbName, String tableName, List<String> partTypes, List<String> partVals) {
    return buildPartitionKey(dbName, tableName, partTypes, partVals, false);
  }

  static byte[] buildPartitionKey(String dbName, String tableName, List<String> partTypes, List<String> partVals, boolean endPrefix) {
    Object[] components = new Object[partVals.size()];
    for (int i=0;i<partVals.size();i++) {
      TypeInfo expectedType =
          TypeInfoUtils.getTypeInfoFromTypeString(partTypes.get(i));
      ObjectInspector outputOI =
          TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(expectedType);
      Converter converter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector, outputOI);
      components[i] = converter.convert(partVals.get(i));
    }

    return buildSerializedPartitionKey(dbName, tableName, partTypes, components, endPrefix);
  }

  static byte[] buildSerializedPartitionKey(String dbName, String tableName, List<String> partTypes, Object[] components, boolean endPrefix) {
    ObjectInspector javaStringOI =
        PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
    Object[] data = new Object[components.length+2];
    List<ObjectInspector> fois = new ArrayList<ObjectInspector>(components.length+2);
    boolean[] endPrefixes = new boolean[components.length+2];

    data[0] = dbName;
    fois.add(javaStringOI);
    endPrefixes[0] = false;
    data[1] = tableName;
    fois.add(javaStringOI);
    endPrefixes[1] = false;

    for (int i = 0; i < components.length; i++) {
      data[i+2] = components[i];
      TypeInfo expectedType =
          TypeInfoUtils.getTypeInfoFromTypeString(partTypes.get(i));
      ObjectInspector outputOI =
          TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(expectedType);
      fois.add(outputOI);
    }
    Output output = new Output();
    try {
      BinarySortableSerDeWithEndPrefix.serializeStruct(output, data, fois, endPrefix);
    } catch (SerDeException e) {
      throw new RuntimeException("Cannot serialize partition " + StringUtils.join(components, ","));
    }
    return Arrays.copyOf(output.getData(), output.getLength());
  }

  static class StorageDescriptorParts {
    byte[] sdHash;
    String location;
    Map<String, String> parameters;
    Partition containingPartition;
    Table containingTable;
    Index containingIndex;
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
    } else if (parts.containingIndex != null) {
      parts.containingIndex.setSd(ssd);
    }
    else {
      throw new RuntimeException("Need either a partition or a table");
    }
  }

  /**
   * Deserialize a partition key when you know nothing about it.  That is, you do not know what
   * dbname, tablename it came from.
   * @param key the key fetched from HBase
   * @param callback A reference to the calling HBaseReadWrite object.  This has to be done as a
   *                 callback because we have to first deserialize the database name and table
   *                 name, and then fetch the table information, and then we will know how to
   *                 desierliaze the rest of the key.
   * @return a list that includes the dbname, tablename, and partition values
   * @throws IOException
   */
  static List<String> deserializePartitionKey(byte[] key, HBaseReadWrite callback)
      throws IOException {
    List<String> keyParts =
        desierliazeDbNameTableNameFromPartitionKey(key, callback.getConf());
    Table table = callback.getTable(keyParts.get(0), keyParts.get(1));
    keyParts.addAll(deserializePartitionKey(table.getPartitionKeys(), key, callback.getConf()));
    return keyParts;
  }

   /**
   * Deserialize a partition.  This version should be used when the partition key is not already
   * known and the database and table name are not known either (eg a full scan).  Because the
    * dbname and tablename (and thus the partition columns) are not known a priori this version
    * has to go fetch the table after it figures out which table.  If you already have the table
    * object you should use
    * {@link #deserializePartition(String,String,List,byte[],byte[],Configuration)}
   * @param key the key fetched from HBase
   * @param serialized the value fetched from HBase
   * @param callback A reference to the calling HBaseReadWrite object.  This has to be done as a
   *                 callback because we have to first deserialize the database name and table
   *                 name, and then fetch the table information, and then we will know how to
   *                 desierliaze the rest of the key.
   * @return A struct that contains the partition plus parts of the storage descriptor
   */
  static StorageDescriptorParts deserializePartition(byte[] key, byte[] serialized,
                                                     HBaseReadWrite callback)
      throws IOException {
    List<String> dbNameTableName =
        desierliazeDbNameTableNameFromPartitionKey(key, callback.getConf());
    Table table = callback.getTable(dbNameTableName.get(0), dbNameTableName.get(1));
    List<String> keys = deserializePartitionKey(table.getPartitionKeys(), key, callback.getConf());
    return deserializePartition(dbNameTableName.get(0), dbNameTableName.get(1), keys, serialized);
  }

  /**
   * Deserialize a partition.  This version should be used when you know the dbname and tablename
   * but not the partition values.
   * @param dbName database this partition is in
   * @param tableName table this partition is in
   * @param partitions schemas for the partition columns of this table
   * @param key key fetched from HBase
   * @param serialized serialized version of the partition
   * @param conf configuration file
   * @return
   * @throws InvalidProtocolBufferException
   */
  static StorageDescriptorParts deserializePartition(String dbName, String tableName,
                                                     List<FieldSchema> partitions, byte[] key,
                                                     byte[] serialized, Configuration conf)
      throws InvalidProtocolBufferException {
    List<String> keys = deserializePartitionKey(partitions, key, conf);
    return deserializePartition(dbName, tableName, keys, serialized);
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

  static String[] deserializeKey(byte[] key) {
    String k = new String(key, ENCODING);
    return k.split(KEY_SEPARATOR_STR);
  }

  private static List<String> desierliazeDbNameTableNameFromPartitionKey(byte[] key,
                                                                         Configuration conf) {
    StringBuffer names = new StringBuffer();
    names.append("dbName,tableName,");
    StringBuffer types = new StringBuffer();
    types.append("string,string,");
    BinarySortableSerDe serDe = new BinarySortableSerDe();
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, names.toString());
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, types.toString());
    try {
      serDe.initialize(conf, props);
      List deserializedkeys = ((List)serDe.deserialize(new BytesWritable(key))).subList(0, 2);
      List<String> keys = new ArrayList<>();
      for (int i=0;i<deserializedkeys.size();i++) {
        Object deserializedKey = deserializedkeys.get(i);
        if (deserializedKey==null) {
          throw new RuntimeException("Can't have a null dbname or tablename");
        } else {
          TypeInfo inputType = TypeInfoUtils.getTypeInfoFromTypeString("string");
          ObjectInspector inputOI =
              TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(inputType);
          Converter converter = ObjectInspectorConverters.getConverter(inputOI,
              PrimitiveObjectInspectorFactory.javaStringObjectInspector);
          keys.add((String) converter.convert(deserializedKey));
        }
      }
      return keys;
    } catch (SerDeException e) {
      throw new RuntimeException("Error when deserialize key", e);
    }
  }

  // Deserialize a partition key and return _only_ the partition values.
  private static List<String> deserializePartitionKey(List<FieldSchema> partitions, byte[] key,
      Configuration conf) {
    StringBuffer names = new StringBuffer();
    names.append("dbName,tableName,");
    StringBuffer types = new StringBuffer();
    types.append("string,string,");
    for (int i=0;i<partitions.size();i++) {
      names.append(partitions.get(i).getName());
      types.append(TypeInfoUtils.getTypeInfoFromTypeString(partitions.get(i).getType()));
      if (i!=partitions.size()-1) {
        names.append(",");
        types.append(",");
      }
    }
    BinarySortableSerDe serDe = new BinarySortableSerDe();
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, names.toString());
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, types.toString());
    try {
      serDe.initialize(conf, props);
      List deserializedkeys = ((List)serDe.deserialize(new BytesWritable(key))).subList(2, partitions.size()+2);
      List<String> partitionKeys = new ArrayList<String>();
      for (int i=0;i<deserializedkeys.size();i++) {
        Object deserializedKey = deserializedkeys.get(i);
        if (deserializedKey==null) {
          partitionKeys.add(HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULTPARTITIONNAME));
        } else {
          TypeInfo inputType =
              TypeInfoUtils.getTypeInfoFromTypeString(partitions.get(i).getType());
          ObjectInspector inputOI =
              TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(inputType);
          Converter converter = ObjectInspectorConverters.getConverter(inputOI,
              PrimitiveObjectInspectorFactory.javaStringObjectInspector);
          partitionKeys.add((String)converter.convert(deserializedKey));
        }
      }
      return partitionKeys;
    } catch (SerDeException e) {
      throw new RuntimeException("Error when deserialize key", e);
    }
  }

  /**
   * Serialize a table
   * @param table table object
   * @param sdHash hash that is being used as a key for the enclosed storage descriptor
   * @return First element is the key, second is the serialized table
   */
  static byte[][] serializeTable(Table table, byte[] sdHash) {
    byte[][] result = new byte[2][];
    result[0] = buildKey(HiveStringUtils.normalizeIdentifier(table.getDbName()),
        HiveStringUtils.normalizeIdentifier(table.getTableName()));
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
    builder.setIsRewriteEnabled(table.isRewriteEnabled());
    if (table.getTableType() != null) builder.setTableType(table.getTableType());
    if (table.getPrivileges() != null) {
      builder.setPrivileges(buildPrincipalPrivilegeSet(table.getPrivileges()));
    }
    // Set only if table is temporary
    if (table.isTemporary()) {
      builder.setIsTemporary(table.isTemporary());
    }
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
    table.setRewriteEnabled(proto.getIsRewriteEnabled());
    table.setTableType(proto.getTableType());
    if (proto.hasPrivileges()) {
      table.setPrivileges(buildPrincipalPrivilegeSet(proto.getPrivileges()));
    }
    if (proto.hasIsTemporary()) table.setTemporary(proto.getIsTemporary());
    return sdParts;
  }

  /**
   * Serialize an index
   * @param index index object
   * @param sdHash hash that is being used as a key for the enclosed storage descriptor
   * @return First element is the key, second is the serialized index
   */
  static byte[][] serializeIndex(Index index, byte[] sdHash) {
    byte[][] result = new byte[2][];
    result[0] = buildKey(HiveStringUtils.normalizeIdentifier(index.getDbName()),
        HiveStringUtils.normalizeIdentifier(index.getOrigTableName()),
        HiveStringUtils.normalizeIdentifier(index.getIndexName()));
    HbaseMetastoreProto.Index.Builder builder = HbaseMetastoreProto.Index.newBuilder();
    builder.setDbName(index.getDbName());
    builder.setOrigTableName(index.getOrigTableName());
    if (index.getSd().getLocation() != null) builder.setLocation(index.getSd().getLocation());
    if (index.getSd().getParameters() != null) {
      builder.setSdParameters(buildParameters(index.getSd().getParameters()));
    }
    if (index.getIndexHandlerClass() != null) {
      builder.setIndexHandlerClass(index.getIndexHandlerClass());
    }
    if (index.getIndexTableName() != null) {
      builder.setIndexTableName(index.getIndexTableName());
    }
    builder
        .setCreateTime(index.getCreateTime())
        .setLastAccessTime(index.getLastAccessTime())
        .setDeferredRebuild(index.isDeferredRebuild());
    if (index.getParameters() != null) {
      builder.setParameters(buildParameters(index.getParameters()));
    }
    if (sdHash != null) {
      builder.setSdHash(ByteString.copyFrom(sdHash));
    }
    result[1] = builder.build().toByteArray();
    return result;
  }

  /**
   * Deserialize an index.  This version should be used when the index key is not already
   * known (eg a scan).
   * @param key the key fetched from HBase
   * @param serialized the value fetched from HBase
   * @return A struct that contains the index plus parts of the storage descriptor
   */
  static StorageDescriptorParts deserializeIndex(byte[] key, byte[] serialized)
      throws InvalidProtocolBufferException {
    String[] keys = deserializeKey(key);
    return deserializeIndex(keys[0], keys[1], keys[2], serialized);
  }

  /**
   * Deserialize an index.  This version should be used when the table key is
   * known (eg a get).
   * @param dbName database name
   * @param origTableName original table name
   * @param indexName index name
   * @param serialized the value fetched from HBase
   * @return A struct that contains the index plus parts of the storage descriptor
   */
  static StorageDescriptorParts deserializeIndex(String dbName, String origTableName,
                                                 String indexName, byte[] serialized)
      throws InvalidProtocolBufferException {
    HbaseMetastoreProto.Index proto = HbaseMetastoreProto.Index.parseFrom(serialized);
    Index index = new Index();
    StorageDescriptorParts sdParts = new StorageDescriptorParts();
    sdParts.containingIndex = index;
    index.setDbName(dbName);
    index.setIndexName(indexName);
    index.setOrigTableName(origTableName);
    if (proto.hasLocation()) sdParts.location = proto.getLocation();
    if (proto.hasSdParameters()) sdParts.parameters = buildParameters(proto.getSdParameters());
    if (proto.hasIndexHandlerClass()) {
      index.setIndexHandlerClass(proto.getIndexHandlerClass());
    }
    if (proto.hasIndexTableName()) {
      index.setIndexTableName(proto.getIndexTableName());
    }
    index.setCreateTime(proto.getCreateTime());
    index.setLastAccessTime(proto.getLastAccessTime());
    index.setDeferredRebuild(proto.getDeferredRebuild());
    index.setParameters(buildParameters(proto.getParameters()));
    if (proto.hasSdHash()) {
      sdParts.sdHash = proto.getSdHash().toByteArray();
    }
    return sdParts;
  }

  static byte[] serializeBloomFilter(String dbName, String tableName, BloomFilter bloom) {
    long[] bitSet = bloom.getBitSet();
    List<Long> bits = new ArrayList<>(bitSet.length);
    for (int i = 0; i < bitSet.length; i++) bits.add(bitSet[i]);
    HbaseMetastoreProto.AggrStatsBloomFilter.BloomFilter protoBloom =
        HbaseMetastoreProto.AggrStatsBloomFilter.BloomFilter.newBuilder()
        .setNumBits(bloom.getBitSize())
        .setNumFuncs(bloom.getNumHashFunctions())
        .addAllBits(bits)
        .build();

    HbaseMetastoreProto.AggrStatsBloomFilter proto =
        HbaseMetastoreProto.AggrStatsBloomFilter.newBuilder()
            .setDbName(ByteString.copyFrom(dbName.getBytes(ENCODING)))
            .setTableName(ByteString.copyFrom(tableName.getBytes(ENCODING)))
            .setBloomFilter(protoBloom)
            .setAggregatedAt(System.currentTimeMillis())
            .build();

    return proto.toByteArray();
  }

  private static HbaseMetastoreProto.ColumnStats protoBufStatsForOneColumn(
      ColumnStatistics partitionColumnStats, ColumnStatisticsObj colStats) throws IOException {
    HbaseMetastoreProto.ColumnStats.Builder builder = HbaseMetastoreProto.ColumnStats.newBuilder();
    if (partitionColumnStats != null) {
      builder.setLastAnalyzed(partitionColumnStats.getStatsDesc().getLastAnalyzed());
    }
    assert colStats.getColType() != null;
    builder.setColumnType(colStats.getColType());
    assert colStats.getColName() != null;
    builder.setColumnName(colStats.getColName());

    ColumnStatisticsData colData = colStats.getStatsData();
    switch (colData.getSetField()) {
    case BOOLEAN_STATS:
      BooleanColumnStatsData boolData = colData.getBooleanStats();
      builder.setNumNulls(boolData.getNumNulls());
      builder.setBoolStats(HbaseMetastoreProto.ColumnStats.BooleanStats.newBuilder()
          .setNumTrues(boolData.getNumTrues()).setNumFalses(boolData.getNumFalses()).build());
      break;

    case LONG_STATS:
      LongColumnStatsData longData = colData.getLongStats();
      builder.setNumNulls(longData.getNumNulls());
      builder.setNumDistinctValues(longData.getNumDVs());
      if (longData.isSetBitVectors()) {
        builder.setBitVectors(longData.getBitVectors());
      }
      builder.setLongStats(HbaseMetastoreProto.ColumnStats.LongStats.newBuilder()
          .setLowValue(longData.getLowValue()).setHighValue(longData.getHighValue()).build());
      break;

    case DOUBLE_STATS:
      DoubleColumnStatsData doubleData = colData.getDoubleStats();
      builder.setNumNulls(doubleData.getNumNulls());
      builder.setNumDistinctValues(doubleData.getNumDVs());
      if (doubleData.isSetBitVectors()) {
        builder.setBitVectors(doubleData.getBitVectors());
      }
      builder.setDoubleStats(HbaseMetastoreProto.ColumnStats.DoubleStats.newBuilder()
          .setLowValue(doubleData.getLowValue()).setHighValue(doubleData.getHighValue()).build());
      break;

    case STRING_STATS:
      StringColumnStatsData stringData = colData.getStringStats();
      builder.setNumNulls(stringData.getNumNulls());
      builder.setNumDistinctValues(stringData.getNumDVs());
      if (stringData.isSetBitVectors()) {
        builder.setBitVectors(stringData.getBitVectors());
      }
      builder.setStringStats(HbaseMetastoreProto.ColumnStats.StringStats.newBuilder()
          .setMaxColLength(stringData.getMaxColLen()).setAvgColLength(stringData.getAvgColLen())
          .build());
      break;

    case BINARY_STATS:
      BinaryColumnStatsData binaryData = colData.getBinaryStats();
      builder.setNumNulls(binaryData.getNumNulls());
      builder.setBinaryStats(HbaseMetastoreProto.ColumnStats.StringStats.newBuilder()
          .setMaxColLength(binaryData.getMaxColLen()).setAvgColLength(binaryData.getAvgColLen())
          .build());
      break;

    case DECIMAL_STATS:
      DecimalColumnStatsData decimalData = colData.getDecimalStats();
      builder.setNumNulls(decimalData.getNumNulls());
      builder.setNumDistinctValues(decimalData.getNumDVs());
      if (decimalData.isSetBitVectors()) {
        builder.setBitVectors(decimalData.getBitVectors());
      }
      if (decimalData.getLowValue() != null && decimalData.getHighValue() != null) {
        builder.setDecimalStats(
            HbaseMetastoreProto.ColumnStats.DecimalStats
                .newBuilder()
                .setLowValue(
                    HbaseMetastoreProto.ColumnStats.DecimalStats.Decimal.newBuilder()
                        .setUnscaled(ByteString.copyFrom(decimalData.getLowValue().getUnscaled()))
                        .setScale(decimalData.getLowValue().getScale()).build())
                .setHighValue(
                    HbaseMetastoreProto.ColumnStats.DecimalStats.Decimal.newBuilder()
                        .setUnscaled(ByteString.copyFrom(decimalData.getHighValue().getUnscaled()))
                        .setScale(decimalData.getHighValue().getScale()).build())).build();
      } else {
        builder.setDecimalStats(HbaseMetastoreProto.ColumnStats.DecimalStats.newBuilder().clear()
            .build());
      }
      break;

    default:
      throw new RuntimeException("Woh, bad.  Unknown stats type!");
    }
    return builder.build();
  }

  static byte[] serializeStatsForOneColumn(ColumnStatistics partitionColumnStats,
                                           ColumnStatisticsObj colStats) throws IOException {
    return protoBufStatsForOneColumn(partitionColumnStats, colStats).toByteArray();
  }

  static ColumnStatisticsObj deserializeStatsForOneColumn(ColumnStatistics partitionColumnStats,
      byte[] bytes) throws IOException {
    HbaseMetastoreProto.ColumnStats proto = HbaseMetastoreProto.ColumnStats.parseFrom(bytes);
    return statsForOneColumnFromProtoBuf(partitionColumnStats, proto);
  }

  private static ColumnStatisticsObj
  statsForOneColumnFromProtoBuf(ColumnStatistics partitionColumnStats,
                                HbaseMetastoreProto.ColumnStats proto) throws IOException {
    ColumnStatisticsObj colStats = new ColumnStatisticsObj();
    long lastAnalyzed = proto.getLastAnalyzed();
    if (partitionColumnStats != null) {
      partitionColumnStats.getStatsDesc().setLastAnalyzed(
          Math.max(lastAnalyzed, partitionColumnStats.getStatsDesc().getLastAnalyzed()));
    }
    colStats.setColType(proto.getColumnType());
    colStats.setColName(proto.getColumnName());

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
      longData.setBitVectors(proto.getBitVectors());
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
      doubleData.setBitVectors(proto.getBitVectors());
      colData.setDoubleStats(doubleData);
    } else if (proto.hasStringStats()) {
      StringColumnStatsData stringData = new StringColumnStatsData();
      stringData.setMaxColLen(proto.getStringStats().getMaxColLength());
      stringData.setAvgColLen(proto.getStringStats().getAvgColLength());
      stringData.setNumNulls(proto.getNumNulls());
      stringData.setNumDVs(proto.getNumDistinctValues());
      stringData.setBitVectors(proto.getBitVectors());
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
      decimalData.setBitVectors(proto.getBitVectors());
      colData.setDecimalStats(decimalData);
    } else {
      throw new RuntimeException("Woh, bad.  Unknown stats type!");
    }
    colStats.setStatsData(colData);
    return colStats;
  }

  static byte[] serializeAggrStats(AggrStats aggrStats) throws IOException {
    List<HbaseMetastoreProto.ColumnStats> protoColStats =
        new ArrayList<>(aggrStats.getColStatsSize());
    for (ColumnStatisticsObj cso : aggrStats.getColStats()) {
      protoColStats.add(protoBufStatsForOneColumn(null, cso));
    }
    return HbaseMetastoreProto.AggrStats.newBuilder()
        .setPartsFound(aggrStats.getPartsFound())
        .addAllColStats(protoColStats)
        .build()
        .toByteArray();
  }

  static AggrStats deserializeAggrStats(byte[] serialized) throws IOException {
    HbaseMetastoreProto.AggrStats protoAggrStats =
        HbaseMetastoreProto.AggrStats.parseFrom(serialized);
    AggrStats aggrStats = new AggrStats();
    aggrStats.setPartsFound(protoAggrStats.getPartsFound());
    for (HbaseMetastoreProto.ColumnStats protoCS : protoAggrStats.getColStatsList()) {
      aggrStats.addToColStats(statsForOneColumnFromProtoBuf(null, protoCS));
    }
    return aggrStats;
  }

  /**
   * Serialize a delegation token
   * @param tokenIdentifier
   * @param delegationToken
   * @return two byte arrays, first contains the key, the second the serialized value.
   */
  static byte[][] serializeDelegationToken(String tokenIdentifier, String delegationToken) {
    byte[][] result = new byte[2][];
    result[0] = buildKey(tokenIdentifier);
    result[1] = HbaseMetastoreProto.DelegationToken.newBuilder()
        .setTokenStr(delegationToken)
        .build()
        .toByteArray();
    return result;
  }

  /**
   * Deserialize a delegation token.
   * @param value value fetched from hbase
   * @return A delegation token.
   * @throws InvalidProtocolBufferException
   */
  static String deserializeDelegationToken(byte[] value) throws InvalidProtocolBufferException {
    HbaseMetastoreProto.DelegationToken protoToken =
        HbaseMetastoreProto.DelegationToken.parseFrom(value);
    return protoToken.getTokenStr();
  }

  /**
   * Serialize a master key
   * @param seqNo
   * @param key
   * @return two byte arrays, first contains the key, the second the serialized value.
   */
  static byte[][] serializeMasterKey(Integer seqNo, String key) {
    byte[][] result = new byte[2][];
    result[0] = buildKey(seqNo.toString());
    result[1] = HbaseMetastoreProto.MasterKey.newBuilder()
        .setMasterKey(key)
        .build()
        .toByteArray();
    return result;
  }

  /**
   * Deserialize a master key.
   * @param value value fetched from hbase
   * @return A master key
   * @throws InvalidProtocolBufferException
   */
  static String deserializeMasterKey(byte[] value) throws InvalidProtocolBufferException {
    HbaseMetastoreProto.MasterKey protoKey = HbaseMetastoreProto.MasterKey.parseFrom(value);
    return protoKey.getMasterKey();
  }

  /**
   * Serialize the primary key for a table.
   * @param pk Primary key columns.  It is expected that all of these match to one pk, since
   *           anything else is meaningless.
   * @return two byte arrays, first containts the hbase key, the second the serialized value.
   */
  static byte[][] serializePrimaryKey(List<SQLPrimaryKey> pk) {
    // First, figure out the dbName and tableName.  We expect this to match for all list entries.
    byte[][] result = new byte[2][];
    String dbName = pk.get(0).getTable_db();
    String tableName = pk.get(0).getTable_name();
    result[0] = buildKey(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tableName));

    HbaseMetastoreProto.PrimaryKey.Builder builder = HbaseMetastoreProto.PrimaryKey.newBuilder();
    // Encode the primary key, if present
    builder.setPkName(pk.get(0).getPk_name());
    builder.setEnableConstraint(pk.get(0).isEnable_cstr());
    builder.setValidateConstraint(pk.get(0).isValidate_cstr());
    builder.setRelyConstraint(pk.get(0).isRely_cstr());

    for (SQLPrimaryKey pkcol : pk) {
      HbaseMetastoreProto.PrimaryKey.PrimaryKeyColumn.Builder pkColBuilder =
          HbaseMetastoreProto.PrimaryKey.PrimaryKeyColumn.newBuilder();
      pkColBuilder.setColumnName(pkcol.getColumn_name());
      pkColBuilder.setKeySeq(pkcol.getKey_seq());
      builder.addCols(pkColBuilder);
    }

    result[1] = builder.build().toByteArray();
    return result;
  }

  /**
   * Serialize the foreign key(s) for a table.
   * @param fks Foreign key columns.  These may belong to multiple foreign keys.
   * @return two byte arrays, first containts the key, the second the serialized value.
   */
  static byte[][] serializeForeignKeys(List<SQLForeignKey> fks) {
    // First, figure out the dbName and tableName.  We expect this to match for all list entries.
    byte[][] result = new byte[2][];
    String dbName = fks.get(0).getFktable_db();
    String tableName = fks.get(0).getFktable_name();
    result[0] = buildKey(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tableName));

    HbaseMetastoreProto.ForeignKeys.Builder builder = HbaseMetastoreProto.ForeignKeys.newBuilder();

    // Encode any foreign keys we find.  This can be complex because there may be more than
    // one foreign key in here, so we need to detect that.
    Map<String, HbaseMetastoreProto.ForeignKeys.ForeignKey.Builder> fkBuilders = new HashMap<>();

    for (SQLForeignKey fkcol : fks) {
      HbaseMetastoreProto.ForeignKeys.ForeignKey.Builder fkBuilder =
          fkBuilders.get(fkcol.getFk_name());
      if (fkBuilder == null) {
        // We haven't seen this key before, so add it
        fkBuilder = HbaseMetastoreProto.ForeignKeys.ForeignKey.newBuilder();
        fkBuilder.setFkName(fkcol.getFk_name());
        fkBuilder.setReferencedDbName(fkcol.getPktable_db());
        assert dbName.equals(fkcol.getFktable_db()) : "You switched databases on me!";
        fkBuilder.setReferencedTableName(fkcol.getPktable_name());
        assert tableName.equals(fkcol.getFktable_name()) : "You switched tables on me!";
        fkBuilder.setReferencedPkName(fkcol.getPk_name());
        fkBuilder.setUpdateRule(fkcol.getUpdate_rule());
        fkBuilder.setDeleteRule(fkcol.getDelete_rule());
        fkBuilder.setEnableConstraint(fkcol.isEnable_cstr());
        fkBuilder.setValidateConstraint(fkcol.isValidate_cstr());
        fkBuilder.setRelyConstraint(fkcol.isRely_cstr());
        fkBuilders.put(fkcol.getFk_name(), fkBuilder);
      }
      HbaseMetastoreProto.ForeignKeys.ForeignKey.ForeignKeyColumn.Builder fkColBuilder =
          HbaseMetastoreProto.ForeignKeys.ForeignKey.ForeignKeyColumn.newBuilder();
      fkColBuilder.setColumnName(fkcol.getFkcolumn_name());
      fkColBuilder.setReferencedColumnName(fkcol.getPkcolumn_name());
      fkColBuilder.setKeySeq(fkcol.getKey_seq());
      fkBuilder.addCols(fkColBuilder);
    }
    for (HbaseMetastoreProto.ForeignKeys.ForeignKey.Builder fkBuilder : fkBuilders.values()) {
      builder.addFks(fkBuilder);
    }
    result[1] = builder.build().toByteArray();
    return result;
  }

  static List<SQLPrimaryKey> deserializePrimaryKey(String dbName, String tableName, byte[] value)
      throws InvalidProtocolBufferException {
    HbaseMetastoreProto.PrimaryKey proto = HbaseMetastoreProto.PrimaryKey.parseFrom(value);
    List<SQLPrimaryKey> result = new ArrayList<>();
    for (HbaseMetastoreProto.PrimaryKey.PrimaryKeyColumn protoPkCol : proto.getColsList()) {
      result.add(new SQLPrimaryKey(dbName, tableName, protoPkCol.getColumnName(),
          protoPkCol.getKeySeq(), proto.getPkName(), proto.getEnableConstraint(),
          proto.getValidateConstraint(), proto.getRelyConstraint()));
    }

    return result;
  }

  static List<SQLForeignKey> deserializeForeignKeys(String dbName, String tableName, byte[] value)
      throws InvalidProtocolBufferException {
    List<SQLForeignKey> result = new ArrayList<>();
    HbaseMetastoreProto.ForeignKeys protoConstraints =
        HbaseMetastoreProto.ForeignKeys.parseFrom(value);

    for (HbaseMetastoreProto.ForeignKeys.ForeignKey protoFk : protoConstraints.getFksList()) {
      for (HbaseMetastoreProto.ForeignKeys.ForeignKey.ForeignKeyColumn protoFkCol :
          protoFk.getColsList()) {
        result.add(new SQLForeignKey(protoFk.getReferencedDbName(), protoFk.getReferencedTableName(),
            protoFkCol.getReferencedColumnName(), dbName, tableName, protoFkCol.getColumnName(),
            protoFkCol.getKeySeq(), protoFk.getUpdateRule(), protoFk.getDeleteRule(),
            protoFk.getFkName(), protoFk.getReferencedPkName(), protoFk.getEnableConstraint(),
            protoFk.getValidateConstraint(), protoFk.getRelyConstraint()));
      }
    }
    return result;
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

  static byte[] makeLongKey(long v) {
    byte[] b = new byte[8];
    b[0] = (byte)(v >>> 56);
    b[1] = (byte)(v >>> 48);
    b[2] = (byte)(v >>> 40);
    b[3] = (byte)(v >>> 32);
    b[4] = (byte)(v >>> 24);
    b[5] = (byte)(v >>> 16);
    b[6] = (byte)(v >>>  8);
    b[7] = (byte)(v >>>  0);
    return b;
  }

  public static double getDoubleValue(Decimal decimal) {
    return new BigDecimal(new BigInteger(decimal.getUnscaled()), decimal.getScale()).doubleValue();
  }
}
