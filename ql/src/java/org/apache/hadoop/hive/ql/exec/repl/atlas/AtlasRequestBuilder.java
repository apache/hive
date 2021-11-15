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

package org.apache.hadoop.hive.ql.exec.repl.atlas;

import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AttributeTransform;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.hive.ql.exec.util.Retryable;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Helper class to create export/import request.
 */
public class AtlasRequestBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(AtlasRequestBuilder.class);
  public static final String ATTRIBUTE_QUALIFIED_NAME = "qualifiedName";
  static final String ATLAS_TYPE_HIVE_DB = "hive_db";
  public static final String ATLAS_TYPE_HIVE_TABLE = "hive_table";
  static final String ATLAS_TYPE_HIVE_SD = "hive_storagedesc";
  static final String QUALIFIED_NAME_FORMAT = "%s@%s";
  static final String QUALIFIED_NAME_HIVE_TABLE_FORMAT = "%s.%s";

  private static final String ATTRIBUTE_NAME_CLUSTER_NAME = ".clusterName";
  private static final String ATTRIBUTE_NAME_NAME = ".name";
  private static final String ATTRIBUTE_NAME_REPLICATED_TO = "replicatedTo";
  private static final String ATTRIBUTE_NAME_REPLICATED_FROM = "replicatedFrom";
  private static final String ATTRIBUTE_NAME_LOCATION = ".location";

  private static final String HIVE_DB_CLUSTER_NAME = ATLAS_TYPE_HIVE_DB + ATTRIBUTE_NAME_CLUSTER_NAME;
  private static final String HIVE_DB_NAME = ATLAS_TYPE_HIVE_DB + ATTRIBUTE_NAME_NAME;
  private static final String HIVE_DB_LOCATION = ATLAS_TYPE_HIVE_DB + ATTRIBUTE_NAME_LOCATION;
  private static final String HIVE_SD_LOCATION = ATLAS_TYPE_HIVE_SD + ATTRIBUTE_NAME_LOCATION;

  private static final String TRANSFORM_ENTITY_SCOPE = "__entity";
  private static final String REPLICATED_TAG_NAME = "%s_replicated";

  public AtlasExportRequest createExportRequest(AtlasReplInfo atlasReplInfo)
          throws SemanticException {
    List<AtlasObjectId> itemsToExport = getItemsToExport(atlasReplInfo);
    Map<String, Object> options = getOptions(atlasReplInfo);
    return createRequest(itemsToExport, options);
  }

  private List<AtlasObjectId> getItemsToExport(AtlasReplInfo atlasReplInfo) throws SemanticException {
    List<AtlasObjectId> atlasObjectIds = new ArrayList<>();
    if (atlasReplInfo.isTableLevelRepl()) {
      final List<String> tableQualifiedNames = getQualifiedNames(atlasReplInfo.getSrcCluster(), atlasReplInfo.getSrcDB(),
              atlasReplInfo.getTableListFile(), atlasReplInfo.getConf());
      for (String tableQualifiedName : tableQualifiedNames) {
        atlasObjectIds.add(new AtlasObjectId(ATLAS_TYPE_HIVE_TABLE, ATTRIBUTE_QUALIFIED_NAME, tableQualifiedName));
      }
    } else {
      final String qualifiedName = getQualifiedName(atlasReplInfo.getSrcCluster(), atlasReplInfo.getSrcDB());
      atlasObjectIds.add(new AtlasObjectId(ATLAS_TYPE_HIVE_DB, ATTRIBUTE_QUALIFIED_NAME, qualifiedName));
    }
    return atlasObjectIds;
  }

  private AtlasExportRequest createRequest(final List<AtlasObjectId> itemsToExport,
                                           final Map<String, Object> options) {
    AtlasExportRequest request = new AtlasExportRequest() {
      {
        setItemsToExport(itemsToExport);
        setOptions(options);
      }
    };
    LOG.debug("createRequest: {}" + request);
    return request;
  }

  private Map<String, Object> getOptions(AtlasReplInfo atlasReplInfo) {
    String targetCluster = atlasReplInfo.getTgtCluster();
    Map<String, Object> options = new HashMap<>();
    options.put(AtlasExportRequest.OPTION_FETCH_TYPE, AtlasExportRequest.FETCH_TYPE_INCREMENTAL);
    options.put(AtlasExportRequest.FETCH_TYPE_INCREMENTAL_CHANGE_MARKER, atlasReplInfo.getTimeStamp());
    options.put(AtlasExportRequest.OPTION_SKIP_LINEAGE, true);
    if (targetCluster != null && !targetCluster.isEmpty()) {
      options.put(AtlasExportRequest.OPTION_KEY_REPLICATED_TO, targetCluster);
    }
    return options;
  }

  public AtlasObjectId getItemToExport(String srcCluster, String srcDB) {
    final String qualifiedName = getQualifiedName(srcCluster, srcDB);
    return new AtlasObjectId(ATLAS_TYPE_HIVE_DB, ATTRIBUTE_QUALIFIED_NAME, qualifiedName);
  }

  private String getQualifiedName(String clusterName, String srcDb) {
    String qualifiedName = String.format(QUALIFIED_NAME_FORMAT, srcDb.toLowerCase(), clusterName);
    LOG.debug("Atlas getQualifiedName: {}", qualifiedName);
    return qualifiedName;
  }

  private String getQualifiedName(String clusterName, String srcDB, String tableName) {
    String qualifiedTableName = String.format(QUALIFIED_NAME_HIVE_TABLE_FORMAT, srcDB, tableName);
    return getQualifiedName(clusterName,  qualifiedTableName);
  }

  private List<String> getQualifiedNames(String clusterName, String srcDb, Path listOfTablesFile, HiveConf conf)
          throws SemanticException {
    List<String> qualifiedNames = new ArrayList<>();
    List<String> tableNames = getFileAsList(listOfTablesFile, conf);
    if (CollectionUtils.isEmpty(tableNames)) {
      LOG.info("Empty file encountered: {}", listOfTablesFile);
      return qualifiedNames;
    }
    for (String tableName : tableNames) {
      qualifiedNames.add(getQualifiedName(clusterName, srcDb, tableName));
    }
    return qualifiedNames;
  }

  public List<String> getFileAsList(Path listOfTablesFile, HiveConf conf) throws SemanticException {
    Retryable retryable = Retryable.builder()
            .withHiveConf(conf)
            .withRetryOnException(IOException.class).build();
    try {
      return retryable.executeCallable(() -> {
        List<String> list = new ArrayList<>();
        InputStream is = null;
        try {
          FileSystem fs = getFileSystem(listOfTablesFile, conf);
          FileStatus fileStatus = fs.getFileStatus(listOfTablesFile);
          if (fileStatus == null) {
            throw new SemanticException("Table list file not found: " + listOfTablesFile);
          }
          is = fs.open(listOfTablesFile);
          list.addAll(IOUtils.readLines(is, Charset.defaultCharset()));
          return list;
        } finally {
          org.apache.hadoop.io.IOUtils.closeStream(is);
        }
      });
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  public FileSystem getFileSystem(Path listOfTablesFile, HiveConf conf) throws IOException {
    return FileSystem.get(listOfTablesFile.toUri(), conf);
  }

  public AtlasImportRequest createImportRequest(String sourceDataSet, String targetDataSet,
                                                String sourceClusterName, String targetClusterName,
                                                String sourceFsEndpoint, String targetFsEndpoint) {
    AtlasImportRequest request = new AtlasImportRequest();
    addTransforms(request.getOptions(),
            sourceClusterName, targetClusterName,
            sourceDataSet, targetDataSet,
            sourceFsEndpoint, targetFsEndpoint);
    addReplicatedFrom(request.getOptions(), sourceClusterName);
    LOG.debug("Atlas metadata import request: {}" + request);
    return request;
  }

  private void addTransforms(Map<String, String> options, String srcClusterName,
                             String tgtClusterName, String sourceDataSet, String targetDataSet,
                             String sourcefsEndpoint, String targetFsEndpoint) {
    List<AttributeTransform> transforms = new ArrayList<>();
    String sanitizedSourceClusterName = sanitizeForClassificationName(srcClusterName);
    addClassificationTransform(transforms,
            String.format(REPLICATED_TAG_NAME, sanitizedSourceClusterName));
    addClearReplicationAttributesTransform(transforms);
    addClusterRenameTransform(transforms, srcClusterName, tgtClusterName);
    if (!sourceDataSet.equals(targetDataSet)) {
      addDataSetRenameTransform(transforms, sourceDataSet, targetDataSet);
    }
    addLocationTransform(transforms, sourcefsEndpoint, targetFsEndpoint);
    options.put(AtlasImportRequest.TRANSFORMERS_KEY, AtlasType.toJson(transforms));
  }

  private void addLocationTransform(List<AttributeTransform> transforms, String srcFsUri, String tgtFsUri) {
    transforms.add(create(
            HIVE_DB_LOCATION, "STARTS_WITH_IGNORE_CASE: " + srcFsUri,
            HIVE_DB_LOCATION, "REPLACE_PREFIX: = :" + srcFsUri + "=" + tgtFsUri
            )
    );
    transforms.add(create(
            HIVE_SD_LOCATION, "STARTS_WITH_IGNORE_CASE: " + srcFsUri,
            HIVE_SD_LOCATION, "REPLACE_PREFIX: = :" + srcFsUri + "=" + tgtFsUri
            )
    );
  }

  private void addDataSetRenameTransform(List<AttributeTransform> transforms,
                                         String sourceDataSet, String targetDataSet) {
    transforms.add(create(
            HIVE_DB_NAME, "EQUALS: " + sourceDataSet,
            HIVE_DB_NAME, "SET: " + targetDataSet));
  }

  private void addClusterRenameTransform(List<AttributeTransform> transforms,
                                         String srcClusterName, String tgtClustername) {
    transforms.add(create(HIVE_DB_CLUSTER_NAME, "EQUALS: " + srcClusterName,
            HIVE_DB_CLUSTER_NAME, "SET: " + tgtClustername));
  }

  private void addReplicatedFrom(Map<String, String> options, String sourceClusterName) {
    options.put(AtlasImportRequest.OPTION_KEY_REPLICATED_FROM, sourceClusterName);
  }

  private void addClassificationTransform(List<AttributeTransform> transforms, String classificationName) {
    transforms.add(create("__entity", "topLevel: ",
            "__entity", "ADD_CLASSIFICATION: " + classificationName));
  }

  private String sanitizeForClassificationName(String s) {
    if (StringUtils.isEmpty(s)) {
      return s;
    }
    return s.replace('-', '_').replace(' ', '_');
  }

  private void addClearReplicationAttributesTransform(List<AttributeTransform> transforms) {
    Map<String, String> actions = new HashMap<>();
    actions.put(TRANSFORM_ENTITY_SCOPE + "." + ATTRIBUTE_NAME_REPLICATED_TO, "CLEAR:");
    actions.put(TRANSFORM_ENTITY_SCOPE + "." + ATTRIBUTE_NAME_REPLICATED_FROM, "CLEAR:");

    transforms.add(new AttributeTransform(null, actions));
  }

  private AttributeTransform create(String conditionLhs, String conditionRhs,
                                    String actionLhs, String actionRhs) {
    return new AttributeTransform(Collections.singletonMap(conditionLhs, conditionRhs),
            Collections.singletonMap(actionLhs, actionRhs));
  }
}
