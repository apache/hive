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
package org.apache.hadoop.hive.registry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DefaultPartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaBranch;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.EventCleanerTask;
import org.apache.hadoop.hive.registry.cache.SchemaBranchCache;
import org.apache.hadoop.hive.registry.common.QueryParam;
import org.apache.hadoop.hive.registry.common.errors.IncompatibleSchemaException;
import org.apache.hadoop.hive.registry.common.errors.InvalidSchemaBranchDeletionException;
import org.apache.hadoop.hive.registry.common.errors.InvalidSchemaException;
import org.apache.hadoop.hive.registry.common.errors.SchemaBranchAlreadyExistsException;
import org.apache.hadoop.hive.registry.common.errors.SchemaBranchNotFoundException;
import org.apache.hadoop.hive.registry.common.errors.SchemaNotFoundException;
import org.apache.hadoop.hive.registry.common.errors.UnsupportedSchemaTypeException;
import org.apache.hadoop.hive.registry.state.SchemaLifecycleException;
import org.apache.hadoop.hive.registry.state.SchemaVersionLifecycleContext;
import org.apache.hadoop.hive.registry.state.SchemaVersionLifecycleStateMachineInfo;
import org.apache.hadoop.hive.registry.state.SchemaVersionLifecycleStates;
import org.apache.hadoop.hive.registry.state.details.InitializedStateDetails;
import org.apache.hadoop.hive.registry.state.details.MergeInfo;
import org.apache.hadoop.hive.registry.utils.ObjectMapperUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Default implementation for schema registry.
 */
public class DefaultSchemaRegistry implements ISchemaRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSchemaRegistry.class);

    public static final String ORDER_BY_FIELDS_PARAM_NAME = "_orderByFields";
    public static final String DEFAULT_SCHEMA_VERSION_MERGE_STRATEGY = "OPTIMISTIC";

    private final Collection<Map<String, Object>> schemaProvidersConfig;
    private final IMetaStoreClient metaStoreClient;

    private Map<String, SchemaProvider> schemaTypeWithProviders;
    private List<SchemaProviderInfo> schemaProviderInfos;
    private SchemaVersionLifecycleManager schemaVersionLifecycleManager;
    private SchemaBranchCache schemaBranchCache;

    public DefaultSchemaRegistry(Collection<Map<String, Object>> schemaProvidersConfig) throws MetaException {
        this.schemaProvidersConfig = schemaProvidersConfig;
        Configuration conf = MetastoreConf.newMetastoreConf();
        MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URIS, "");
        MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.EXECUTE_SET_UGI, false);
        MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.AUTO_CREATE_ALL, true);
        MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.SCHEMA_VERIFICATION, false);
        if (MetastoreConf.getVar(conf, MetastoreConf.ConfVars.TASK_THREADS_ALWAYS).equals(
                MetastoreConf.ConfVars.TASK_THREADS_ALWAYS.getDefaultVal())) {
          MetastoreConf.setVar(conf, MetastoreConf.ConfVars.TASK_THREADS_ALWAYS,
                  EventCleanerTask.class.getName());
        }
        if (MetastoreConf.getVar(conf, MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS).equals(
                MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS.getDefaultVal())) {
          MetastoreConf.setClass(conf, MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS,
                  DefaultPartitionExpressionProxy.class, PartitionExpressionProxy.class);
        }
        this.metaStoreClient = new HiveMetaStoreClient(conf);
    }

    @Override
    public void init(Map<String, Object> props) {

        Options options = new Options(props);

        schemaBranchCache = new SchemaBranchCache(options.getMaxSchemaCacheSize(),
                options.getSchemaExpiryInSecs(),
                createSchemaBranchFetcher());
        SchemaMetadataFetcher schemaMetadataFetcher = createSchemaMetadataFetcher();
        schemaVersionLifecycleManager = new SchemaVersionLifecycleManager(metaStoreClient,
                                                                          props,
                                                                          schemaMetadataFetcher,
                                                                          schemaBranchCache);

        Collection<? extends SchemaProvider> schemaProviders = initSchemaProviders(schemaProvidersConfig,
                                                                                   null);

        this.schemaTypeWithProviders = schemaProviders.stream()
                                                      .collect(Collectors.toMap(SchemaProvider::getType,
                                                                                Function.identity()));

        schemaProviderInfos = Collections.unmodifiableList(
                schemaProviders.stream()
                               .map(schemaProvider
                                            -> new SchemaProviderInfo(schemaProvider
                                                                              .getType(),
                                                                      schemaProvider
                                                                              .getName(),
                                                                      schemaProvider
                                                                              .getDescription(),
                                                                      schemaProvider
                                                                              .getDefaultSerializerClassName(),
                                                                      schemaProvider
                                                                              .getDefaultDeserializerClassName()))
                               .collect(Collectors.toList()));
    }


  private SchemaBranchCache.SchemaBranchFetcher createSchemaBranchFetcher() {
    return new SchemaBranchCache.SchemaBranchFetcher() {
      @Override
      public SchemaBranch getSchemaBranch(SchemaBranchKey schemaBranchKey) throws SchemaBranchNotFoundException {
        return DefaultSchemaRegistry.this.getSchemaBranch(schemaBranchKey);
      }

      @Override
      public SchemaBranch getSchemaBranch(Long id) throws SchemaBranchNotFoundException {
        return DefaultSchemaRegistry.this.getSchemaBranch(id);
      }
    };
  }

  private SchemaMetadataFetcher createSchemaMetadataFetcher() {
        return new SchemaMetadataFetcher() {

            @Override
            public SchemaMetadataInfo getSchemaMetadataInfo(
                    String schemaName) {
                return DefaultSchemaRegistry.this.getSchemaMetadataInfo(schemaName);
            }

            @Override
            public SchemaMetadataInfo getSchemaMetadataInfo(
                    Long schemaMetadataId) {
                return DefaultSchemaRegistry.this.getSchemaMetadataInfo(schemaMetadataId);
            }

            @Override
            public SchemaProvider getSchemaProvider(String providerType) {
                return schemaTypeWithProviders.get(providerType);
            }
        };
  }

    public interface SchemaMetadataFetcher {
        SchemaMetadataInfo getSchemaMetadataInfo(String schemaName);

        SchemaMetadataInfo getSchemaMetadataInfo(Long schemaMetadataId);

        SchemaProvider getSchemaProvider(String providerType);
    }

    private Collection<? extends SchemaProvider> initSchemaProviders(final Collection<Map<String, Object>> schemaProvidersConfig,
                                                                     final SchemaVersionRetriever schemaVersionRetriever) {
        if (schemaProvidersConfig == null || schemaProvidersConfig.isEmpty()) {
            throw new IllegalArgumentException("No [" + SCHEMA_PROVIDERS + "] property is configured in schema registry configuration file.");
        }

        return schemaProvidersConfig.stream()
                                    .map(schemaProviderConfig -> {
                                        String className = (String) schemaProviderConfig.get("providerClass");
                                        if (className == null || className.isEmpty()) {
                                            throw new IllegalArgumentException("Schema provider class name must be non empty, Invalid provider class name [" + className + "]");
                                        }

                                        try {
                                            SchemaProvider schemaProvider =
                                                    (SchemaProvider) Class.forName(className,
                                                                                   true,
                                                                                   Thread.currentThread()
                                                                                         .getContextClassLoader())
                                                                          .newInstance();
                                            HashMap<String, Object> config = new HashMap<>(schemaProviderConfig);
                                            config.put(SchemaProvider.SCHEMA_VERSION_RETRIEVER_CONFIG, schemaVersionRetriever);
                                            schemaProvider.init(Collections.unmodifiableMap(config));

                                            return schemaProvider;
                                        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                                            LOG.error("Error encountered while loading SchemaProvider [{}] ", className, e);
                                            throw new IllegalArgumentException(e);
                                        }
                                    })
                                    .collect(Collectors.toList());
    }

    @Override
    public Collection<SchemaProviderInfo> getSupportedSchemaProviders() {
        return schemaProviderInfos;
    }

    @Override
    public Long registerSchemaMetadata(SchemaMetadata schemaMetadata) throws UnsupportedSchemaTypeException {
        return addSchemaMetadata(schemaMetadata);
    }

    public Long addSchemaMetadata(SchemaMetadata schemaMetadata) throws UnsupportedSchemaTypeException {
        return addSchemaMetadata(schemaMetadata, false);
    }

    public Long addSchemaMetadata(SchemaMetadata schemaMetadata,
                                  boolean throwErrorIfExists) throws UnsupportedSchemaTypeException {
        SchemaMetadataInfo givenSchemaMetadataInfo = new SchemaMetadataInfo(schemaMetadata);
        String type = schemaMetadata.getType();
        if (schemaTypeWithProviders.get(type) == null) {
            throw new UnsupportedSchemaTypeException("Given schema type " + type + " not supported");
        }
        try {
          if (!throwErrorIfExists) {
            ISchema lookupSchemaMetadata = metaStoreClient.getISchemaByName(givenSchemaMetadataInfo.getSchemaMetadata().getName());
            if (lookupSchemaMetadata != null) {
              return lookupSchemaMetadata.getSchemaId();
            }
          }
          Long schemaId = metaStoreClient.createISchema(givenSchemaMetadataInfo.buildThriftSchemaRequest());
          givenSchemaMetadataInfo.setId(schemaId);
          // Add a schema branch for this metadata
          SchemaBranch schemaBranch = new SchemaBranch(SchemaBranch.MASTER_BRANCH, schemaMetadata.getName(),
                  String.format(SchemaBranch.MASTER_BRANCH_DESC, schemaMetadata.getName()),
                  System.currentTimeMillis());
          metaStoreClient.addSchemaBranch(schemaBranch.buildThriftSchemaBranchRequest());
        } catch (TException te) {

        }
        return givenSchemaMetadataInfo.getId();
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(Long schemaMetadataId) {
      try {
        ISchema iSchema = metaStoreClient.getISchema(schemaMetadataId);
        if (iSchema != null) {
          SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo(iSchema);
          LOG.info("SchemaMetadata entries with id [{}] is [{}]", schemaMetadataInfo);
          return schemaMetadataInfo;
        }
      } catch(TException te) {

      }
      return null;
    }

    public Collection<AggregatedSchemaMetadataInfo> findAggregatedSchemaMetadata(Map<String, String> props)
            throws SchemaBranchNotFoundException, SchemaNotFoundException {

        return findSchemaMetadata(props)
              .stream()
              .map(schemaMetadataInfo -> {
                try {
                  return buildAggregatedSchemaMetadataInfo(schemaMetadataInfo);
                } catch (SchemaNotFoundException | SchemaBranchNotFoundException e) {
                  throw new RuntimeException(e);
                }
              })
              .collect(Collectors.toList());
    }


    @Override
    public Collection<SchemaMetadataInfo> findSchemaMetadata(Map<String, String> props) {
      /*// todo get only few selected columns instead of getting the whole row.
      Collection<SchemaMetadata> storables;

      if (props == null || props.isEmpty()) {
        storables = storageManager.list(SchemaMetadataStorable.NAME_SPACE);
      } else {
        List<QueryParam> orderByFieldQueryParams = new ArrayList<>();
        List<QueryParam> queryParams = new ArrayList<>(props.size());
        for (Map.Entry<String, String> entry : props.entrySet()) {
          QueryParam queryParam = new QueryParam(entry.getKey(), entry.getValue());
          if (ORDER_BY_FIELDS_PARAM_NAME.equals(entry.getKey())) {
            orderByFieldQueryParams.add(queryParam);
          } else {
            queryParams.add(queryParam);
          }
        }
        storables = storageManager.find(SchemaMetadataStorable.NAME_SPACE, queryParams, getOrderByFields(orderByFieldQueryParams));
      }

      List<SchemaMetadataInfo> result;
      if (storables != null && !storables.isEmpty()) {
        result = storables.stream().map(SchemaMetadataStorable::toSchemaMetadataInfo).collect(Collectors.toList());
      } else {
        result = Collections.emptyList();
      }*/
      //return result;
      return null;
    }


    @Override
    public Collection<AggregatedSchemaBranch> getAggregatedSchemaBranch(String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException {
      /*Collection<AggregatedSchemaBranch> aggregatedSchemaBranches = new ArrayList<>();
      for (SchemaBranch schemaBranch : getSchemaBranches(schemaName)) {
        Long rootVersion = schemaBranch.getName().equals(SchemaBranch.MASTER_BRANCH) ? null: schemaVersionLifecycleManager.getRootVersion(schemaBranch).getId();
        Collection<SchemaVersionInfo> schemaVersionInfos = getAllVersions(schemaBranch.getName(), schemaName);
        schemaVersionInfos.stream().forEach(schemaVersionInfo -> {
          SchemaVersionLifecycleContext context = null;
          try {
            context = schemaVersionLifecycleManager.createSchemaVersionLifeCycleContext(schemaVersionInfo.getId(), SchemaVersionLifecycleStates.INITIATED);
            MergeInfo mergeInfo = null;
            if (context.getDetails() == null) {
              mergeInfo = null;
            } else {
              try {
                InitializedStateDetails details = ObjectMapperUtils.deserialize(context.getDetails(), InitializedStateDetails.class);
                mergeInfo = details.getMergeInfo();
              } catch (IOException e) {
                throw new RuntimeException(String.format("Failed to serialize state details of schema version : '%s'",context.getSchemaVersionId()),e);
              }
            }
            schemaVersionInfo.setMergeInfo(mergeInfo);
          } catch (SchemaNotFoundException e) {
            // If the schema version has never been in 'INITIATED' state, then SchemaNotFoundException error is thrown which is expected
            schemaVersionInfo.setMergeInfo(null);
          }
        });
        aggregatedSchemaBranches.add(new AggregatedSchemaBranch(schemaBranch, rootVersion, schemaVersionInfos));

      }*/
      //return aggregatedSchemaBranches;
      return null;
    }

    public SchemaVersionLifecycleStateMachineInfo getSchemaVersionLifecycleStateMachineInfo() {
      return schemaVersionLifecycleManager.getSchemaVersionLifecycleStateMachine().toConfig();
    }

    private AggregatedSchemaMetadataInfo buildAggregatedSchemaMetadataInfo(SchemaMetadataInfo schemaMetadataInfo) throws SchemaNotFoundException, SchemaBranchNotFoundException {

      if (schemaMetadataInfo == null) {
        return null;
      }

      List<SerDesInfo> serDesInfos = new ArrayList<>();

      return new AggregatedSchemaMetadataInfo(schemaMetadataInfo.getSchemaMetadata(),
              schemaMetadataInfo.getId(),
              schemaMetadataInfo.getTimestamp(),
              getAggregatedSchemaBranch(schemaMetadataInfo.getSchemaMetadata().getName()),
              serDesInfos);
    }


    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(String schemaName) {
      return null;
    }

    public SchemaIdVersion addSchemaVersion(SchemaMetadata schemaMetadata,
                                            SchemaVersion schemaVersion)
            throws IncompatibleSchemaException, InvalidSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        return schemaVersionLifecycleManager.addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, schemaVersion, x -> registerSchemaMetadata(x));
    }

    public SchemaIdVersion addSchemaVersion(String schemaBranchName,
                                            SchemaMetadata schemaMetadata,
                                            SchemaVersion schemaVersion)
            throws IncompatibleSchemaException, InvalidSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        return schemaVersionLifecycleManager.addSchemaVersion(schemaBranchName, schemaMetadata, schemaVersion, x -> registerSchemaMetadata(x));
    }

    public SchemaIdVersion addSchemaVersion(String schemaName,
                                            SchemaVersion schemaVersion)
            throws SchemaNotFoundException, IncompatibleSchemaException, InvalidSchemaException, SchemaBranchNotFoundException {
        return schemaVersionLifecycleManager.addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaName, schemaVersion);
    }

    public SchemaIdVersion addSchemaVersion(String schemaBranchName,
                                            String schemaName,
                                            SchemaVersion schemaVersion)
            throws SchemaNotFoundException, IncompatibleSchemaException, InvalidSchemaException, SchemaBranchNotFoundException {
        return schemaVersionLifecycleManager.addSchemaVersion(schemaBranchName, schemaName, schemaVersion);
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(final String schemaName) throws SchemaNotFoundException {
        return schemaVersionLifecycleManager.getAllVersions(schemaName);
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(final String schemaBranchName, final String schemaName)
            throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return schemaVersionLifecycleManager.getAllVersions(schemaBranchName, schemaName);
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(String schemaName,
                                                  String schemaText) throws SchemaNotFoundException, InvalidSchemaException, SchemaBranchNotFoundException {
        return schemaVersionLifecycleManager.getSchemaVersionInfo(schemaName, schemaText);
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(SchemaIdVersion schemaIdVersion) throws SchemaNotFoundException {
        return schemaVersionLifecycleManager.getSchemaVersionInfo(schemaIdVersion);
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        return schemaVersionLifecycleManager.getSchemaVersionInfo(schemaVersionKey);
    }

    @Override
    public void deleteSchemaVersion(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException, SchemaLifecycleException {
        schemaVersionLifecycleManager.deleteSchemaVersion(schemaVersionKey);
    }

    @Override
    public void enableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException, IncompatibleSchemaException, SchemaBranchNotFoundException {
        schemaVersionLifecycleManager.enableSchemaVersion(schemaVersionId);
    }

    @Override
    public void deleteSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        schemaVersionLifecycleManager.deleteSchemaVersion(schemaVersionId);
    }

    @Override
    public void archiveSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        schemaVersionLifecycleManager.archiveSchemaVersion(schemaVersionId);
    }

    @Override
    public void disableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        schemaVersionLifecycleManager.disableSchemaVersion(schemaVersionId);
    }

    @Override
    public void startSchemaVersionReview(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        schemaVersionLifecycleManager.startSchemaVersionReview(schemaVersionId);
    }

    @Override
    public void transitionState(Long schemaVersionId, Byte targetStateId, byte[] transitionDetails) throws SchemaNotFoundException, SchemaLifecycleException {
        schemaVersionLifecycleManager.executeState(schemaVersionId, targetStateId, transitionDetails);
    }

    @Override
    public SchemaVersionMergeResult mergeSchemaVersion(Long schemaVersionId, SchemaVersionMergeStrategy schemaVersionMergeStrategy) throws SchemaNotFoundException, IncompatibleSchemaException {
        return schemaVersionLifecycleManager.mergeSchemaVersion(schemaVersionId, schemaVersionMergeStrategy);
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(String schemaBranchName, String schemaName, List<Byte> stateIds) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        if(stateIds == null || stateIds.isEmpty())
            return getAllVersions(schemaBranchName, schemaName);
        else
            return schemaVersionLifecycleManager.getAllVersions(schemaBranchName, schemaName, stateIds);
    }

    @Override
    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaBranchName, String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return schemaVersionLifecycleManager.getLatestSchemaVersionInfo(schemaBranchName, schemaName);
    }


    public CompatibilityResult checkCompatibility(String schemaBranchName, String schemaName, String toSchema) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return schemaVersionLifecycleManager.checkCompatibility(schemaBranchName, schemaName, toSchema);
    }

    public String uploadFile(InputStream inputStream) {
        return null;
    }


    private SchemaBranch getSchemaBranch(SchemaBranchKey schemaBranchKey) throws SchemaBranchNotFoundException {

      try {
        Collection <ISchemaBranch> iSchemaBranches = metaStoreClient.getSchemaBranchBySchemaName(schemaBranchKey.getSchemaMetadataName());
        if (iSchemaBranches == null || iSchemaBranches.isEmpty())
          throw new SchemaBranchNotFoundException(String.format("Schema branch with key : %s not found", schemaBranchKey));
        else if (iSchemaBranches.size() > 1)
          throw new SchemaBranchNotFoundException(String.format("Failed to unique determine a schema branch with key : %s", schemaBranchKey));
        return new SchemaBranch(iSchemaBranches.iterator().next());
      } catch (TException te) {

      }
      return null;
    }

    private SchemaBranch getSchemaBranch(Long id) throws SchemaBranchNotFoundException {
    try {
        ISchemaBranch iSchemaBranch = metaStoreClient.getSchemaBranch(id);
        if (iSchemaBranch == null)
          throw new SchemaBranchNotFoundException(String.format("Schema branch with id : '%s' not found", id.toString()));
        // size of the collection will always be less than 2, as ID is a primary key, so no need handle the case where size > 1
        return new SchemaBranch(iSchemaBranch);
      } catch (TException te) {

      }
      return null;
    }

    @Override
    public void deleteSchemaBranch(Long schemaBranchId) throws SchemaBranchNotFoundException, InvalidSchemaBranchDeletionException {

    }


    public static class Options {
        // we may want to remove schema.registry prefix from configuration properties as these are all properties
        // given by client.
        public static final String SCHEMA_CACHE_SIZE = "schemaCacheSize";
        public static final String SCHEMA_CACHE_EXPIRY_INTERVAL_SECS = "schemaCacheExpiryInterval";
        public static final int DEFAULT_SCHEMA_CACHE_SIZE = 10000;
        public static final long DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS = 60 * 60L;

        private final Map<String, ?> config;

        public Options(Map<String, ?> config) {
            this.config = config;
        }

        private Object getPropertyValue(String propertyKey, Object defaultValue) {
            Object value = config.get(propertyKey);
            return value != null ? value : defaultValue;
        }

        public int getMaxSchemaCacheSize() {
            return Integer.valueOf(getPropertyValue(SCHEMA_CACHE_SIZE, DEFAULT_SCHEMA_CACHE_SIZE).toString());
        }

        public long getSchemaExpiryInSecs() {
            return Long.valueOf(getPropertyValue(SCHEMA_CACHE_EXPIRY_INTERVAL_SECS, DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS)
                                        .toString());
        }
    }

}
