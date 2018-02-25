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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.registry.cache.SchemaBranchCache;
import org.apache.hadoop.hive.registry.common.QueryParam;
import org.apache.hadoop.hive.registry.common.util.FileStorage;
import org.apache.hadoop.hive.registry.errors.IncompatibleSchemaException;
import org.apache.hadoop.hive.registry.errors.InvalidSchemaBranchDeletionException;
import org.apache.hadoop.hive.registry.errors.InvalidSchemaException;
import org.apache.hadoop.hive.registry.errors.SchemaBranchAlreadyExistsException;
import org.apache.hadoop.hive.registry.errors.SchemaBranchNotFoundException;
import org.apache.hadoop.hive.registry.errors.SchemaNotFoundException;
import org.apache.hadoop.hive.registry.errors.UnsupportedSchemaTypeException;
import org.apache.hadoop.hive.registry.serde.SerDesException;
import org.apache.hadoop.hive.registry.state.SchemaLifecycleException;
import org.apache.hadoop.hive.registry.state.SchemaVersionLifecycleContext;
import org.apache.hadoop.hive.registry.state.SchemaVersionLifecycleStateMachineInfo;
import org.apache.hadoop.hive.registry.state.SchemaVersionLifecycleStates;
import org.apache.hadoop.hive.registry.state.details.InitializedStateDetails;
import org.apache.hadoop.hive.registry.state.details.MergeInfo;
import org.apache.hadoop.hive.registry.utils.ObjectMapperUtils;
import org.apache.hadoop.hive.registry.storage.core.OrderByField;
import org.apache.hadoop.hive.registry.storage.core.Storable;
import org.apache.hadoop.hive.registry.storage.core.StorableKey;
import org.apache.hadoop.hive.registry.storage.core.StorageManager;
import org.apache.hadoop.hive.registry.storage.core.search.OrderBy;
import org.apache.hadoop.hive.registry.storage.core.search.SearchQuery;
import org.apache.hadoop.hive.registry.storage.core.search.WhereClause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Default implementation for schema registry.
 */
public class DefaultSchemaRegistry implements ISchemaRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSchemaRegistry.class);

    public static final String ORDER_BY_FIELDS_PARAM_NAME = "_orderByFields";
    public static final String DEFAULT_SCHEMA_VERSION_MERGE_STRATEGY = "OPTIMISTIC";

    private final StorageManager storageManager;
    private final FileStorage fileStorage;
    private final Collection<Map<String, Object>> schemaProvidersConfig;

    private Map<String, SchemaProvider> schemaTypeWithProviders;
    private List<SchemaProviderInfo> schemaProviderInfos;
    private SchemaVersionLifecycleManager schemaVersionLifecycleManager;
    private SchemaBranchCache schemaBranchCache;

    public DefaultSchemaRegistry(StorageManager storageManager,
                                 FileStorage fileStorage,
                                 Collection<Map<String, Object>> schemaProvidersConfig) {
        this.storageManager = storageManager;
        this.fileStorage = fileStorage;
        this.schemaProvidersConfig = schemaProvidersConfig;
    }

    @Override
    public void init(Map<String, Object> props) {

        storageManager.registerStorables(
                Arrays.asList(
                        SchemaMetadataStorable.class,
                        SchemaVersionStorable.class,
                        SchemaVersionStateStorable.class,
                        SchemaFieldInfoStorable.class,
                        SerDesInfoStorable.class,
                        SchemaSerDesMapping.class,
                        SchemaBranchStorable.class,
                        SchemaBranchVersionMapping.class));

        Options options = new Options(props);
        schemaBranchCache = new SchemaBranchCache(options.getMaxSchemaCacheSize(),
                                                  options.getSchemaExpiryInSecs(),
                                                  createSchemaBranchFetcher());

        SchemaMetadataFetcher schemaMetadataFetcher = createSchemaMetadataFetcher();
        schemaVersionLifecycleManager = new SchemaVersionLifecycleManager(storageManager,
                                                                          props,
                                                                          schemaMetadataFetcher,
                                                                          schemaBranchCache);

        Collection<? extends SchemaProvider> schemaProviders = initSchemaProviders(schemaProvidersConfig,
                                                                                   schemaVersionLifecycleManager.getSchemaVersionRetriever());

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
        SchemaMetadataStorable givenSchemaMetadataStorable = SchemaMetadataStorable.fromSchemaMetadataInfo(new SchemaMetadataInfo(schemaMetadata));
        String type = schemaMetadata.getType();
        if (schemaTypeWithProviders.get(type) == null) {
            throw new UnsupportedSchemaTypeException("Given schema type " + type + " not supported");
        }

        if (!throwErrorIfExists) {
            Storable schemaMetadataStorable = storageManager.get(givenSchemaMetadataStorable.getStorableKey());
            if (schemaMetadataStorable != null) {
                return schemaMetadataStorable.getId();
            }
        }
        final Long nextId = storageManager.nextId(givenSchemaMetadataStorable.getNameSpace());
        givenSchemaMetadataStorable.setId(nextId);
        givenSchemaMetadataStorable.setTimestamp(System.currentTimeMillis());
        storageManager.addOrUpdate(givenSchemaMetadataStorable);

        // Add a schema branch for this metadata
        SchemaBranchStorable schemaBranchStorable = new SchemaBranchStorable(SchemaBranch.MASTER_BRANCH, schemaMetadata.getName(), String.format(SchemaBranch.MASTER_BRANCH_DESC, schemaMetadata.getName()), System.currentTimeMillis());
        schemaBranchStorable.setId(storageManager.nextId(SchemaBranchStorable.NAME_SPACE));
        storageManager.add(schemaBranchStorable);

        return givenSchemaMetadataStorable.getId();
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(Long schemaMetadataId) {
        SchemaMetadataStorable givenSchemaMetadataStorable = new SchemaMetadataStorable();
        givenSchemaMetadataStorable.setId(schemaMetadataId);

        List<QueryParam> params = Collections.singletonList(new QueryParam(SchemaMetadataStorable.ID, schemaMetadataId.toString()));
        Collection<SchemaMetadataStorable> schemaMetadataStorables = storageManager.find(SchemaMetadataStorable.NAME_SPACE, params);
        SchemaMetadataInfo schemaMetadataInfo = null;
        if (schemaMetadataStorables != null && !schemaMetadataStorables.isEmpty()) {
            schemaMetadataInfo = schemaMetadataStorables.iterator().next().toSchemaMetadataInfo();
            if (schemaMetadataStorables.size() > 1) {
                LOG.warn("No unique entry with schemaMetatadataId: [{}]", schemaMetadataId);
            }
            LOG.info("SchemaMetadata entries with id [{}] is [{}]", schemaMetadataStorables);
        }

        return schemaMetadataInfo;
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(String schemaName) {
        SchemaMetadataStorable givenSchemaMetadataStorable = new SchemaMetadataStorable();
        givenSchemaMetadataStorable.setName(schemaName);

        SchemaMetadataStorable schemaMetadataStorable = storageManager.get(givenSchemaMetadataStorable.getStorableKey());

        return schemaMetadataStorable != null ? schemaMetadataStorable.toSchemaMetadataInfo() : null;
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
    public SchemaMetadataInfo updateSchemaMetadata(String schemaName, SchemaMetadata schemaMetadata) {
        if (!schemaName.equals(schemaMetadata.getName())) {
            throw new IllegalArgumentException("schemaName must match the name in schemaMetadata");
        }
        SchemaMetadataStorable givenSchemaMetadataStorable = new SchemaMetadataStorable();
        givenSchemaMetadataStorable.setName(schemaName);

        SchemaMetadataStorable schemaMetadataStorable = storageManager.get(givenSchemaMetadataStorable.getStorableKey());
        if (schemaMetadataStorable != null) {
            schemaMetadataStorable = SchemaMetadataStorable.updateSchemaMetadata(schemaMetadataStorable, schemaMetadata);
            storageManager.addOrUpdate(schemaMetadataStorable);
            return schemaMetadataStorable.toSchemaMetadataInfo();
        } else {
            return null;
        }
    }

    @Override
    public Collection<SchemaMetadataInfo> findSchemaMetadata(Map<String, String> props) {
        // todo get only few selected columns instead of getting the whole row.
        Collection<SchemaMetadataStorable> storables;

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
        }

        return result;
    }

    private List<OrderByField> getOrderByFields(List<QueryParam> queryParams) {
        if (queryParams == null || queryParams.isEmpty()) {
            return Collections.emptyList();
        }

        List<OrderByField> orderByFields = new ArrayList<>();
        for (QueryParam queryParam : queryParams) {
            if (ORDER_BY_FIELDS_PARAM_NAME.equals(queryParam.getName())) {
                // _orderByFields=[<field-name>,<a/d>,]*
                // example can be : _orderByFields=foo,a,bar,d
                // order by foo with ascending then bar with descending
                String value = queryParam.getValue();
                String[] splitStrings = value.split(",");
                for (int i = 0; i < splitStrings.length; i += 2) {
                    String ascStr = splitStrings[i + 1];
                    boolean descending;
                    if ("a".equals(ascStr)) {
                        descending = false;
                    } else if ("d".equals(ascStr)) {
                        descending = true;
                    } else {
                        throw new IllegalArgumentException("Ascending or Descending identifier can only be 'a' or 'd' respectively.");
                    }

                    orderByFields.add(OrderByField.of(splitStrings[i], descending));
                }
            }
        }

        return orderByFields;
    }

    @Override
    public Collection<SchemaVersionKey> findSchemasByFields(SchemaFieldQuery schemaFieldQuery) {
        List<QueryParam> queryParams = buildQueryParam(schemaFieldQuery);

        Collection<SchemaFieldInfoStorable> fieldInfos = storageManager.find(SchemaFieldInfoStorable.STORABLE_NAME_SPACE, queryParams);
        Collection<SchemaVersionKey> schemaVersionKeys;
        if (fieldInfos != null && !fieldInfos.isEmpty()) {
            List<Long> schemaIds = new ArrayList<>();
            for (SchemaFieldInfoStorable fieldInfo : fieldInfos) {
                schemaIds.add(fieldInfo.getSchemaInstanceId());
            }

            // todo get only few selected columns instead of getting the whole row.
            // add OR query to find items from store
            schemaVersionKeys = new ArrayList<>();
            for (Long schemaId : schemaIds) {
                SchemaVersionKey schemaVersionKey = getSchemaKey(schemaId);
                if (schemaVersionKey != null) {
                    schemaVersionKeys.add(schemaVersionKey);
                }
            }
        } else {
            schemaVersionKeys = Collections.emptyList();
        }

        return schemaVersionKeys;
    }

    private SchemaVersionKey getSchemaKey(Long schemaId) {
        SchemaVersionKey schemaVersionKey = null;

        List<QueryParam> queryParams = Collections.singletonList(new QueryParam(SchemaVersionStorable.ID, schemaId.toString()));
        Collection<SchemaVersionStorable> versionedSchemas = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams);
        if (versionedSchemas != null && !versionedSchemas.isEmpty()) {
            SchemaVersionStorable storable = versionedSchemas.iterator().next();
            schemaVersionKey = new SchemaVersionKey(storable.getName(), storable.getVersion());
        }

        return schemaVersionKey;
    }

    private List<QueryParam> buildQueryParam(SchemaFieldQuery schemaFieldQuery) {
        List<QueryParam> queryParams = new ArrayList<>(3);
        if (schemaFieldQuery.getNamespace() != null) {
            queryParams.add(new QueryParam(SchemaFieldInfo.FIELD_NAMESPACE, schemaFieldQuery.getNamespace()));
        }
        if (schemaFieldQuery.getName() != null) {
            queryParams.add(new QueryParam(SchemaFieldInfo.NAME, schemaFieldQuery.getName()));
        }
        if (schemaFieldQuery.getType() != null) {
            queryParams.add(new QueryParam(SchemaFieldInfo.TYPE, schemaFieldQuery.getType()));
        }

        return queryParams;
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
    public Collection<AggregatedSchemaBranch> getAggregatedSchemaBranch(String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        Collection<AggregatedSchemaBranch> aggregatedSchemaBranches = new ArrayList<>();
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
        }
        return aggregatedSchemaBranches;
    }


    @Override
    public SchemaVersionMergeResult mergeSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, IncompatibleSchemaException {
        return mergeSchemaVersion(schemaVersionId, SchemaVersionMergeStrategy.valueOf(DEFAULT_SCHEMA_VERSION_MERGE_STRATEGY));
    }

    public SchemaVersionLifecycleStateMachineInfo getSchemaVersionLifecycleStateMachineInfo() {
        return schemaVersionLifecycleManager.getSchemaVersionLifecycleStateMachine().toConfig();
    }

    @Override
    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName) throws SchemaNotFoundException {
        return schemaVersionLifecycleManager.getLatestSchemaVersionInfo(schemaName);
    }

    public CompatibilityResult checkCompatibility(String schemaName, String toSchema) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return schemaVersionLifecycleManager.checkCompatibility(SchemaBranch.MASTER_BRANCH, schemaName, toSchema);
    }

    @Override
    public SchemaBranch createSchemaBranch(Long schemaVersionId, SchemaBranch schemaBranch) throws SchemaBranchAlreadyExistsException, SchemaNotFoundException {

        Preconditions.checkNotNull(schemaBranch.getName(), "Schema branch name can't be null");

        SchemaVersionInfo schemaVersionInfo = schemaVersionLifecycleManager.getSchemaVersionInfo(new SchemaIdVersion(schemaVersionId));

        SchemaBranchKey schemaBranchKey = new SchemaBranchKey(schemaBranch.getName(), schemaVersionInfo.getName());
        SchemaBranch existingSchemaBranch = null;
        try {
            existingSchemaBranch = schemaBranchCache.get(SchemaBranchCache.Key.of(schemaBranchKey));
        } catch (SchemaBranchNotFoundException e) {
            // Ignore this error
        }

        if (existingSchemaBranch != null)
           throw new SchemaBranchAlreadyExistsException(String.format("A schema branch with name : '%s' already exists", schemaBranch.getName()));
        SchemaBranchStorable schemaBranchStorable = SchemaBranchStorable.from(schemaBranch);
        schemaBranchStorable.setSchemaMetadataName(schemaVersionInfo.getName());
        schemaBranchStorable.setId(storageManager.nextId(SchemaBranchStorable.NAME_SPACE));
        storageManager.add(schemaBranchStorable);

        SchemaBranch persistedSchemaBranch;
        try {
           persistedSchemaBranch  = schemaBranchCache.get(SchemaBranchCache.Key.of(schemaBranchKey));
        } catch (SchemaBranchNotFoundException e) {
            throw new RuntimeException(String.format("Failed to fetch persisted schema branch : '%s' from the database", schemaBranch.getName()));
        }

        SchemaBranchVersionMapping schemaBranchVersionMapping = new SchemaBranchVersionMapping(persistedSchemaBranch.getId(), schemaVersionInfo.getId());
        storageManager.add(schemaBranchVersionMapping);

        return persistedSchemaBranch;
    }

    @Override
    public Collection<SchemaBranch> getSchemaBranches(String schemaName) throws SchemaNotFoundException {
        Collection<SchemaVersionInfo> schemaVersionInfos = getAllVersions(schemaName);
        return schemaVersionInfos.stream().flatMap(schemaVersionInfo -> {
            try {
                return schemaVersionLifecycleManager.getSchemaBranches(schemaVersionInfo.getId()).stream();
            } catch (SchemaBranchNotFoundException e) {
                throw new RuntimeException(String.format("Failed to obtain schema branch associated with schema name : %s", schemaName),e);
            }
        }).collect(Collectors.toSet());
    }

    @Override
    public void deleteSchemaBranch(Long schemaBranchId) throws SchemaBranchNotFoundException, InvalidSchemaBranchDeletionException {

        Preconditions.checkNotNull(schemaBranchId, "Schema branch name can't be null");

        SchemaBranch schemaBranch = schemaBranchCache.get(SchemaBranchCache.Key.of(schemaBranchId));

        if (schemaBranch.getName().equals(SchemaBranch.MASTER_BRANCH))
            throw new InvalidSchemaBranchDeletionException(String.format("Can't delete '%s' branch", SchemaBranch.MASTER_BRANCH));

        SchemaBranchCache.Key keyOfSchemaBranchToDelete = SchemaBranchCache.Key.of(schemaBranchId);
        schemaBranchCache.invalidateSchemaBranch(keyOfSchemaBranchToDelete);

        List<QueryParam> schemaVersionMappingStorableQueryParams = new ArrayList<>();
        schemaVersionMappingStorableQueryParams.add(new QueryParam(SchemaBranchVersionMapping.SCHEMA_BRANCH_ID, schemaBranch.getId().toString()));
        List<OrderByField> schemaVersionMappingOrderbyFields = new ArrayList<>();
        schemaVersionMappingOrderbyFields.add(OrderByField.of(SchemaBranchVersionMapping.SCHEMA_VERSION_INFO_ID, false));
        Collection<SchemaBranchVersionMapping> schemaBranchVersionMappings = storageManager.find(SchemaBranchVersionMapping.NAMESPACE,
                schemaVersionMappingStorableQueryParams,
                schemaVersionMappingOrderbyFields);

        if (schemaBranchVersionMappings == null)
            throw new RuntimeException("Schema branch is invalid state, its not associated with any schema versions");

        // Ignore the first version as it used in the 'MASTER' branch
        Iterator<SchemaBranchVersionMapping> schemaBranchVersionMappingIterator = schemaBranchVersionMappings.iterator();
        SchemaBranchVersionMapping rootVersionMapping = schemaBranchVersionMappingIterator.next();
        storageManager.remove(rootVersionMapping.getStorableKey());

        // Validate if the schema versions in the branch to be deleted are the root versions for other branches
        Map <Integer, List<String>> schemaVersionTiedToOtherBranch = new HashMap<>();
        List <Long> schemaVersionsToBeDeleted = new ArrayList<>();

        while(schemaBranchVersionMappingIterator.hasNext()) {
            SchemaBranchVersionMapping schemaBranchVersionMapping = schemaBranchVersionMappingIterator.next();
            Long schemaVersionId = schemaBranchVersionMapping.getSchemaVersionInfoId();
            try {
                List<QueryParam> schemaVersionCountParam = new ArrayList<>();
                schemaVersionCountParam.add(new QueryParam(SchemaBranchVersionMapping.SCHEMA_VERSION_INFO_ID, schemaBranchVersionMapping.getSchemaVersionInfoId().toString()));
                Collection<SchemaBranchVersionMapping> mappingsForSchemaTiedToMutlipleBranch = storageManager.find(SchemaBranchVersionMapping.NAMESPACE, schemaVersionCountParam);
                if (mappingsForSchemaTiedToMutlipleBranch.size() > 1) {
                    SchemaVersionInfo schemaVersionInfo = schemaVersionLifecycleManager.getSchemaVersionInfo(new SchemaIdVersion(schemaVersionId));
                    List<String> forkedBranchName = mappingsForSchemaTiedToMutlipleBranch.stream().
                            filter(mapping -> !mapping.getSchemaBranchId().equals(schemaBranchId)).
                            map(mappping -> schemaBranchCache.get(SchemaBranchCache.Key.of(mappping.getSchemaBranchId())).getName()).
                            collect(Collectors.toList());
                    schemaVersionTiedToOtherBranch.put(schemaVersionInfo.getVersion(), forkedBranchName);
                } else {
                    schemaVersionsToBeDeleted.add(schemaVersionId);
                }
            } catch (SchemaNotFoundException e) {
                throw new RuntimeException(String.format("Failed to delete schema version : '%s' of schema branch : '%s'",schemaVersionId.toString(), schemaBranchId), e);
            }
        }

        if (!schemaVersionTiedToOtherBranch.isEmpty()) {
            StringBuilder message = new StringBuilder();
            message.append("Failed to delete branch");
            schemaVersionTiedToOtherBranch.entrySet().stream().forEach(versionWithBranch -> {
                message.append(", schema version : '").append(versionWithBranch.getKey()).append("'");
                message.append(" is tied to branch : '").append(Arrays.toString(versionWithBranch.getValue().toArray())).append("'");
            });
            throw new InvalidSchemaBranchDeletionException(message.toString());
        } else {

            // Delete schema versions after validation

            for (Long schemaVersionId : schemaVersionsToBeDeleted) {
                try {
                    schemaVersionLifecycleManager.deleteSchemaVersion(schemaVersionId);
                } catch (SchemaLifecycleException e) {
                    throw new InvalidSchemaBranchDeletionException("Failed to delete schema branch, all schema versions in the branch should be in one of 'INITIATED', 'ChangesRequired' or 'Archived' state ", e);
                } catch (SchemaNotFoundException e) {
                    throw new RuntimeException(String.format("Failed to delete schema version : '%s' of schema branch : '%s'", schemaVersionId.toString(), schemaBranchId), e);
                }
            }
        }

        storageManager.remove(new SchemaBranchStorable(schemaBranchId).getStorableKey());

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

    @Override
    public String uploadFile(InputStream inputStream) {
        String fileName = UUID.randomUUID().toString();
        try {
            String uploadedFilePath = fileStorage.upload(inputStream, fileName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return fileName;
    }

    @Override
    public InputStream downloadFile(String fileId) throws IOException {
        return fileStorage.download(fileId);
    }

    @Override
    public Long addSerDes(SerDesPair serDesInfo) {
        SerDesInfoStorable serDesInfoStorable = new SerDesInfoStorable(serDesInfo);
        Long nextId = storageManager.nextId(serDesInfoStorable.getNameSpace());
        serDesInfoStorable.setId(nextId);
        serDesInfoStorable.setTimestamp(System.currentTimeMillis());
        storageManager.add(serDesInfoStorable);

        return serDesInfoStorable.getId();
    }

    @Override
    public SerDesInfo getSerDes(Long serDesId) {
        SerDesInfoStorable serDesInfoStorable = storageManager.get(createSerDesStorableKey(serDesId));
        return serDesInfoStorable != null ? serDesInfoStorable.toSerDesInfo() : null;
    }

    private StorableKey createSerDesStorableKey(Long serDesId) {
        SerDesInfoStorable serDesInfoStorable = new SerDesInfoStorable();
        serDesInfoStorable.setId(serDesId);
        return serDesInfoStorable.getStorableKey();
    }

    @Override
    public Collection<SerDesInfo> getSerDes(String schemaName) {
        return getSerDesInfos(schemaName);
    }

    private Collection<SchemaSerDesMapping> getSchemaSerDesMappings(Long schemaMetadataId) {
        List<QueryParam> queryParams =
                Collections.singletonList(new QueryParam(SchemaSerDesMapping.SCHEMA_METADATA_ID, schemaMetadataId.toString()));

        return storageManager.find(SchemaSerDesMapping.NAMESPACE, queryParams);
    }

    private List<SerDesInfo> getSerDesInfos(String schemaName) {
        Collection<SchemaSerDesMapping> schemaSerDesMappings = getSchemaSerDesMappings(getSchemaMetadataInfo(schemaName)
                                                                                               .getId());
        List<SerDesInfo> serDesInfos;
        if (schemaSerDesMappings == null || schemaSerDesMappings.isEmpty()) {
            serDesInfos = Collections.emptyList();
        } else {
            serDesInfos = new ArrayList<>();
            for (SchemaSerDesMapping schemaSerDesMapping : schemaSerDesMappings) {
                SerDesInfo serDesInfo = getSerDes(schemaSerDesMapping.getSerDesId());
                serDesInfos.add(serDesInfo);
            }
        }
        return serDesInfos;
    }

    @Override
    public void mapSchemaWithSerDes(String schemaName, Long serDesId) {
        SerDesInfo serDesInfo = getSerDes(serDesId);
        if (serDesInfo == null) {
            throw new SerDesException("Serializer with given ID " + serDesId + " does not exist");
        }

        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        SchemaSerDesMapping schemaSerDesMapping = new SchemaSerDesMapping(schemaMetadataInfo.getId(), serDesId);
        storageManager.add(schemaSerDesMapping);
    }

    @Override
    public Collection<SchemaMetadataInfo> searchSchemas(WhereClause whereClause, List<OrderBy> orderByFields) {
        SearchQuery searchQuery = SearchQuery.searchFrom(SchemaMetadataStorable.NAME_SPACE)
                                             .where(whereClause)
                                             .orderBy(orderByFields.toArray(new OrderBy[orderByFields.size()]));

        return storageManager.search(searchQuery)
                             .stream()
                             .map(y -> ((SchemaMetadataStorable) y).toSchemaMetadataInfo())
                             .collect(Collectors.toList());

    }

    public AggregatedSchemaMetadataInfo getAggregatedSchemaMetadataInfo(String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        return buildAggregatedSchemaMetadataInfo(schemaMetadataInfo);
    }

    private AggregatedSchemaMetadataInfo buildAggregatedSchemaMetadataInfo(SchemaMetadataInfo schemaMetadataInfo) throws SchemaNotFoundException, SchemaBranchNotFoundException {

        if (schemaMetadataInfo == null) {
            return null;
        }

        List<SerDesInfo> serDesInfos = getSerDesInfos(schemaMetadataInfo.getSchemaMetadata().getName());

        return new AggregatedSchemaMetadataInfo(schemaMetadataInfo.getSchemaMetadata(),
                                                schemaMetadataInfo.getId(),
                                                schemaMetadataInfo.getTimestamp(),
                                                getAggregatedSchemaBranch(schemaMetadataInfo.getSchemaMetadata().getName()),
                                                serDesInfos);
    }

    private SchemaBranch getSchemaBranch(SchemaBranchKey schemaBranchKey) throws SchemaBranchNotFoundException {
        List<QueryParam> queryParams = new ArrayList<>();
        queryParams.add(new QueryParam(SchemaBranchStorable.NAME, schemaBranchKey.getSchemaBranchName()));
        queryParams.add(new QueryParam(SchemaBranchStorable.SCHEMA_METADATA_NAME, schemaBranchKey.getSchemaMetadataName()));
        Collection <SchemaBranchStorable> schemaBranchStorables = storageManager.find(SchemaBranchStorable.NAME_SPACE, queryParams);
        if (schemaBranchStorables == null || schemaBranchStorables.isEmpty())
            throw new SchemaBranchNotFoundException(String.format("Schema branch with key : %s not found", schemaBranchKey));
        else if (schemaBranchStorables.size() > 1)
            throw new SchemaBranchNotFoundException(String.format("Failed to unique determine a schema branch with key : %s", schemaBranchKey));
        return schemaBranchStorables.iterator().next().toSchemaBranch();
    }

    private SchemaBranch getSchemaBranch(Long id) throws SchemaBranchNotFoundException {
        List<QueryParam> schemaBranchQueryParam = new ArrayList<>();
        schemaBranchQueryParam.add(new QueryParam(SchemaBranchStorable.ID, id.toString()));
        Collection<SchemaBranchStorable> schemaBranchStorables = storageManager.find(SchemaBranchStorable.NAME_SPACE, schemaBranchQueryParam);
        if(schemaBranchStorables == null || schemaBranchStorables.isEmpty())
            throw new SchemaBranchNotFoundException(String.format("Schema branch with id : '%s' not found", id.toString()));
        // size of the collection will always be less than 2, as ID is a primary key, so no need handle the case where size > 1
        return schemaBranchStorables.iterator().next().toSchemaBranch();
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
