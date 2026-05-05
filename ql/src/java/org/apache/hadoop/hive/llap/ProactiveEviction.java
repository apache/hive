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
package org.apache.hadoop.hive.llap;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.net.SocketFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.impl.LlapManagementProtocolClientImpl;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.net.NetUtils;
import org.apache.hive.common.util.ShutdownHookManager;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Through this class the caller (typically HS2) can request eviction of buffers from LLAP cache by specifying a DB,
 * table or partition name/(value). Request sending is implemented here.
 */
public final class ProactiveEviction {

  private static final Logger LOG = LoggerFactory.getLogger(ProactiveEviction.class);

  static {
    ShutdownHookManager.addShutdownHook(new Runnable() {
      @Override
      public void run() {
        if (EXECUTOR != null) {
          EXECUTOR.shutdownNow();
        }
      }
    });
  }

  private static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setNameFormat("Proactive-Eviction-Requester").setDaemon(true).build());

  private ProactiveEviction() {
    // Not to be used;
  }

  /**
   * Trigger LLAP cache eviction of buffers related to entities residing in request parameter.
   * @param conf
   * @param request
   */
  public static void evict(Configuration conf, Request request) {
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_IO_PROACTIVE_EVICTION_ENABLED)) {
      return;
    }

    try {
      LlapRegistryService llapRegistryService = LlapRegistryService.getClient(conf);
      Collection<LlapServiceInstance> instances = llapRegistryService.getInstances().getAll();
      if (instances.size() == 0) {
        // Not in LLAP mode.
        return;
      }
      LOG.info("Requesting proactive LLAP cache eviction.");
      LOG.debug("Request: {}", request);
      // Fire and forget - requests are enqueued on the single threaded executor and this (caller) thread won't wait.
      for (LlapServiceInstance instance : instances) {
        EvictionRequestTask task = new EvictionRequestTask(conf, instance, request);
        EXECUTOR.execute(task);
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * The executable task to carry out request sending.
   */
  public static class EvictionRequestTask implements Runnable {
    private final Request request;
    private Configuration conf;
    private LlapServiceInstance instance;
    private SocketFactory socketFactory;
    private RetryPolicy retryPolicy;

    EvictionRequestTask(Configuration conf, LlapServiceInstance llapServiceInstance, Request request) {
      this.conf = conf;
      this.instance = llapServiceInstance;
      this.socketFactory = NetUtils.getDefaultSocketFactory(conf);
      //not making this configurable, best effort
      this.retryPolicy = RetryPolicies.retryUpToMaximumTimeWithFixedSleep(
          10000, 2000L, TimeUnit.MILLISECONDS);
      this.request = request;
    }

    @Override
    public void run() {
      if (request.isEmpty()) {
        throw new IllegalArgumentException("No entities set to trigger eviction on.");
      }
      try {
        LlapManagementProtocolClientImpl client = new LlapManagementProtocolClientImpl(conf, instance.getHost(),
            instance.getManagementPort(), retryPolicy, socketFactory);

        List<LlapDaemonProtocolProtos.EvictEntityRequestProto> protoRequests = request.toProtoRequests();

        long evictedBytes = 0;
        for (LlapDaemonProtocolProtos.EvictEntityRequestProto protoRequest : protoRequests) {
          LOG.debug("Requesting proactive eviction for entities in catalog {}, database {}",
              protoRequest.getCatalogName(), protoRequest.getDbName());
          LlapDaemonProtocolProtos.EvictEntityResponseProto response = client.evictEntity(null, protoRequest);
          evictedBytes += response.getEvictedBytes();
          LOG.debug("Proactively evicted {} bytes", response.getEvictedBytes());
        }
        LOG.debug("Proactive eviction freed {} bytes on LLAP daemon {} in total", evictedBytes, instance);
      } catch (Exception e) {
        LOG.warn("Exception while requesting proactive eviction.", e);
      }
    }
  }

  /**
   * Holds information on entities: catalog name(s), DB name(s), table name(s), partitions.
   */
  public static final class Request {

    public record PartitionSpec(Map<String, String> spec) {}
    public record CatalogDb(String catalog, String database){}
    private final Map<CatalogDb, Map<String, Set<PartitionSpec>>> entities;

    private Request(Map<CatalogDb, Map<String, Set<PartitionSpec>>> entities) {
      this.entities = entities;
    }

    public Map<CatalogDb, Map<String, Set<PartitionSpec>>> getEntities() {
      return entities;
    }

    public boolean isEmpty() {
      return entities.isEmpty();
    }

    public boolean hasDatabaseName(String catalogName, String dbName) {
      return entities.containsKey(new CatalogDb(catalogName, dbName));
    }

    /**
     * Translate to Protobuf requests.
     * @return list of request instances ready to be sent over protobuf.
     */
    public List<LlapDaemonProtocolProtos.EvictEntityRequestProto> toProtoRequests() {
      return entities.entrySet().stream()
          .map(entry -> {
            CatalogDb catalogDb = entry.getKey();
            Map<String, Set<PartitionSpec>> tables = entry.getValue();
            LlapDaemonProtocolProtos.EvictEntityRequestProto.Builder requestBuilder =
                LlapDaemonProtocolProtos.EvictEntityRequestProto.newBuilder();

            requestBuilder.setCatalogName(catalogDb.catalog().toLowerCase());
            requestBuilder.setDbName(catalogDb.database().toLowerCase());

            tables.forEach((tableName, partitions) -> {
              LlapDaemonProtocolProtos.TableProto.Builder tableBuilder =
                  LlapDaemonProtocolProtos.TableProto.newBuilder();

              tableBuilder.setTableName(tableName.toLowerCase());

              Set<String> partitionKeys = null;

              for (PartitionSpec partitionSpec : partitions) {
                if (partitionKeys == null) {
                  partitionKeys = new LinkedHashSet<>(partitionSpec.spec().keySet());
                  tableBuilder.addAllPartKey(partitionKeys);
                }
                for (String partKey : tableBuilder.getPartKeyList()) {
                  tableBuilder.addPartVal(partitionSpec.spec().get(partKey));
                }
              }
              // For a given table the set of partition columns (keys) should not change.
              requestBuilder.addTable(tableBuilder.build());
            });
            return requestBuilder.build();
          })
          .toList();
    }

    /**
     * Match a CacheTag to this eviction request. Must only be used on LLAP side only, where the received request may
     * only contain one information for one DB.
     *
     * @param cacheTag
     * @return true if cacheTag matches and the related buffer is eligible for proactive eviction, false otherwise.
     */
    public boolean isTagMatch(CacheTag cacheTag) {
      String[] names = cacheTag.getTableName().split("\\.");
      String catalog = Warehouse.DEFAULT_CATALOG_NAME;
      String db = null;
      if (names.length == 2) {
        db = names[0];
      } else if (names.length == 3) {
        catalog = names[0];
        db = names[1];
      }
      if (db == null) {
        // Number of (catalog, DB) pairs in the request was not exactly 1.
        throw new UnsupportedOperationException("Predicate only implemented for 1 catalog and 1 DB case.");
      }
      // getTableName() returns "catalog.db.table"; TableName.fromString handles 3-part names.
      TableName tagTableName = TableName.fromString(cacheTag.getTableName(), null, null);

      // Check that the tag's catalog and database is present in the eviction request.
      if (!entities.containsKey(new CatalogDb(catalog, db))) {
        return false;
      }

      Map<String, Set<PartitionSpec>> tables = entities.getOrDefault(new CatalogDb(catalog, db), Map.of());

      // If true, must be a drop DB event and this cacheTag matches.
      if (tables.isEmpty()) {
        return true;
      }

      Map<String, String> tagPartDescMap = null;
      if (cacheTag instanceof CacheTag.PartitionCacheTag) {
        tagPartDescMap = ((CacheTag.PartitionCacheTag) cacheTag).getPartitionDescMap();
      }

      // Check against table name.
      for (String tableAndDbName : tables.keySet()) {
        if (tableAndDbName.equals(tagTableName.getNotEmptyDbTable())) {

          Set<PartitionSpec> partDescs = tables.get(tableAndDbName);

          // If true, must be a drop table event, and this cacheTag matches.
          if (partDescs == null) {
            return true;
          }

          // Check against partition keys and values and alas for drop partition event.
          if (!(cacheTag instanceof CacheTag.PartitionCacheTag)) {
            throw new IllegalArgumentException("CacheTag has no partition information, while trying" +
                " to evict due to (and based on) a drop partition DDL statement..");
          }

          if (partDescs.contains(new PartitionSpec(tagPartDescMap))) {
            return true;
          }
        }
      }
      return false;
    }

    @Override
    public String toString() {
      return "Request { entities = " + entities + " }";
    }

    /**
     * Lets callers specify what entities are requested to be evicted, and builds a Request instance accordingly.
     */
    public static final class Builder {

      private final Map<CatalogDb, Map<String, Set<PartitionSpec>>> entities;

      private Builder() {
        this.entities = new HashMap<>();
      }

      public static Builder create() {
        return new Builder();
      }

      /**
       * Add a partition of a table scoped to the given catalog.
       */
      public Builder addPartitionOfATable(String catalog, String db, String tableName,
                                          Map<String, String> partSpec) {
        ensureTable(catalog, db, tableName);
        entities.get(new CatalogDb(catalog, db)).get(tableName).add(new PartitionSpec(partSpec));
        return this;
      }

      /**
       * Add a partition of a table scoped to the default catalog.
       */
      public Builder addPartitionOfATable(String db, String tableName, Map<String, String> partSpec) {
        return addPartitionOfATable(Warehouse.DEFAULT_CATALOG_NAME, db, tableName, partSpec);
      }

      /**
       * Add a database scoped to the given catalog.
       */
      public Builder addDb(String catalog, String db) {
        ensureDb(catalog, db);
        return this;
      }

      /**
       * Add a database scoped to the default catalog.
       */
      public Builder addDb(String db) {
        return addDb(Warehouse.DEFAULT_CATALOG_NAME, db);
      }

      /**
       * Add a table scoped to the given catalog.
       */
      public Builder addTable(String catalog, String db, String table) {
        ensureTable(catalog, db, table);
        return this;
      }

      /**
       * Add a table scoped to the default catalog.
       */
      public Builder addTable(String db, String table) {
        return addTable(Warehouse.DEFAULT_CATALOG_NAME, db, table);
      }

      public Request build() {
        return new Request(entities);
      }

      private void ensureDb(String catalogName, String dbName) {
        entities.computeIfAbsent(new CatalogDb(catalogName, dbName), k -> new HashMap<>());
      }

      private void ensureTable(String catalogName, String dbName, String tableName) {
        ensureDb(catalogName, dbName);
        entities.get(new CatalogDb(catalogName, dbName)).computeIfAbsent(tableName, k -> new HashSet<>());
      }

      /**
       * Translate from Protobuf request.
       * @param protoRequest
       * @return the builder itself.
       */
      public Builder fromProtoRequest(LlapDaemonProtocolProtos.EvictEntityRequestProto protoRequest) {
        entities.clear();
        String catalogName = protoRequest.getCatalogName().toLowerCase();
        String dbName = protoRequest.getDbName().toLowerCase();

        Map<String, Set<PartitionSpec>> entitiesInDb = new HashMap<>();
        List<LlapDaemonProtocolProtos.TableProto> tables = protoRequest.getTableList();

        if (tables != null && !tables.isEmpty()) {
          for (LlapDaemonProtocolProtos.TableProto table : tables) {
            String dbAndTableName =
                (new StringBuilder().append(dbName).append('.').append(table.getTableName())).toString().toLowerCase();

            if (table.getPartValCount() == 0) {
              entitiesInDb.put(dbAndTableName, null);
              continue;
            }
            Set<PartitionSpec> partitions = new HashSet<>();
            Map<String, String> partDesc = new HashMap<>();

            for (int valIx = 0; valIx < table.getPartValCount(); ++valIx) {
              int keyIx = valIx % table.getPartKeyCount();

              partDesc.put(table.getPartKey(keyIx).toLowerCase(), table.getPartVal(valIx));

              if (keyIx == table.getPartKeyCount() - 1) {
                partitions.add(new PartitionSpec(partDesc));
                partDesc = new HashMap<>();
              }
            }

            entitiesInDb.put(dbAndTableName, partitions);
          }
        }
        entities.put(new CatalogDb(catalogName, dbName), entitiesInDb);
        return this;
      }
    }
  }

}
