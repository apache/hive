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

import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.impexp.AtlasServer;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.util.Retryable;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.sun.jersey.api.client.ClientResponse.Status.NOT_FOUND;

/**
 * Implementation of RESTClient, encapsulates Atlas' REST APIs.
 */
public class AtlasRestClientImpl extends RetryingClientTimeBased implements AtlasRestClient {
  private static final Logger LOG = LoggerFactory.getLogger(AtlasRestClientImpl.class);
  private final AtlasClientV2 clientV2;

  public AtlasRestClientImpl(AtlasClientV2 clientV2, HiveConf conf) {
    this.clientV2 = clientV2;
    this.totalDurationInSeconds = conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_TOTAL_DURATION, TimeUnit.SECONDS);
    this.initialDelayInSeconds = conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_INTIAL_DELAY, TimeUnit.SECONDS);
    this.maxRetryDelayInSeconds = conf.getTimeVar(HiveConf.ConfVars
      .REPL_RETRY_MAX_DELAY_BETWEEN_RETRIES, TimeUnit.SECONDS);
    this.backOff = conf.getFloatVar(HiveConf.ConfVars.REPL_RETRY_BACKOFF_COEFFICIENT);
    this.maxJitterInSeconds = (int) conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_JITTER, TimeUnit.SECONDS);
  }

  private <T> T runWithTimeout(Callable<T> callable, long timeout, TimeUnit timeUnit) throws Exception {
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final Future<T> future = executor.submit(callable);
    executor.shutdown();
    try {
      return future.get(timeout, timeUnit);
    } catch (TimeoutException e) {
      future.cancel(true);
      throw e;
    } catch (ExecutionException e) {
      Throwable t = e.getCause();
      if (t instanceof Error) {
        throw (Error) t;
      } else if (t instanceof Exception) {
        throw (Exception) t;
      } else {
        throw new IllegalStateException(t);
      }
    }
  }

  public InputStream exportData(AtlasExportRequest request) throws SemanticException {
    LOG.debug("exportData: {}" + request);
    return invokeWithRetry(new Callable<InputStream>() {
      @Override
      public InputStream call() throws Exception {
        SecurityUtils.reloginExpiringKeytabUser();
        return clientV2.exportData(request);
      }
    });
  }

  public AtlasImportResult importData(AtlasImportRequest request, AtlasReplInfo atlasReplInfo) throws Exception {
    AtlasImportResult defaultResult = getDefaultAtlasImportResult(request);
    Path exportFilePath = new Path(atlasReplInfo.getStagingDir(), ReplUtils.REPL_ATLAS_EXPORT_FILE_NAME);
    FileSystem fs = FileSystem.get(exportFilePath.toUri(), atlasReplInfo.getConf());
    if (!fs.exists(exportFilePath)) {
      LOG.info("There is nothing to load, returning the default result.");
      return defaultResult;
    }
    LOG.debug("Atlas import data request: {}" + request);
    return invokeWithRetry(new Callable<AtlasImportResult>() {
      @Override
      public AtlasImportResult call() throws Exception {
        InputStream is = null;
        try {
          SecurityUtils.reloginExpiringKeytabUser();
          is = fs.open(exportFilePath);
          return clientV2.importData(request, is);
        } finally {
          if (is != null) {
            is.close();
          }
        }
      }
    });
  }

  private AtlasImportResult getDefaultAtlasImportResult(AtlasImportRequest request) {
    return new AtlasImportResult(request, "", "", "", 0L);
  }

  public AtlasServer getServer(String endpoint, HiveConf conf) throws SemanticException {
    Retryable retryable = Retryable.builder()
      .withHiveConf(conf)
      .withRetryOnException(AtlasServiceException.class).build();
    try {
      return retryable.executeCallable((Callable<AtlasServer>) () -> {
        try {
          return clientV2.getServer(endpoint);
        } catch (AtlasServiceException e) {
          int statusCode = e.getStatus() != null ? e.getStatus().getStatusCode() : -1;
          if (NOT_FOUND.getStatusCode() == statusCode) {
            // Atlas server entity is initialized on first import/export o/p.
            LOG.info("Atlas server entity is not found");
            return null;
          }
          throw e;
        }
      });
    } catch (Exception e) {
      throw new SemanticException(ErrorMsg.REPL_RETRY_EXHAUSTED.format(e.getMessage()), e);
    }
  }

  public String getEntityGuid(final String entityType,
                              final String attributeName, final String qualifiedName) throws SemanticException {
    int entityApiTimeOut = 10;
    final Map<String, String> attributes = new HashMap<String, String>() {
      {
        put(attributeName, qualifiedName);
      }
    };

    try {
      AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = runWithTimeout(
        () -> clientV2.getEntityByAttribute(entityType, attributes), entityApiTimeOut, TimeUnit.SECONDS);

      if (entityWithExtInfo == null || entityWithExtInfo.getEntity() == null) {
        LOG.warn("Atlas entity cannot be retrieved using: type: {} and {} - {}",
                entityType, attributeName, qualifiedName);
        return null;
      }
      return entityWithExtInfo.getEntity().getGuid();
    } catch (AtlasServiceException e) {
      int statusCode = e.getStatus() != null ? e.getStatus().getStatusCode() : -1;
      if (statusCode != NOT_FOUND.getStatusCode()) {
        throw new SemanticException(ErrorMsg.REPL_INVALID_INTERNAL_CONFIG_FOR_SERVICE.format("Exception " +
          "while getEntityGuid ", ReplUtils.REPL_ATLAS_SERVICE), e.getCause());
      }
      LOG.warn("getEntityGuid: Could not retrieve entity guid for: {}-{}-{}",
              entityType, attributeName, qualifiedName, e.getMessage());
      return null;
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  public boolean getStatus(HiveConf conf) throws SemanticException {
    Retryable retryable = Retryable.builder()
      .withHiveConf(conf)
      .withRetryOnException(Exception.class).build();
    try {
      return retryable.executeCallable(() -> clientV2.isServerReady());
    } catch (Exception e) {
      throw new SemanticException(ErrorMsg.REPL_RETRY_EXHAUSTED.format(e.getMessage()), e);
    }
  }
}
