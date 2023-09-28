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

package org.apache.hadoop.hive.ql.exec.repl;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasServer;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.repl.atlas.AtlasReplInfo;
import org.apache.hadoop.hive.ql.exec.repl.atlas.AtlasRequestBuilder;
import org.apache.hadoop.hive.ql.exec.repl.atlas.AtlasRestClient;
import org.apache.hadoop.hive.ql.exec.repl.atlas.AtlasRestClientBuilder;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.util.Retryable;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.parse.repl.dump.log.AtlasDumpLogger;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Status;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;

/**
 * Atlas Metadata Replication Dump Task.
 **/
public class AtlasDumpTask extends Task<AtlasDumpWork> implements Serializable {

  private static final transient Logger LOG = LoggerFactory.getLogger(AtlasDumpTask.class);
  private static final long serialVersionUID = 1L;
  private transient AtlasRestClient atlasRestClient;

  public AtlasDumpTask() {
    super();
  }

  @VisibleForTesting
  AtlasDumpTask(final AtlasRestClient atlasRestClient, final HiveConf conf, final AtlasDumpWork work) {
    this.conf = conf;
    this.work = work;
    this.atlasRestClient = atlasRestClient;
  }

  @Override
  public int execute() {
    try {
      SecurityUtils.reloginExpiringKeytabUser();
      AtlasReplInfo atlasReplInfo = createAtlasReplInfo();
      LOG.info("Dumping Atlas metadata of srcDb: {}, for TgtDb: {} to staging location: {}",
              atlasReplInfo.getSrcDB(), atlasReplInfo.getTgtDB(), atlasReplInfo.getStagingDir());
      AtlasDumpLogger replLogger = new AtlasDumpLogger(atlasReplInfo.getSrcDB(),
              atlasReplInfo.getStagingDir().toString());
      replLogger.startLog();
      Map<String, Long> metricMap = new HashMap<>();
      metricMap.put(ReplUtils.MetricName.ENTITIES.name(), 0L);
      work.getMetricCollector().reportStageStart(getName(), metricMap);
      atlasRestClient = new AtlasRestClientBuilder(atlasReplInfo.getAtlasEndpoint())
              .getClient(atlasReplInfo.getConf());
      AtlasRequestBuilder atlasRequestBuilder = new AtlasRequestBuilder();
      String entityGuid = checkHiveEntityGuid(atlasRequestBuilder, atlasReplInfo.getSrcCluster(),
              atlasReplInfo.getSrcDB());
      long numBytesWritten = dumpAtlasMetaData(atlasRequestBuilder, atlasReplInfo);
      LOG.debug("Finished dumping atlas metadata, total:{} bytes written", numBytesWritten);
      long currentModifiedTime = getCurrentTimestamp(atlasReplInfo, entityGuid);
      createDumpMetadata(atlasReplInfo, currentModifiedTime);
      replLogger.endLog(0L);
      work.getMetricCollector().reportStageEnd(getName(), Status.SUCCESS);
      return 0;
    } catch (RuntimeException e) {
      LOG.error("RuntimeException while dumping atlas metadata", e);
      setException(e);
      try{
        ReplUtils.handleException(true, e, work.getStagingDir().getParent().toString(), work.getMetricCollector(),
                getName(), conf);
      } catch (Exception ex){
        LOG.error("Failed to collect replication metrics: ", ex);
      }
      throw e;
    } catch (Exception e) {
      LOG.error("Exception while dumping atlas metadata", e);
      setException(e);
      int errorCode = ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode();
      try{
        return ReplUtils.handleException(true, e, work.getStagingDir().getParent().toString(), work.getMetricCollector(),
                getName(), conf);
      } catch (Exception ex) {
        LOG.error("Failed to collect replication metrics: ", ex);
        return errorCode;
      }
    }
  }

  private AtlasReplInfo createAtlasReplInfo() throws SemanticException, MalformedURLException {
    String errorFormat = "%s is mandatory config for Atlas metadata replication";
    //Also validates URL for endpoint.
    String endpoint = new URL(ReplUtils.getNonEmpty(HiveConf.ConfVars.REPL_ATLAS_ENDPOINT.varname, conf, errorFormat))
            .toString();
    String tgtDB = ReplUtils.getNonEmpty(HiveConf.ConfVars.REPL_ATLAS_REPLICATED_TO_DB.varname, conf, errorFormat);
    String srcCluster = ReplUtils.getNonEmpty(HiveConf.ConfVars.REPL_SOURCE_CLUSTER_NAME.varname, conf, errorFormat);
    String tgtCluster = ReplUtils.getNonEmpty(HiveConf.ConfVars.REPL_TARGET_CLUSTER_NAME.varname, conf, errorFormat);
    AtlasReplInfo atlasReplInfo = new AtlasReplInfo(endpoint, work.getSrcDB(), tgtDB, srcCluster,
            tgtCluster, work.getStagingDir(), work.getTableListPath(), conf);
    atlasReplInfo.setSrcFsUri(conf.get(ReplUtils.DEFAULT_FS_CONFIG));
    long lastTimeStamp = work.isBootstrap() ? 0L : lastStoredTimeStamp();
    atlasReplInfo.setTimeStamp(lastTimeStamp);
    return atlasReplInfo;
  }

  private long lastStoredTimeStamp() throws SemanticException {
    Path prevMetadataPath = new Path(work.getPrevAtlasDumpDir(), EximUtil.METADATA_NAME);
    Retryable retryable = Retryable.builder()
      .withHiveConf(conf)
      .withRetryOnException(IOException.class)
      .withFailOnException(FileNotFoundException.class).build();
    try {
      return retryable.executeCallable(() -> {
        BufferedReader br = null;
        try {
          FileSystem fs = prevMetadataPath.getFileSystem(conf);
          br = new BufferedReader(new InputStreamReader(fs.open(prevMetadataPath), Charset.defaultCharset()));
          String line = br.readLine();
          if (line == null) {
            throw new SemanticException(ErrorMsg.REPL_INVALID_INTERNAL_CONFIG_FOR_SERVICE
              .format("Could not read lastStoredTimeStamp from atlas metadata file",
                ReplUtils.REPL_ATLAS_SERVICE));
          }
          String[] lineContents = line.split("\t", 5);
          return Long.parseLong(lineContents[1]);
        } finally {
          if (br != null) {
            try {
              br.close();
            } catch (IOException e) {
              //Do nothing
            }
          }
        }
      });
    } catch (SemanticException e) {
      throw e;
    } catch (Exception e) {
      throw new SemanticException(ErrorMsg.REPL_RETRY_EXHAUSTED.format(e.getMessage()), e);
    }
  }

  private long getCurrentTimestamp(AtlasReplInfo atlasReplInfo, String entityGuid) throws SemanticException {
    AtlasServer atlasServer = atlasRestClient.getServer(atlasReplInfo.getSrcCluster(), conf);
    long ret = (atlasServer == null || atlasServer.getAdditionalInfoRepl(entityGuid) == null)
            ? 0L : (long) atlasServer.getAdditionalInfoRepl(entityGuid);
    LOG.debug("Current timestamp is: {}", ret);
    return ret;
  }

  long dumpAtlasMetaData(AtlasRequestBuilder atlasRequestBuilder, AtlasReplInfo atlasReplInfo)
          throws SemanticException {
    InputStream inputStream = null;
    long numBytesWritten = 0L;
    try {
      AtlasExportRequest exportRequest = atlasRequestBuilder.createExportRequest(atlasReplInfo);
      inputStream = atlasRestClient.exportData(exportRequest);
      if (inputStream == null) {
        LOG.info("There is no Atlas metadata to be exported");
      } else {
        FileSystem fs = atlasReplInfo.getStagingDir().getFileSystem(atlasReplInfo.getConf());
        Path exportFilePath = new Path(atlasReplInfo.getStagingDir(), ReplUtils.REPL_ATLAS_EXPORT_FILE_NAME);
        numBytesWritten = Utils.writeFile(fs, exportFilePath, inputStream, conf);
      }
    } catch (SemanticException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new SemanticException(ex.getMessage(), ex);
    } finally {
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e) {
          //Do nothing
        }
      }
    }
    return numBytesWritten;
  }

  private String checkHiveEntityGuid(AtlasRequestBuilder atlasRequestBuilder, String clusterName,
                                     String srcDb)
          throws SemanticException {
    AtlasObjectId objectId = atlasRequestBuilder.getItemToExport(clusterName, srcDb);
    Set<Map.Entry<String, Object>> entries = objectId.getUniqueAttributes().entrySet();
    if (entries == null || entries.isEmpty()) {
      throw new SemanticException(ErrorMsg.REPL_INVALID_INTERNAL_CONFIG_FOR_SERVICE.format("Could find " +
        "entries in objectId for:" + clusterName, ReplUtils.REPL_ATLAS_SERVICE));
    }
    Map.Entry<String, Object> item = entries.iterator().next();
    String guid = atlasRestClient.getEntityGuid(objectId.getTypeName(), item.getKey(), (String) item.getValue());
    if (guid == null || guid.isEmpty()) {
      throw new SemanticException(ErrorMsg.REPL_INVALID_INTERNAL_CONFIG_FOR_SERVICE
        .format("Entity not found:" + objectId, ReplUtils.REPL_ATLAS_SERVICE));
    }
    return guid;
  }

  void createDumpMetadata(AtlasReplInfo atlasReplInfo, long lastModifiedTime) throws SemanticException {
    Path dumpFile = new Path(atlasReplInfo.getStagingDir(), EximUtil.METADATA_NAME);
    List<List<String>> listValues = new ArrayList<>();
    listValues.add(
            Arrays.asList(
                    atlasReplInfo.getSrcFsUri(),
                    String.valueOf(lastModifiedTime)
            )
    );
    Utils.writeOutput(listValues, dumpFile, conf, true);
    LOG.debug("Stored metadata for Atlas dump at:", dumpFile.toString());
  }

  @Override
  public StageType getType() {
    return StageType.ATLAS_DUMP;
  }

  @Override
  public String getName() {
    return "ATLAS_DUMP";
  }

  @Override
  public boolean canExecuteInParallel() {
    return false;
  }
}
