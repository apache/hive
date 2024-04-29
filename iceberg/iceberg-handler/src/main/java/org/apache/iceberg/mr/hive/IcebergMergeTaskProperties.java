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

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.plan.MergeTaskProperties;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobContext;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.mr.Catalogs;

public class IcebergMergeTaskProperties implements MergeTaskProperties {

  private final Properties properties;

  IcebergMergeTaskProperties(Properties properties) {
    this.properties = properties;
  }

  public Path getTmpLocation() {
    String location = properties.getProperty(Catalogs.LOCATION);
    return new Path(location + "/data/");
  }

  @Override
  public Properties getSplitProperties() throws IOException {
    String tableName = properties.getProperty(Catalogs.NAME);
    String snapshotRef = properties.getProperty(Catalogs.SNAPSHOT_REF);
    Configuration configuration = SessionState.getSessionConf();
    List<JobContext> originalContextList = HiveIcebergOutputCommitter
        .generateJobContext(configuration, tableName, snapshotRef);
    List<JobContext> jobContextList = originalContextList.stream()
            .map(TezUtil::enrichContextWithVertexId)
            .collect(Collectors.toList());
    if (jobContextList.isEmpty()) {
      return null;
    }
    List<ContentFile> contentFiles = HiveIcebergOutputCommitter.getInstance().getOutputContentFiles(jobContextList);
    Properties pathToContentFile = new Properties();
    contentFiles.forEach(contentFile ->
        pathToContentFile.put(new Path(String.valueOf(contentFile.path())), contentFile));
    return pathToContentFile;
  }
}
