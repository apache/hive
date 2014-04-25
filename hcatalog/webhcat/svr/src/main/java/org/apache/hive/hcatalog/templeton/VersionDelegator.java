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
package org.apache.hive.hcatalog.templeton;

import org.apache.hadoop.util.VersionInfo;
import org.apache.hive.common.util.HiveVersionInfo;
import org.eclipse.jetty.http.HttpStatus;

import javax.ws.rs.core.Response;
import java.io.IOException;

/**
 * Find the version of Hive, Hadoop, or Pig that is being used in this
 * interface.
 */
public class VersionDelegator extends TempletonDelegator {

  public VersionDelegator(AppConfig appConf) {
    super(appConf);

  }

  public Response getVersion(String module) throws IOException {
    if (module.toLowerCase().equals("hadoop")) {
      return getHadoopVersion();
    } else if (module.toLowerCase().equals("hive")) {
      return getHiveVersion();
    } else if (module.toLowerCase().equals("sqoop")) {
      return getSqoopVersion();
    } else if (module.toLowerCase().equals("pig")) {
      return getPigVersion();
    } else {
      return SimpleWebException.buildMessage(HttpStatus.NOT_FOUND_404, null,
          "Unknown module " + module);
    }
  }

  private Response getHadoopVersion() throws IOException {
    String version = VersionInfo.getVersion();
    return JsonBuilder.create()
        .put("module", "hadoop")
        .put("version", version)
        .build();
  }

  private Response getHiveVersion() throws IOException {
    String version = HiveVersionInfo.getVersion();
    return JsonBuilder.create()
        .put("module", "hive")
        .put("version", version)
        .build();
  }

  private Response getSqoopVersion() {
    return SimpleWebException.buildMessage(HttpStatus.NOT_IMPLEMENTED_501,
        null, "Sqoop version request not yet implemented");
  }

  private Response getPigVersion() {
    return SimpleWebException.buildMessage(HttpStatus.NOT_IMPLEMENTED_501,
        null, "Pig version request not yet implemented");
  }
}
