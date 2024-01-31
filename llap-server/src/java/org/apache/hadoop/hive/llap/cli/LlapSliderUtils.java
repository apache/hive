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

package org.apache.hadoop.hive.llap.cli;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.service.utils.CoreFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapSliderUtils {
  private static final Logger LOG = LoggerFactory.getLogger(LlapSliderUtils.class);
  private static final String LLAP_PACKAGE_DIR = ".yarn/package/LLAP/";

  public static ServiceClient createServiceClient(Configuration conf) throws Exception {
    ServiceClient serviceClient = new ServiceClient();
    serviceClient.init(conf);
    serviceClient.start();
    return serviceClient;
  }

  public static Service getService(Configuration conf, String name) {
    LOG.info("Get service details for " + name);
    ServiceClient sc;
    try {
      sc = createServiceClient(conf);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    Service service = null;
    try {
      service = sc.getStatus(name);
    } catch (YarnException | IOException e) {
      // Probably the app does not exist
      LOG.info(e.getLocalizedMessage());
      throw new RuntimeException(e);
    } finally {
      try {
        sc.close();
      } catch (IOException e) {
        LOG.info("Failed to close service client", e);
      }
    }
    return service;
  }

  public static void startCluster(Configuration conf, String name, String packageName, Path packageDir, String queue) {
    LOG.info("Starting cluster with " + name + ", " + packageName + ", " + queue + ", " + packageDir);
    ServiceClient sc;
    try {
      sc = createServiceClient(conf);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    try {
      try {
        LOG.info("Executing the stop command");
        sc.actionStop(name, true);
      } catch (Exception ex) {
        // Ignore exceptions from stop
        LOG.info(ex.getLocalizedMessage());
      }
      try {
        LOG.info("Executing the destroy command");
        sc.actionDestroy(name);
      } catch (Exception ex) {
        // Ignore exceptions from destroy
        LOG.info(ex.getLocalizedMessage());
      }
      LOG.info("Uploading the app tarball");
      CoreFileSystem fs = new CoreFileSystem(conf);
      fs.copyLocalFileToHdfs(new File(packageDir.toString(), packageName),
          new Path(LLAP_PACKAGE_DIR), new FsPermission("755"));

      LOG.info("Executing the launch command");
      File yarnfile = new File(new Path(packageDir, "Yarnfile").toString());
      Long lifetime = null; // unlimited lifetime
      try {
        sc.actionLaunch(yarnfile.getAbsolutePath(), name, lifetime, queue);
      } finally {
      }
      LOG.debug("Started the cluster via service API");
    } catch (YarnException | IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        sc.close();
      } catch (IOException e) {
        LOG.info("Failed to close service client", e);
      }
    }
  }

}
