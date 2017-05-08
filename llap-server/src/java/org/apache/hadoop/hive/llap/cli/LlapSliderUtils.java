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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.slider.client.SliderClient;
import org.apache.slider.common.params.ActionCreateArgs;
import org.apache.slider.common.params.ActionDestroyArgs;
import org.apache.slider.common.params.ActionFreezeArgs;
import org.apache.slider.common.params.ActionInstallPackageArgs;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class LlapSliderUtils {
  private static final String SLIDER_GZ = "slider-agent.tar.gz";
  private static final Logger LOG = LoggerFactory.getLogger(LlapSliderUtils.class);

  public static SliderClient createSliderClient(
      Configuration conf) throws Exception {
    SliderClient sliderClient = new SliderClient() {
      @Override
      public void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        initHadoopBinding();
      }
    };
    Configuration sliderClientConf = new Configuration(conf);
    sliderClientConf = sliderClient.bindArgs(sliderClientConf,
      new String[]{"help"});
    sliderClient.init(sliderClientConf);
    sliderClient.start();
    return sliderClient;
  }

  public static void startCluster(Configuration conf, String name,
      String packageName, Path packageDir, String queue) {
    LOG.info("Starting cluster with " + name + ", "
      + packageName + ", " + queue + ", " + packageDir);
    SliderClient sc;
    try {
      sc = createSliderClient(conf);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    try {
      LOG.info("Executing the freeze command");
      ActionFreezeArgs freezeArgs = new ActionFreezeArgs();
      freezeArgs.force = true;
      freezeArgs.setWaittime(3600); // Wait forever (or at least for an hour).
      try {
        sc.actionFreeze(name, freezeArgs);
      } catch (UnknownApplicationInstanceException ex) {
        LOG.info("There was no old application instance to freeze");
      }
      LOG.info("Executing the destroy command");
      ActionDestroyArgs destroyArg = new ActionDestroyArgs();
      destroyArg.force = true;
      try {
        sc.actionDestroy(name, destroyArg);
      } catch (UnknownApplicationInstanceException ex) {
        LOG.info("There was no old application instance to destroy");
      }
      LOG.info("Executing the install command");
      ActionInstallPackageArgs installArgs = new ActionInstallPackageArgs();
      installArgs.name = "LLAP";
      installArgs.packageURI = new Path(packageDir, packageName).toString();
      installArgs.replacePkg = true;
      sc.actionInstallPkg(installArgs);
      LOG.info("Executing the create command");
      ActionCreateArgs createArgs = new ActionCreateArgs();
      createArgs.resources = new File(new Path(packageDir, "resources.json").toString());
      createArgs.template = new File(new Path(packageDir, "appConfig.json").toString());
      createArgs.setWaittime(3600);
      if (queue != null) {
        createArgs.queue = queue;
      }
      // See the comments in the method. SliderClient doesn't work in normal circumstances.
      File bogusSliderFile = startSetSliderLibDir();
      try {
        sc.actionCreate(name, createArgs);
      } finally {
        endSetSliderLibDir(bogusSliderFile);
      }
      LOG.debug("Started the cluster via slider API");
    } catch (YarnException | IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        sc.close();
      } catch (IOException e) {
        LOG.info("Failed to close slider client", e);
      }
    }
  }

  public static File startSetSliderLibDir() throws IOException {
    // TODO: this is currently required for the use of slider create API. Need SLIDER-1192.
    File sliderJarDir = SliderUtils.findContainingJar(SliderClient.class).getParentFile();
    File gz = new File(sliderJarDir, SLIDER_GZ);
    if (gz.exists()) {
      String path = sliderJarDir.getAbsolutePath();
      LOG.info("Setting slider.libdir based on jar file location: " + path);
      System.setProperty("slider.libdir", path);
      return null;
    }

    // There's no gz file next to slider jars. Due to the horror that is SliderClient, we'd have
    // to find it and copy it there. Let's try to find it. Also set slider.libdir.
    String path = System.getProperty("slider.libdir");
    gz = null;
    if (path != null && !path.isEmpty()) {
      LOG.info("slider.libdir was already set: " + path);
      gz = new File(path, SLIDER_GZ);
      if (!gz.exists()) {
        gz = null;
      }
    }
    if (gz == null) {
      path = System.getenv("SLIDER_HOME");
      if (path != null && !path.isEmpty()) {
        gz = new File(new File(path, "lib"), SLIDER_GZ);
        if (gz.exists()) {
          path = gz.getParentFile().getAbsolutePath();
          LOG.info("Setting slider.libdir based on SLIDER_HOME: " + path);
          System.setProperty("slider.libdir", path);
        } else {
          gz = null;
        }
      }
    }
    if (gz == null) {
      // This is a terrible hack trying to find slider on a typical installation. Sigh...
      File rootDir = SliderUtils.findContainingJar(HiveConf.class)
          .getParentFile().getParentFile().getParentFile();
      File sliderJarDir2 = new File(new File(rootDir, "slider"), "lib");
      if (sliderJarDir2.exists()) {
        gz = new File(sliderJarDir2, SLIDER_GZ);
        if (gz.exists()) {
          path = sliderJarDir2.getAbsolutePath();
          LOG.info("Setting slider.libdir based on guesswork: " + path);
          System.setProperty("slider.libdir", path);
        } else {
          gz = null;
        }
      }
    }
    if (gz == null) {
      throw new IOException("Cannot find " + SLIDER_GZ + ". Please ensure SLIDER_HOME is set.");
    }
    File newGz = new File(sliderJarDir, SLIDER_GZ);
    LOG.info("Copying " + gz + " to " + newGz);
    Files.copy(gz, newGz);
    newGz.deleteOnExit();
    return newGz;
  }

  public static void endSetSliderLibDir(File newGz) throws IOException {
    if (newGz == null || !newGz.exists()) return;
    LOG.info("Deleting " + newGz);
    newGz.delete();
  }
}
