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
package org.apache.hcatalog.templeton;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.exec.ExecuteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hcatalog.templeton.tool.JobState;
import org.apache.hcatalog.templeton.tool.TempletonControllerJob;
import org.apache.hcatalog.templeton.tool.TempletonStorage;
import org.apache.hcatalog.templeton.tool.TempletonUtils;
import org.apache.hcatalog.templeton.tool.ZooKeeperStorage;

/**
 * The helper class for all the Templeton delegator classes that
 * launch child jobs.
 */
public class LauncherDelegator extends TempletonDelegator {
    private static final Log LOG = LogFactory.getLog(Server.class);
    public static final String JAR_CLASS = TempletonControllerJob.class.getName();
    protected String runAs = null;

    public LauncherDelegator(AppConfig appConf) {
        super(appConf);
    }

    public void registerJob(String id, String user, String callback)
        throws IOException {
        JobState state = null;
        try {
            state = new JobState(id, Main.getAppConfigInstance());
            state.setUser(user);
            state.setCallback(callback);
        } finally {
            if (state != null)
                state.close();
        }
    }

    /**
     * Enqueue the TempletonControllerJob directly calling doAs.
     */
    public EnqueueBean enqueueController(String user, String callback,
                                         List<String> args)
        throws NotAuthorizedException, BusyException, ExecuteException,
        IOException, QueueException {
        try {
            UserGroupInformation ugi = UgiFactory.getUgi(user);

            final long startTime = System.nanoTime();

            String id = queueAsUser(ugi, args);

            long elapsed = ((System.nanoTime() - startTime) / ((int) 1e6));
            LOG.debug("queued job " + id + " in " + elapsed + " ms");

            if (id == null)
                throw new QueueException("Unable to get job id");

            registerJob(id, user, callback);

            return new EnqueueBean(id);
        } catch (InterruptedException e) {
            throw new QueueException("Unable to launch job " + e);
        }
    }

    private String queueAsUser(UserGroupInformation ugi, final List<String> args)
        throws IOException, InterruptedException {
        String id = ugi.doAs(new PrivilegedExceptionAction<String>() {
            public String run() throws Exception {
                String[] array = new String[args.size()];
                TempletonControllerJob ctrl = new TempletonControllerJob();
                ToolRunner.run(ctrl, args.toArray(array));
                return ctrl.getSubmittedId();
            }
        });

        return id;
    }

    public List<String> makeLauncherArgs(AppConfig appConf, String statusdir,
                                         String completedUrl,
                                         List<String> copyFiles) {
        ArrayList<String> args = new ArrayList<String>();

        args.add("-libjars");
        args.add(appConf.libJars());
        addCacheFiles(args, appConf);

        // Hadoop vars
        addDef(args, "user.name", runAs);
        addDef(args, AppConfig.HADOOP_SPECULATIVE_NAME, "false");
        addDef(args, AppConfig.HADOOP_CHILD_JAVA_OPTS, appConf.controllerMRChildOpts());

        // Internal vars
        addDef(args, TempletonControllerJob.STATUSDIR_NAME, statusdir);
        addDef(args, TempletonControllerJob.COPY_NAME,
            TempletonUtils.encodeArray(copyFiles));
        addDef(args, TempletonControllerJob.OVERRIDE_CLASSPATH,
            makeOverrideClasspath(appConf));

        // Job vars
        addStorageVars(args);
        addCompletionVars(args, completedUrl);

        return args;
    }

    // Storage vars
    private void addStorageVars(List<String> args) {
        addDef(args, TempletonStorage.STORAGE_CLASS,
            appConf.get(TempletonStorage.STORAGE_CLASS));
        addDef(args, TempletonStorage.STORAGE_ROOT,
            appConf.get(TempletonStorage.STORAGE_ROOT));
        addDef(args, ZooKeeperStorage.ZK_HOSTS,
            appConf.get(ZooKeeperStorage.ZK_HOSTS));
        addDef(args, ZooKeeperStorage.ZK_SESSION_TIMEOUT,
            appConf.get(ZooKeeperStorage.ZK_SESSION_TIMEOUT));
    }

    // Completion notifier vars
    private void addCompletionVars(List<String> args, String completedUrl) {
        addDef(args, AppConfig.HADOOP_END_RETRY_NAME,
            appConf.get(AppConfig.CALLBACK_RETRY_NAME));
        addDef(args, AppConfig.HADOOP_END_INTERVAL_NAME,
            appConf.get(AppConfig.CALLBACK_INTERVAL_NAME));
        addDef(args, AppConfig.HADOOP_END_URL_NAME, completedUrl);
    }

    /**
     * Add files to the Distributed Cache for the controller job.
     */
    public static void addCacheFiles(List<String> args, AppConfig appConf) {
        String overrides = appConf.overrideJarsString();
        if (overrides != null) {
            args.add("-files");
            args.add(overrides);
        }
    }

    /**
     * Create the override classpath, which will be added to
     * HADOOP_CLASSPATH at runtime by the controller job.
     */
    public static String makeOverrideClasspath(AppConfig appConf) {
        String[] overrides = appConf.overrideJars();
        if (overrides == null)
            return null;

        ArrayList<String> cp = new ArrayList<String>();
        for (String fname : overrides) {
            Path p = new Path(fname);
            cp.add(p.getName());
        }
        return StringUtils.join(":", cp);
    }


    /**
     * Add a Hadoop command line definition to args if the value is
     * not null.
     */
    public static void addDef(List<String> args, String name, String val) {
        if (val != null) {
            args.add("-D");
            args.add(name + "=" + val);
        }
    }

}
