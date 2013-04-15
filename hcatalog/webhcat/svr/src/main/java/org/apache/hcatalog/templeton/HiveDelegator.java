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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.exec.ExecuteException;
import org.apache.hcatalog.templeton.tool.TempletonControllerJob;
import org.apache.hcatalog.templeton.tool.TempletonUtils;

/**
 * Submit a Hive job.
 *
 * This is the backend of the hive web service.
 */
public class HiveDelegator extends LauncherDelegator {

    public HiveDelegator(AppConfig appConf) {
        super(appConf);
    }

    public EnqueueBean run(String user,
                           String execute, String srcFile, List<String> defines,
                           String statusdir, String callback, String completedUrl)
        throws NotAuthorizedException, BadParam, BusyException, QueueException,
        ExecuteException, IOException, InterruptedException
    {
        runAs = user;
        List<String> args = makeArgs(execute, srcFile, defines, statusdir,
                                     completedUrl);

        return enqueueController(user, callback, args);
    }

    private List<String> makeArgs(String execute, String srcFile,
            List<String> defines, String statusdir, String completedUrl)
        throws BadParam, IOException, InterruptedException
    {
        ArrayList<String> args = new ArrayList<String>();
        try {
            args.addAll(makeBasicArgs(execute, srcFile, statusdir, completedUrl));
            args.add("--");
            args.add(appConf.hivePath());
            
            args.add("--service");
            args.add("cli");

            //the token file location as initial hiveconf arg
            args.add("--hiveconf");
            args.add(TempletonControllerJob.TOKEN_FILE_ARG_PLACEHOLDER);

            for (String prop : appConf.getStrings(AppConfig.HIVE_PROPS_NAME)) {
                args.add("--hiveconf");
                args.add(prop);
            }
            for (String prop : defines) {
                args.add("--hiveconf");
                args.add(prop);
            }
            if (TempletonUtils.isset(execute)) {
                args.add("-e");
                args.add(execute);
            } else if (TempletonUtils.isset(srcFile)) {
                args.add("-f");
                args.add(TempletonUtils.hadoopFsPath(srcFile, appConf, runAs)
                        .getName());
            }
        } catch (FileNotFoundException e) {
            throw new BadParam(e.getMessage());
        } catch (URISyntaxException e) {
            throw new BadParam(e.getMessage());
        }

        return args;
    }

    private List<String> makeBasicArgs(String execute, String srcFile,
                                       String statusdir, String completedUrl)
        throws URISyntaxException, FileNotFoundException, IOException,
        InterruptedException
    {
        ArrayList<String> args = new ArrayList<String>();

        ArrayList<String> allFiles = new ArrayList<String>();
        if (TempletonUtils.isset(srcFile))
            allFiles.add(TempletonUtils.hadoopFsFilename(srcFile, appConf,
                    runAs));

        args.addAll(makeLauncherArgs(appConf, statusdir, completedUrl, allFiles));

        args.add("-archives");
        args.add(appConf.hiveArchive());

        return args;
    }
}
