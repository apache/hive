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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.exec.ExecuteException;

/**
 * Submit a streaming job to the MapReduce queue.  Really just a front
 end to the JarDelegator.
 *
 * This is the backend of the mapreduce/streaming web service.
 */
public class StreamingDelegator extends LauncherDelegator {
  public StreamingDelegator(AppConfig appConf) {
    super(appConf);
  }

  public EnqueueBean run(String user, Map<String, Object> userArgs,
               List<String> inputs, String output,
               String mapper, String reducer,
               List<String> files, List<String> defines,
               List<String> cmdenvs,
               List<String> jarArgs,
               String statusdir,
               String callback,
               String completedUrl,
               boolean enableLog,
               JobType jobType)
    throws NotAuthorizedException, BadParam, BusyException, QueueException,
    ExecuteException, IOException, InterruptedException {
    List<String> args = makeArgs(inputs, output, mapper, reducer,
      files, defines, cmdenvs, jarArgs);

    JarDelegator d = new JarDelegator(appConf);
    return d.run(user, userArgs,
      appConf.streamingJar(), null,
      null, null, args, defines,
      statusdir, callback, completedUrl, enableLog, jobType);
  }

  private List<String> makeArgs(List<String> inputs,
                  String output,
                  String mapper,
                  String reducer,
                  List<String> files,
                  List<String> defines,
                  List<String> cmdenvs,
                  List<String> jarArgs) {
    ArrayList<String> args = new ArrayList<String>();
    for (String input : inputs) {
      args.add("-input");
      args.add(input);
    }
    args.add("-output");
    args.add(output);
    args.add("-mapper");
    args.add(mapper);
    args.add("-reducer");
    args.add(reducer);

    for (String f : files)
      args.add("-file" + f);
    for (String d : defines)
      args.add("-D" + d);
    for (String e : cmdenvs)
      args.add("-cmdenv" + e);
    args.addAll(jarArgs);

    return args;
  }
}
