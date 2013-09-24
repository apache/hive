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
package org.apache.hive.hcatalog.templeton.tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 * This is the base class for the output parser.
 * Output parser will parse the output of a Pig/
 * Hive/Hadoop or other job and extract jobid.
 * Note Hadoop jobid extract is rely on the API
 * Hadoop application submitting the job. Different
 * api will result in different console output. The
 * jobid extraction logic is not always working in
 * this case
 */
abstract class JobIDParser {
  private String statusdir;
  private Configuration conf;
  
  JobIDParser(String statusdir, Configuration conf) {
    this.statusdir = statusdir;
    this.conf = conf;
  }
  
  private BufferedReader openStatusFile(String fname) throws IOException {
    Path p = new Path(statusdir, fname);
    FileSystem fs = p.getFileSystem(conf);
    BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(p)));
    return in;
  }

  private List<String> findJobID(BufferedReader in, String patternAsString) throws IOException {
    Pattern pattern = Pattern.compile(patternAsString);
    Matcher matcher;
    String line;
    List<String> jobs = new ArrayList<String>();
    while ((line=in.readLine())!=null) {
      matcher = pattern.matcher(line);
      if (matcher.find()) {
        String jobid = matcher.group(1);
        jobs.add(jobid);
      }
    }
    return jobs;
  }

  abstract List<String> parseJobID() throws IOException;

  List<String> parseJobID(String fname, String pattern) throws IOException {
    BufferedReader in=null;
    try {
      in = openStatusFile(fname);
      List<String> jobs = findJobID(in, pattern);
      return jobs;
    } catch (IOException e) {
      throw e;
    } finally {
      if (in!=null) {
        in.close();
      }
    }
  }
}
