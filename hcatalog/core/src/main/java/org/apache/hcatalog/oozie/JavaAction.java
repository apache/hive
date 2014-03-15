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
package org.apache.hcatalog.oozie;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;

/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.oozie.JavaAction} instead
 */
public class JavaAction {

  public static void main(String[] args) throws Exception {

    HiveConf conf = new HiveConf();
    conf.addResource(new Path("file:///", System.getProperty("oozie.action.conf.xml")));
    conf.setVar(ConfVars.SEMANTIC_ANALYZER_HOOK, HCatSemanticAnalyzer.class.getName());
    conf.setBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL, true);
    SessionState.start(new CliSessionState(conf));
    new CliDriver().processLine(args[0]);
  }

}
