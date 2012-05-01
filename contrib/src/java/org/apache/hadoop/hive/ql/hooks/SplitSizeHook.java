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

package org.apache.hadoop.hive.ql.hooks;

import java.util.Random;
import java.util.List;
import java.io.Serializable;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContextImpl;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.HiveParser;

/**
 * Implementation of a compile time hook to set all split size parameters from
 * mapred.min.split.size if it is CombineHiveInputFormat
 * of queries
 */
public class SplitSizeHook extends AbstractSemanticAnalyzerHook {
  final static String COMBINE_HIVE_INPUT_FORMAT =
    "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat";
  final static String CONF_MAPRED_MAX_SPLIT_SIZE = "mapred.max.split.size";
  final static String CONF_MAPRED_MIN_SPLIT_PER_RACK = "mapred.min.split.size.per.rack";
  final static String CONF_MAPRED_MIN_SPLIT_PER_NODE = "mapred.min.split.size.per.node";

  // If input format is CombineHiveInputFormat, set all 3 related split size parameter to
  // mapred.min.split.size. mapred.max.split.size remains its old value if it is larger
  // than the new value.
  public ASTNode preAnalyze(
    HiveSemanticAnalyzerHookContext context,
    ASTNode ast) throws SemanticException {
    HiveSemanticAnalyzerHookContextImpl ctx = (HiveSemanticAnalyzerHookContextImpl)context;
    HiveConf conf = (HiveConf)ctx.getConf();

    String hiveInputFormat = conf.getVar(HiveConf.ConfVars.HIVEINPUTFORMAT);

    if (!hiveInputFormat.equals(COMBINE_HIVE_INPUT_FORMAT)) {
      return ast;
    }

    long mapredMinSplitSize = conf.getLongVar(HiveConf.ConfVars.MAPREDMINSPLITSIZE);

    conf.setLong(CONF_MAPRED_MIN_SPLIT_PER_NODE, mapredMinSplitSize);
    conf.setLong(CONF_MAPRED_MIN_SPLIT_PER_RACK, mapredMinSplitSize);
    long maxSplit = conf.getLong(CONF_MAPRED_MAX_SPLIT_SIZE, (long)-1);
    if (mapredMinSplitSize > maxSplit) {
      conf.setLong(CONF_MAPRED_MAX_SPLIT_SIZE, mapredMinSplitSize);
    }

    return ast;
  }

  // Nothing to do
  public void postAnalyze(
    HiveSemanticAnalyzerHookContext context,
    List<Task<? extends Serializable>> rootTasks) throws SemanticException {
    // no nothing
  }
}
