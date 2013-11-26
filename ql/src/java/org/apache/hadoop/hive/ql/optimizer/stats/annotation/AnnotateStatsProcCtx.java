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

package org.apache.hadoop.hive.ql.optimizer.stats.annotation;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.Statistics;

public class AnnotateStatsProcCtx implements NodeProcessorCtx {

  private ParseContext pctx;
  private HiveConf conf;
  private Statistics andExprStats = null;

  public AnnotateStatsProcCtx(ParseContext pctx) {
    this.setParseContext(pctx);
    if(pctx != null) {
      this.setConf(pctx.getConf());
    } else {
      this.setConf(null);
    }
  }

  public HiveConf getConf() {
    return conf;
  }

  public void setConf(HiveConf conf) {
    this.conf = conf;
  }

  public ParseContext getParseContext() {
    return pctx;
  }

  public void setParseContext(ParseContext pctx) {
    this.pctx = pctx;
  }

  public Statistics getAndExprStats() {
    return andExprStats;
  }

  public void setAndExprStats(Statistics andExprStats) {
    this.andExprStats = andExprStats;
  }

}
