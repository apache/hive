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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hive.common.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Optimizer that updates TableScanOperators' Table-references with properties that might be
 * updated/pre-fetched by initializing the table's SerDe.
 * E.g. AvroSerDes can now prefetch schemas from schema-urls and update the table-properties directly.
 */
class TablePropertyEnrichmentOptimizer extends Transform {

  private static Logger LOG = LoggerFactory.getLogger(TablePropertyEnrichmentOptimizer.class);

  private static class WalkerCtx implements NodeProcessorCtx {

    Configuration conf;
    Set<String> serdeClassesUnderConsideration = Sets.newHashSet();

    WalkerCtx(Configuration conf) {
      this.conf = conf;
      serdeClassesUnderConsideration.addAll(
          Arrays.asList( HiveConf.getVar(conf,
                                         HiveConf.ConfVars.HIVE_OPTIMIZE_TABLE_PROPERTIES_FROM_SERDE_LIST)
                                 .split(",")));

      if (LOG.isDebugEnabled()) {
        LOG.debug("TablePropertyEnrichmentOptimizer considers these SerDe classes:");
        for (String className : serdeClassesUnderConsideration) {
          LOG.debug(className);
        }
      }
    }
  }

  /**
   * Retrieves the table properties as well as the properties from Serde.
   */
  private static Map<String, String> getTableParameters(Table table) {
    Map<String, String> originalTableParameters = new HashMap<>(table.getParameters());
    Properties tableMetadata = MetaStoreUtils.getTableMetadata(table);
    for (String property : tableMetadata.stringPropertyNames()) {
      if (!originalTableParameters.containsKey(property)) {
        originalTableParameters.put(property, tableMetadata.getProperty(property));
      }
    }
    return originalTableParameters;
  }

  private static class Processor implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
      TableScanOperator tsOp = (TableScanOperator) nd;
      WalkerCtx context = (WalkerCtx)procCtx;
      TableScanDesc tableScanDesc = tsOp.getConf();
      Table table = tsOp.getConf().getTableMetadata().getTTable();

      Map<String, String> originalTableParameters = getTableParameters(table);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Original Table parameters: " + originalTableParameters);
      }
      Properties clonedTableParameters = new Properties();
      clonedTableParameters.putAll(originalTableParameters);

      String deserializerClassName = null;
      try {
        deserializerClassName = tableScanDesc.getTableMetadata().getSd().getSerdeInfo().getSerializationLib();
        Deserializer deserializer = ReflectionUtil.newInstance(
            context.conf.getClassByName(deserializerClassName)
                .asSubclass(Deserializer.class),
            context.conf);

        if (context.serdeClassesUnderConsideration.contains(deserializerClassName)) {
          deserializer.initialize(context.conf, clonedTableParameters);
          LOG.debug("SerDe init succeeded for class: " + deserializerClassName);
          for (Map.Entry property : clonedTableParameters.entrySet()) {
            if (!property.getValue().equals(originalTableParameters.get(property.getKey()))) {
              LOG.debug("Resolving changed parameters! key=" + property.getKey() + ", value=" + property.getValue());
              table.getParameters().put((String) property.getKey(), (String) property.getValue());
            }
          }
        }
        else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping prefetch for " + deserializerClassName);
          }
        }
      }
      catch(Throwable t) {
        LOG.error("SerDe init failed for SerDe class==" + deserializerClassName
                  + ". Didn't change table-properties", t);
      }

      return nd;
    }
  }

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    LOG.info("TablePropertyEnrichmentOptimizer::transform().");

    Map<SemanticRule, SemanticNodeProcessor> opRules = Maps.newLinkedHashMap();
    opRules.put(new RuleRegExp("R1", TableScanOperator.getOperatorName() + "%"),
        new Processor());

    WalkerCtx context = new WalkerCtx(pctx.getConf());
    SemanticDispatcher disp = new DefaultRuleDispatcher(null, opRules, context);

    List<Node> topNodes = Lists.newArrayList();
    topNodes.addAll(pctx.getTopOps().values());

    SemanticGraphWalker walker = new PreOrderWalker(disp);
    walker.startWalking(topNodes, null);

    LOG.info("TablePropertyEnrichmentOptimizer::transform() complete!");
    return pctx;
  }
}
