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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.parser.PartFilterParser;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;

/**
 * Utility functions for working with partition filter expressions
 */
public class PartFilterExprUtil {
  private static final Logger LOG = LoggerFactory.getLogger(PartFilterExprUtil.class.getName());


  public static ExpressionTree makeExpressionTree(PartitionExpressionProxy expressionProxy,
      byte[] expr, String defaultPartitionName, Configuration conf) throws MetaException {
    // We will try pushdown first, so make the filter. This will also validate the expression,
    // if serialization fails we will throw incompatible metastore error to the client.
    String filter = null;
    try {
      filter = expressionProxy.convertExprToFilter(expr, defaultPartitionName,
          conf.getBoolean(MetastoreConf.ConfVars.DECODE_FILTER_EXPRESSION_TO_STRING.getVarname(), false));
    } catch (MetaException ex) {
      // TODO MS-SPLIT - for now we have construct this by reflection because IMetaStoreClient
      // can't be
      // moved until after HiveMetaStore is moved, which can't be moved until this is moved.
      Class<? extends MetaException> exClass = JavaUtils.getClass(
          "org.apache.hadoop.hive.metastore.IMetaStoreClient$IncompatibleMetastoreException",
          MetaException.class);
      throw JavaUtils.newInstance(exClass, new Class<?>[]{String.class}, new Object[]{ex.getMessage()});
    }

    // Make a tree out of the filter.
    // TODO: this is all pretty ugly. The only reason we need all these transformations
    //       is to maintain support for simple filters for HCat users that query metastore.
    //       If forcing everyone to use thick client is out of the question, maybe we could
    //       parse the filter into standard hive expressions and not all this separate tree
    //       Filter.g stuff. That way this method and ...ByFilter would just be merged.
    return PartFilterExprUtil.makeExpressionTree(filter);
  }


  /**
   * Creates the proxy used to evaluate expressions. This is here to prevent circular
   * dependency - ql -&gt; metastore client &lt;-&gt; metastore server -&gt; ql. If server and
   * client are split, this can be removed.
   * @param conf Configuration.
   * @return The partition expression proxy.
   */
  public static PartitionExpressionProxy createExpressionProxy(Configuration conf) {
    String className = MetastoreConf.getVar(conf, ConfVars.EXPRESSION_PROXY_CLASS);
    try {
      @SuppressWarnings("unchecked")
      Class<? extends PartitionExpressionProxy> clazz =
          JavaUtils.getClass(className, PartitionExpressionProxy.class);
      return JavaUtils.newInstance(
          clazz, new Class<?>[0], new Object[0]);
    } catch (MetaException e) {
      if (e.getMessage().matches(".* class not found")) {
        // TODO MS-SPLIT For now if we cannot load the default PartitionExpressionForMetastore
        // class (since it's from ql) load the DefaultPartitionExpressionProxy, which just throws
        // UnsupportedOperationExceptions.  This allows existing Hive instances to work but also
        // allows us to instantiate the metastore stand alone for testing.  Not sure if this is
        // the best long term solution.
        return new DefaultPartitionExpressionProxy();
      }
      LOG.error("Error loading PartitionExpressionProxy", e);
      throw new RuntimeException("Error loading PartitionExpressionProxy: " + e.getMessage());
    }
  }

  /**
   * Makes expression tree out of expr.
   * @param filter Filter.
   * @return Expression tree. Null if there was an error.
   */
  private static ExpressionTree makeExpressionTree(String filter) throws MetaException {
    // TODO: ExprNodeDesc is an expression tree, we could just use that and be rid of Filter.g.
    if (filter == null || filter.isEmpty()) {
      return ExpressionTree.EMPTY_TREE;
    }
    LOG.debug("Filter specified is " + filter);
    ExpressionTree tree = null;
    try {
      tree = parseFilterTree(filter);
    } catch (MetaException ex) {
      LOG.info("Unable to make the expression tree from expression string ["
          + filter + "]" + ex.getMessage()); // Don't log the stack, this is normal.
    }
    return tree;
  }

  public static ExpressionTree parseFilterTree(String filter) throws MetaException {
    return PartFilterParser.parseFilter(filter);
  }
}
