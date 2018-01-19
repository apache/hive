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

package org.apache.hadoop.hive.ql.optimizer.ppr;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.FileFormatProxy;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.orc.OrcFileFormatProxy;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The basic implementation of PartitionExpressionProxy that uses ql package classes.
 */
public class PartitionExpressionForMetastore implements PartitionExpressionProxy {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionExpressionForMetastore.class);

  @Override
  public String convertExprToFilter(byte[] exprBytes) throws MetaException {
    return deserializeExpr(exprBytes).getExprString();
  }

  @Override
  public boolean filterPartitionsByExpr(List<FieldSchema> partColumns,
      byte[] exprBytes, String defaultPartitionName, List<String> partitionNames) throws MetaException {
    List<String> partColumnNames = new ArrayList<>();
    List<PrimitiveTypeInfo> partColumnTypeInfos = new ArrayList<>();
    for (FieldSchema fs : partColumns) {
      partColumnNames.add(fs.getName());
      partColumnTypeInfos.add(TypeInfoFactory.getPrimitiveTypeInfo(fs.getType()));
    }
    ExprNodeGenericFuncDesc expr = deserializeExpr(exprBytes);
    try {
      ExprNodeDescUtils.replaceEqualDefaultPartition(expr, defaultPartitionName);
    } catch (SemanticException ex) {
      LOG.error("Failed to replace default partition", ex);
      throw new MetaException(ex.getMessage());
    }
    try {
      long startTime = System.nanoTime(), len = partitionNames.size();
      boolean result = PartitionPruner.prunePartitionNames(
          partColumnNames, partColumnTypeInfos, expr, defaultPartitionName, partitionNames);
      double timeMs = (System.nanoTime() - startTime) / 1000000.0;
      LOG.debug("Pruning " + len + " partition names took " + timeMs + "ms");
      return result;
    } catch (HiveException ex) {
      LOG.error("Failed to apply the expression", ex);
      throw new MetaException(ex.getMessage());
    }
  }

  private ExprNodeGenericFuncDesc deserializeExpr(byte[] exprBytes) throws MetaException {
    ExprNodeGenericFuncDesc expr = null;
    try {
      expr = SerializationUtilities.deserializeExpressionFromKryo(exprBytes);
    } catch (Exception ex) {
      LOG.error("Failed to deserialize the expression", ex);
      throw new MetaException(ex.getMessage());
    }
    if (expr == null) {
      throw new MetaException("Failed to deserialize expression - ExprNodeDesc not present");
    }
    return expr;
  }

  @Override
  public FileFormatProxy getFileFormatProxy(FileMetadataExprType type) {
    switch (type) {
    case ORC_SARG: return new OrcFileFormatProxy();
    default: throw new RuntimeException("Unsupported format " + type);
    }
  }

  @Override
  public FileMetadataExprType getMetadataType(String inputFormat) {
    try {
      Class<?> ifClass = Class.forName(inputFormat);
      if (OrcInputFormat.class.isAssignableFrom(ifClass)) {
        return FileMetadataExprType.ORC_SARG;
      }
      return null;
    } catch (Throwable t) {
      LOG.warn("Can't create the class for input format " + inputFormat, t);
      return null;
    }
  }

  @Override
  public SearchArgument createSarg(byte[] expr) {
    return ConvertAstToSearchArg.create(expr);
  }
}
