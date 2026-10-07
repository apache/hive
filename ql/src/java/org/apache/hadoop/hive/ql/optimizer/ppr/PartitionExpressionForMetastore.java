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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.optimizer.ppr;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.FileFormatProxy;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.io.orc.OrcFileFormatProxy;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFInFile;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMacro;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFReflect;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFReflect2;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The basic implementation of PartitionExpressionProxy that uses ql package classes.
 */
public class PartitionExpressionForMetastore implements PartitionExpressionProxy {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionExpressionForMetastore.class);

  /**
   * Classes that are never acceptable in a partition expression.
   * GenericUDFReflect, GenericUDFReflect2, and GenericUDFInFile are typically disallowed in a secure environment.
   * This set should be in sync with the denylist in
   * {@link org.apache.hadoop.hive.ql.security.authorization.plugin.SettableConfigUpdater}.
   */
  private static final Set<Class<? extends GenericUDF>> DENIED_UDFS = Set.of(
      GenericUDFReflect.class,
      GenericUDFReflect2.class,
      GenericUDFInFile.class
  );

  @Override
  public String convertExprToFilter(byte[] exprBytes, String defaultPartitionName, boolean decodeFilterExpToStr)
      throws MetaException {
    ExprNodeDesc expr;
    try {
      expr = deserializeExpr(exprBytes);
    } catch (MetaException e) {
      // When deserializeExpr fails try to deserialize th exprBytes to string based on the
      // flag decodeFilterExpToStr. This usually happens when MSCK command is run with partition
      // filters. When MSCK command tries to drop the partitions, The string partition filter is serialized
      // to byte array and during deserialization we need to construct the filter string back.
      if (decodeFilterExpToStr) {
        return new String(exprBytes, StandardCharsets.UTF_8);
      }
      throw new MetaException(e.getMessage());
    }
    if ((defaultPartitionName != null) && (!defaultPartitionName.isEmpty())) {
      try {
        ExprNodeDescUtils.replaceNullFiltersWithDefaultPartition(expr, defaultPartitionName);
      } catch (SemanticException ex) {
        LOG.error("Failed to replace \"is null\" and \"is not null\" expression with default partition", ex);
        throw new MetaException(ex.getMessage());
      }
    }
    return expr.getExprString();
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
    ExprNodeDesc expr = deserializeExpr(exprBytes);
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

  private ExprNodeDesc deserializeExpr(byte[] exprBytes) throws MetaException {
    ExprNodeDesc expr = null;
    try {
      expr = SerializationUtilities.deserializeObjectWithTypeInformation(exprBytes, true);
    } catch (Exception ex) {
      LOG.error("Failed to deserialize the expression, fall back to deserializeObjectFromKryo", ex);
      try {
        expr = SerializationUtilities.deserializeObjectFromKryo(exprBytes, ExprNodeGenericFuncDesc.class);
      } catch (Exception e) {
        LOG.error("Failed to deserialize the expression", e);
        throw new MetaException("SerializationUtilities#deserializeObjectWithTypeInformation: " + ex.getMessage() +
            ", SerializationUtilities#deserializeObjectFromKryo: " + e.getMessage());
      }
    }
    if (expr == null) {
      throw new MetaException("Failed to deserialize expression - ExprNodeDesc not present");
    }
    validateDeserializedExpr(expr);
    return expr;
  }

  /**
   * Rejects client-supplied expression graphs that would execute arbitrary code when the metastore stringifies or
   * evaluates them.
   */
  private void validateDeserializedExpr(ExprNodeDesc expr) throws MetaException {
    if (expr instanceof ExprNodeGenericFuncDesc exprNodeGenericFuncDesc) {
      validateDeserializedExprNodeGenericFuncDesc(exprNodeGenericFuncDesc);
    }
    if (expr.getChildren() != null) {
      for (ExprNodeDesc child : expr.getChildren()) {
        validateDeserializedExpr(child);
      }
    }
  }

  private void validateDeserializedExprNodeGenericFuncDesc(ExprNodeGenericFuncDesc expr) throws MetaException {
    GenericUDF genericUDF = expr.getGenericUDF();
    if (DENIED_UDFS.contains(genericUDF.getClass())) {
      throw new MetaException(genericUDF.getUdfName() + " is not allowed in partition expressions");
    }
    if (!FunctionRegistry.isBuiltInFuncExpr(expr)) {
      throw new MetaException("Only built-in UDFs are allowed in partition expressions");
    }
    if (genericUDF instanceof GenericUDFBridge genericUDFBridge) {
      Class<? extends UDF> udfClass = genericUDFBridge.getUdfClass();
      if (!UDF.class.isAssignableFrom(udfClass)) {
        throw new MetaException("Class in partition filter expression is not a UDF: " + udfClass);
      }
    }
    if (genericUDF instanceof GenericUDFMacro genericUDFMacro) {
      // a macro body is an expression graph of its own
      ExprNodeDesc body = genericUDFMacro.getBody();
      if (body != null) {
        validateDeserializedExpr(body);
      }
    }
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
    return SerializationUtilities.deserializeObjectFromKryo(expr, SearchArgumentImpl.class);
  }
}
