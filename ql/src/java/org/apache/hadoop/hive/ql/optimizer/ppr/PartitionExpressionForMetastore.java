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

import org.apache.hadoop.hive.metastore.FileFormatProxy;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.api.MetaException;
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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMacro;
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
      LOG.error("Failed to deserialize the expression, fall back to deserializeUntrustedObjectFromKryo", ex);
      try {
        // The fallback must use the same untrusted-payload restrictions as the primary path:
        // these bytes come straight from a Thrift client.
        expr = SerializationUtilities.deserializeUntrustedObjectFromKryo(exprBytes, ExprNodeGenericFuncDesc.class);
      } catch (Exception e) {
        LOG.error("Failed to deserialize the expression", e);
        throw new MetaException("SerializationUtilities#deserializeObjectWithTypeInformation: " + ex.getMessage() +
            ", SerializationUtilities#deserializeUntrustedObjectFromKryo: " + e.getMessage());
      }
    }
    if (expr == null) {
      throw new MetaException("Failed to deserialize expression - ExprNodeDesc not present");
    }
    validateDeserializedExpr(expr);
    return expr;
  }

  /**
   * Rejects client-supplied expression graphs that would execute arbitrary code when the
   * metastore stringifies or evaluates them. The Kryo-level class allowlist already blocks
   * reflect()/reflect2(); a {@link GenericUDFBridge} instance is legitimate (it wraps builtin
   * old-style UDFs like year()), but it instantiates whatever class name its
   * {@code udfClassName} field carries, so that name must resolve to a real {@link UDF}.
   */
  private void validateDeserializedExpr(ExprNodeDesc expr) throws MetaException {
    if (expr instanceof ExprNodeGenericFuncDesc) {
      GenericUDF genericUDF = ((ExprNodeGenericFuncDesc) expr).getGenericUDF();
      if (genericUDF instanceof GenericUDFBridge) {
        String udfClassName = ((GenericUDFBridge) genericUDF).getUdfClassName();
        Class<?> udfClass;
        try {
          udfClass = Class.forName(udfClassName, false, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException | LinkageError e) {
          throw new MetaException("Unknown UDF class in partition filter expression: " + udfClassName);
        }
        if (!UDF.class.isAssignableFrom(udfClass)) {
          throw new MetaException("Class in partition filter expression is not a UDF: " + udfClassName);
        }
      }
      if (genericUDF instanceof GenericUDFMacro) {
        // a macro body is an expression graph of its own
        ExprNodeDesc body = ((GenericUDFMacro) genericUDF).getBody();
        if (body != null) {
          validateDeserializedExpr(body);
        }
      }
    }
    if (expr.getChildren() != null) {
      for (ExprNodeDesc child : expr.getChildren()) {
        validateDeserializedExpr(child);
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
    // These bytes also come straight from a Thrift client (get_file_metadata_by_expr), so they
    // get the same untrusted-payload restrictions as the partition filter expressions above.
    return SerializationUtilities.deserializeUntrustedObjectFromKryo(expr, SearchArgumentImpl.class);
  }
}
