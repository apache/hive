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
package org.apache.hadoop.hive.ql.plan.impala.funcmapper;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimestampString;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.type.RexNodeExprFactory;
import org.apache.hadoop.hive.ql.plan.impala.rex.ImpalaRexVisitor.ImpalaInferMappingRexVisitor;
import org.apache.hadoop.hive.ql.plan.impala.ImpalaQueryContext;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TColumnValue;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor for {@link RexNode} based on Impala semantics.
 */
public class ImpalaRexExecutorImpl extends RexExecutorImpl {

  private static final Logger LOG = LoggerFactory.getLogger(ImpalaRexExecutorImpl.class);

  private final ImpalaQueryContext queryContext;

  public ImpalaRexExecutorImpl(ImpalaQueryContext queryContext) {
    super(null);
    this.queryContext = queryContext;
  }

  @Override
  public void reduce(RexBuilder rexBuilder, List<RexNode> constExps, List<RexNode> reducedValues) {
    Analyzer analyzer = queryContext.getAnalyzer();
    for (RexNode constExp : constExps) {
      // No need to do any reducing if this is a literal or a cast of a literal.  The
      // reduction of a cast will be done through Calcite's rules.
      if (RexUtil.isLiteral(constExp, true)) {
        reducedValues.add(constExp);
        continue;
      }
      try {
        ImpalaInferMappingRexVisitor visitor = new ImpalaInferMappingRexVisitor(
          analyzer, Lists.newArrayList(), rexBuilder);
        Expr expr = constExp.accept(visitor);
        TColumnValue colValue = FeSupport.EvalExprWithoutRowBounded(expr,
            queryContext.getTQueryCtx(), 65536);
        RexNode returnedRexNode = getRexNodeFromColumnValue(rexBuilder, constExp, colValue);
        reducedValues.add(returnedRexNode);
      } catch (Exception e) {
        LOG.warn("Could not do constant folding for expression " + constExp, e);
        reducedValues.add(constExp);
      }
    }
  }

  private RexNode getRexNodeFromColumnValue(RexBuilder builder, RexNode constExp,
      TColumnValue colVal) throws HiveException {
    RelDataType returnType = constExp.getType();
    if (colVal.isSetBool_val()) {
      return builder.makeLiteral(colVal.bool_val, returnType, true);
    } else if (colVal.isSetByte_val()) {
      return builder.makeLiteral(colVal.byte_val, returnType, true);
    } else if (colVal.isSetShort_val()) {
      return builder.makeLiteral(colVal.short_val, returnType, true);
    } else if (colVal.isSetInt_val()) {
      return builder.makeLiteral(colVal.int_val, returnType, true);
    } else if (colVal.isSetLong_val()) {
      return builder.makeLiteral(colVal.long_val, returnType, true);
    } else if (colVal.isSetString_val()) {
      if (returnType.getSqlTypeName() == SqlTypeName.TIMESTAMP) {
        return builder.makeTimestampLiteral(new TimestampString(colVal.string_val),
            RelDataType.PRECISION_NOT_SPECIFIED);
      }
      // decimal is in string val, strings are in binaryVal
      return builder.makeLiteral(new BigDecimal(colVal.string_val), returnType, true);
    } else if (colVal.isSetBinary_val()) {
      byte[] bytes = new byte[colVal.binary_val.remaining()];
      colVal.binary_val.get(bytes);

      // Converting strings between the BE/FE does not work properly for the
      // extended ASCII characters above 127. Bail in such cases to avoid
      // producing incorrect results.
      for (byte b: bytes) {
        if (b < 0) {
          return constExp;
        }
      }

      try {
        // CDPD-14514: Investigate handling of unicode strings.
        String newString = new String(bytes, "US-ASCII");
        NlsString newHiveNlsString =
            RexNodeExprFactory.makeHiveUnicodeString(newString);
        return builder.makeLiteral(newHiveNlsString, returnType, true);
      } catch (UnsupportedEncodingException e) {
        LOG.debug("Could not interpret return value for " + colVal);
        throw new HiveException(e);
      }
    }
    return builder.makeNullLiteral(returnType);
  }

}
