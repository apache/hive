// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.hadoop.hive.ql.plan.impala;

import com.google.common.collect.Lists;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.TableName;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.FeDataSource;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HdfsCachePool;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TGetPartitionStatsResponse;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.util.PatternMatcher;

import java.util.List;
import java.util.Set;

/**
 * Impala relies on the Analyzer for various semantic analysis of expressions
 * and plan nodes. Since Hive has already done most of this analysis we want
 * a basic analyzer that allows for analyzing/validating the final physical plan nodes, slots
 * and expressions. This BasicAnalyzer extends the Analyzer and overrides a few methods.
 */
public class ImpalaBasicAnalyzer extends Analyzer {

  private StmtMetadataLoader.StmtTableCache stmtTableCache;

  public ImpalaBasicAnalyzer(StmtMetadataLoader.StmtTableCache stmtTableCache,
      TQueryCtx queryCtx,
        AuthorizationFactory authzFactory,
        List<TNetworkAddress> hostLocations) {
    super(stmtTableCache, queryCtx, authzFactory, null);
    this.stmtTableCache = stmtTableCache;

    this.getHostIndex().populate(hostLocations);
  }

  /**
   * The createAuxEqPredicate is called internally by Impala to
   * create new predicates based on transitive equality. At this point,
   * we expect that such predicates should have already been created
   * by Hive and hence we make this a no-op
   */
  @Override
  public void createAuxEqPredicate(Expr lhs, Expr rhs) {
  }

  /**
   * Method to allow Calcite to sneak in a catalog table definition
   * into the stmttablecache analyzer.
   */
  public void setTable(TableName tableName, FeTable table) {
    stmtTableCache.tables.put(tableName, table);
  }

  /**
   * No need to worry about bound predicates because Calcite takes
   * care of this.
   */
  @Override
  public List<Expr> getBoundPredicates(TupleId destTid, Set<SlotId> ignoreSlots,
      boolean markAssigned) {
    return Lists.newArrayList();
  }

  /**
   * No need to worry about unassigned conjuncts because Calcite takes
   * care of this.
   */
  @Override
  public List<Expr> getUnassignedConjuncts(
      List<TupleId> tupleIds, boolean inclOjConjuncts) {
    return Lists.newArrayList();
  }

  /**
   * No need to worry about assigned conjuncts because Calcite takes
   * care of this.
   */
  @Override
  public void markConjunctsAssigned(List<Expr> conjuncts) {
  }

  /**
   * No need to worry about equivalent conjuncts because Calcite takes
   * care of this.
   */
  @Override
  public void createEquivConjuncts(List<TupleId> lhsTids,
      List<TupleId> rhsTids, List<BinaryPredicate> conjuncts) {
  }

  /**
   * No need to worry about equivalent conjuncts because Calcite takes
   * care of this.
   */
  @Override
  public <T extends Expr> void createEquivConjuncts(TupleId tid, List<T> conjuncts,
      Set<SlotId> ignoreSlots) {
  }

  /**
   * Override getDb method.  Within Impala, this gets called when creating new builtin
   * expressions.  For instance, within MultiAggregateInfo, the "aggif" Expr gets created
   * and analyzed.  When that happens, the Analyzer makes a call to this "getDb()" for the
   * Builtins database.  So we return the BuiltinsDb instance.  We pass in "true" for the
   * parameter to ensure that it will not try to load in the BuiltinsDb from the shared library.
   */
  @Override
  public FeDb getDb(String dbName, Privilege privilege, boolean throwIfDoesNotExist)
      throws AnalysisException {
    if (dbName.equals(BuiltinsDb.NAME)) {
      return BuiltinsDb.getInstance(true);
    }
    throw new AnalysisException("Db " + dbName + " not found.");
  }

  /**
   * Calcite materializes all slot descriptors within every node, so we can
   * always set the field as materialized.
   */
  @Override
  public SlotDescriptor addSlotDescriptor(TupleDescriptor tupleDesc) {
    SlotDescriptor result = super.addSlotDescriptor(tupleDesc);
    result.setIsMaterialized(true);
    return result;
  }

  /**
   * TODO: CDPD-8182: This needs to be looked at in more detail for phase 3.
   */
  @Override
  public boolean setsHaveValueTransfer(List<Expr> l1, List<Expr> l2, boolean mutual) {
      return false;
  }
}
