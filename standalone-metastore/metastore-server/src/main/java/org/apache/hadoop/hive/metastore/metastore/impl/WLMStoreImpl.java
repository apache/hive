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

package org.apache.hadoop.hive.metastore.metastore.impl;

import com.google.common.collect.Sets;

import javax.jdo.Query;
import java.sql.SQLIntegrityConstraintViolationException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.QueryWrapper;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMPoolTrigger;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMResourcePlanStatus;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metastore.RawStoreAware;
import org.apache.hadoop.hive.metastore.metastore.iface.WLMStore;
import org.apache.hadoop.hive.metastore.model.MRuntimeStat;
import org.apache.hadoop.hive.metastore.model.MWMMapping;
import org.apache.hadoop.hive.metastore.model.MWMPool;
import org.apache.hadoop.hive.metastore.model.MWMResourcePlan;
import org.apache.hadoop.hive.metastore.model.MWMTrigger;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

public class WLMStoreImpl extends RawStoreAware implements WLMStore {
  private static final Logger LOG = LoggerFactory.getLogger(WLMStoreImpl.class);
  private static final DateTimeFormatter YMDHMS_FORMAT = DateTimeFormatter.ofPattern(
      "yyyy_MM_dd_HH_mm_ss");

  private void checkForConstraintException(Exception e, String msg) throws AlreadyExistsException {
    if (getConstraintException(e) != null) {
      LOG.error(msg, e);
      throw new AlreadyExistsException(msg);
    }
  }

  private Throwable getConstraintException(Throwable t) {
    while (t != null) {
      if (t instanceof SQLIntegrityConstraintViolationException) {
        return t;
      }
      t = t.getCause();
    }
    return null;
  }

  @Override
  public void createResourcePlan(
      WMResourcePlan resourcePlan, String copyFromName, int defaultPoolSize)
      throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException {
    String rpName = normalizeIdentifier(resourcePlan.getName());
    if (rpName.isEmpty()) {
      throw new InvalidObjectException("Resource name cannot be empty.");
    }
    MWMResourcePlan rp = null;
    if (copyFromName == null) {
      Integer queryParallelism = null;
      if (resourcePlan.isSetQueryParallelism()) {
        queryParallelism = resourcePlan.getQueryParallelism();
        if (queryParallelism <= 0) {
          throw new InvalidObjectException("Query parallelism should be positive.");
        }
      }
      rp = new MWMResourcePlan(rpName, queryParallelism, MWMResourcePlan.Status.DISABLED);
    } else {
      rp = new MWMResourcePlan(rpName, null, MWMResourcePlan.Status.DISABLED);
    }
    rp.setNs(resourcePlan.getNs());
    try {
      pm.makePersistent(rp);
      if (copyFromName != null) {
        String ns = getNsOrDefault(resourcePlan.getNs());
        MWMResourcePlan copyFrom = getMWMResourcePlan(copyFromName, ns, false);
        if (copyFrom == null) {
          throw new NoSuchObjectException(copyFromName + " in " + ns);
        }
        copyRpContents(rp, copyFrom);
      } else {
        // TODO: ideally, this should be moved outside to HiveMetaStore to be shared between
        //       all the RawStore-s. Right now there's no method to create a pool.
        if (defaultPoolSize > 0) {
          MWMPool defaultPool = new MWMPool(rp, "default", 1.0, defaultPoolSize, null);
          pm.makePersistent(defaultPool);
          rp.setPools(Sets.newHashSet(defaultPool));
          rp.setDefaultPool(defaultPool);
        }
      }
    } catch (InvalidOperationException e) {
      throw new RuntimeException(e);
    } catch (Exception e) {
      checkForConstraintException(e, "Resource plan already exists: " + rpName);
      throw e;
    }
  }

  private void copyRpContents(MWMResourcePlan dest, MWMResourcePlan src) {
    dest.setQueryParallelism(src.getQueryParallelism());
    dest.setNs(src.getNs());
    Map<String, MWMPool> pools = new HashMap<>();
    Map<String, Set<MWMPool>> triggersToPools = new HashMap<>();
    for (MWMPool copyPool : src.getPools()) {
      MWMPool pool = new MWMPool(dest, copyPool.getPath(), copyPool.getAllocFraction(),
          copyPool.getQueryParallelism(), copyPool.getSchedulingPolicy());
      pm.makePersistent(pool);
      pools.put(copyPool.getPath(), pool);
      if (copyPool.getTriggers() != null) {
        for (MWMTrigger trigger : copyPool.getTriggers()) {
          Set<MWMPool> p2t = triggersToPools.get(trigger.getName());
          if (p2t == null) {
            p2t = new HashSet<>();
            triggersToPools.put(trigger.getName(), p2t);
          }
          p2t.add(pool);
          pool.setTriggers(new HashSet<>());
        }
      }
    }
    dest.setPools(new HashSet<>(pools.values()));
    if (src.getDefaultPool() != null) {
      dest.setDefaultPool(pools.get(src.getDefaultPool().getPath()));
    }
    Set<MWMMapping> mappings = new HashSet<>();
    for (MWMMapping copyMapping : src.getMappings()) {
      MWMPool pool = null;
      if (copyMapping.getPool() != null) {
        pool = pools.get(copyMapping.getPool().getPath());
      }
      MWMMapping mapping = new MWMMapping(dest, copyMapping.getEntityType(),
          copyMapping.getEntityName(), pool, copyMapping.getOrdering());
      pm.makePersistent(mapping);
      mappings.add(mapping);
    }
    dest.setMappings(mappings);
    Set<MWMTrigger> triggers = new HashSet<>();
    for (MWMTrigger copyTrigger : src.getTriggers()) {
      Set<MWMPool> p2t = triggersToPools.get(copyTrigger.getName());
      if (p2t == null) {
        p2t = new HashSet<>();
      }
      MWMTrigger trigger = new MWMTrigger(dest, copyTrigger.getName(),
          copyTrigger.getTriggerExpression(), copyTrigger.getActionExpression(), p2t,
          copyTrigger.getIsInUnmanaged());
      pm.makePersistent(trigger);
      for (MWMPool pool : p2t) {
        pool.getTriggers().add(trigger);
      }
      triggers.add(trigger);
    }
    dest.setTriggers(triggers);
  }

  private WMResourcePlan fromMResourcePlan(MWMResourcePlan mplan) {
    if (mplan == null) {
      return null;
    }
    WMResourcePlan rp = new WMResourcePlan();
    rp.setName(mplan.getName());
    rp.setNs(mplan.getNs());
    rp.setStatus(WMResourcePlanStatus.valueOf(mplan.getStatus().name()));
    if (mplan.getQueryParallelism() != null) {
      rp.setQueryParallelism(mplan.getQueryParallelism());
    }
    if (mplan.getDefaultPool() != null) {
      rp.setDefaultPoolPath(mplan.getDefaultPool().getPath());
    }
    return rp;
  }

  private WMFullResourcePlan fullFromMResourcePlan(MWMResourcePlan mplan) {
    if (mplan == null) {
      return null;
    }
    WMFullResourcePlan rp = new WMFullResourcePlan();
    rp.setPlan(fromMResourcePlan(mplan));
    for (MWMPool mPool : mplan.getPools()) {
      rp.addToPools(fromMPool(mPool, mplan.getName()));
      for (MWMTrigger mTrigger : mPool.getTriggers()) {
        rp.addToPoolTriggers(new WMPoolTrigger(mPool.getPath(), mTrigger.getName()));
      }
    }
    for (MWMTrigger mTrigger : mplan.getTriggers()) {
      rp.addToTriggers(fromMWMTrigger(mTrigger, mplan.getName()));
    }
    for (MWMMapping mMapping : mplan.getMappings()) {
      rp.addToMappings(fromMMapping(mMapping, mplan.getName()));
    }
    return rp;
  }

  private WMPool fromMPool(MWMPool mPool, String rpName) {
    WMPool result = new WMPool(rpName, mPool.getPath());
    assert mPool.getAllocFraction() != null;
    result.setAllocFraction(mPool.getAllocFraction());
    assert mPool.getQueryParallelism() != null;
    result.setQueryParallelism(mPool.getQueryParallelism());
    result.setSchedulingPolicy(mPool.getSchedulingPolicy());
    result.setNs(mPool.getResourcePlan().getNs());
    return result;
  }

  private WMMapping fromMMapping(MWMMapping mMapping, String rpName) {
    WMMapping result = new WMMapping(rpName,
        mMapping.getEntityType().toString(), mMapping.getEntityName());
    if (mMapping.getPool() != null) {
      result.setPoolPath(mMapping.getPool().getPath());
    }
    if (mMapping.getOrdering() != null) {
      result.setOrdering(mMapping.getOrdering());
    }
    result.setNs(mMapping.getResourcePlan().getNs());
    return result;
  }

  private String getNsOrDefault(String ns) {
    // This is only needed for old clients not setting NS in requests.
    // Not clear how to handle this... this is properly a HS2 config but metastore needs its default
    // value for backward compat, and we don't want it configurable separately because it's also
    // used in upgrade scripts, were it cannot be configured.
    return normalizeIdentifier(ns == null ? "default" : ns);
  }

  @Override
  public WMFullResourcePlan getResourcePlan(String name, String ns) throws NoSuchObjectException {
    try {
      return fullFromMResourcePlan(getMWMResourcePlan(name, ns, false));
    } catch (InvalidOperationException e) {
      // Should not happen, edit check is false.
      throw new RuntimeException(e);
    }
  }

  private MWMResourcePlan getMWMResourcePlan(String name, String ns, boolean editCheck)
      throws NoSuchObjectException, InvalidOperationException {
    return getMWMResourcePlan(name, ns, editCheck, true);
  }

  private MWMResourcePlan getMWMResourcePlan(String name, String ns, boolean editCheck, boolean mustExist)
      throws NoSuchObjectException, InvalidOperationException {
    MWMResourcePlan resourcePlan;

    name = normalizeIdentifier(name);
    Query query = createGetResourcePlanQuery();
    ns = getNsOrDefault(ns);
    resourcePlan = (MWMResourcePlan) query.execute(name, ns);
    if (resourcePlan != null) {
      pm.retrieve(resourcePlan);
    }
    if (mustExist && resourcePlan == null) {
      throw new NoSuchObjectException("There is no resource plan named: " + name + " in " + ns);
    }
    if (editCheck && resourcePlan != null
        && resourcePlan.getStatus() != MWMResourcePlan.Status.DISABLED) {
      throw new InvalidOperationException("Resource plan must be disabled to edit it.");
    }
    return resourcePlan;
  }

  private Query createGetResourcePlanQuery() {
    Query query = pm.newQuery(MWMResourcePlan.class, "name == rpname && ns == nsname");
    query.declareParameters("java.lang.String rpname, java.lang.String nsname");
    query.setUnique(true);
    return query;
  }

  @Override
  public List<WMResourcePlan> getAllResourcePlans(String ns) throws MetaException {
    List<WMResourcePlan> resourcePlans = new ArrayList();
    Query query = pm.newQuery(MWMResourcePlan.class, "ns == nsname");
    query.declareParameters("java.lang.String nsname");
    List<MWMResourcePlan> mplans = (List<MWMResourcePlan>) query.execute(getNsOrDefault(ns));
    pm.retrieveAll(mplans);
    if (mplans != null) {
      for (MWMResourcePlan mplan : mplans) {
        resourcePlans.add(fromMResourcePlan(mplan));
      }
    }
    return resourcePlans;
  }

  @Override
  public WMFullResourcePlan alterResourcePlan(String name, String ns, WMNullableResourcePlan changes,
      boolean canActivateDisabled, boolean canDeactivate, boolean isReplace)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    name = name == null ? null : normalizeIdentifier(name);
    if (isReplace && name == null) {
      throw new InvalidOperationException("Cannot replace without specifying the source plan");
    }
    // This method only returns the result when activating a resource plan.
    // We could also add a boolean flag to be specified by the caller to see
    // when the result might be needed.
    WMFullResourcePlan result = null;
    try {
      if (isReplace) {
        result = handleAlterReplace(name, ns, changes);
      } else {
        result = handleSimpleAlter(name, ns, changes, canActivateDisabled, canDeactivate);
      }
      return result;
    } catch (Exception e) {
      checkForConstraintException(e, "Resource plan name should be unique: ");
      throw e;
    }
  }

  private WMFullResourcePlan handleSimpleAlter(String name, String ns, WMNullableResourcePlan changes,
      boolean canActivateDisabled, boolean canDeactivate)
      throws InvalidOperationException, NoSuchObjectException, MetaException {
    MWMResourcePlan plan = name == null ? getActiveMWMResourcePlan(ns)
        : getMWMResourcePlan(name, ns, !changes.isSetStatus());
    boolean hasNsChange = changes.isSetNs() && !changes.getNs().equals(getNsOrDefault(plan.getNs()));
    if (hasNsChange) {
      throw new InvalidOperationException("Cannot change ns; from " + getNsOrDefault(plan.getNs())
          + " to " + changes.getNs());
    }
    boolean hasNameChange = changes.isSetName() && !changes.getName().equals(name);
    // Verify that field changes are consistent with what Hive does. Note: we could handle this.
    if (changes.isSetIsSetQueryParallelism()
        || changes.isSetIsSetDefaultPoolPath() || hasNameChange) {
      if (changes.isSetStatus()) {
        throw new InvalidOperationException("Cannot change values during status switch.");
      } else if (plan.getStatus() != MWMResourcePlan.Status.DISABLED) {
        throw new InvalidOperationException("Resource plan must be disabled to edit it.");
      }
    }

    // Handle rename and other changes.
    if (changes.isSetName()) {
      String newName = normalizeIdentifier(changes.getName());
      if (newName.isEmpty()) {
        throw new InvalidOperationException("Cannot rename to empty value.");
      }
      if (!newName.equals(plan.getName())) {
        plan.setName(newName);
      }
    }
    if (changes.isSetIsSetQueryParallelism() && changes.isIsSetQueryParallelism()) {
      if (changes.isSetQueryParallelism()) {
        if (changes.getQueryParallelism() <= 0) {
          throw new InvalidOperationException("queryParallelism should be positive.");
        }
        plan.setQueryParallelism(changes.getQueryParallelism());
      } else {
        plan.setQueryParallelism(null);
      }
    }
    if (changes.isSetIsSetDefaultPoolPath() && changes.isIsSetDefaultPoolPath()) {
      if (changes.isSetDefaultPoolPath()) {
        MWMPool pool = getPool(plan, changes.getDefaultPoolPath());
        plan.setDefaultPool(pool);
      } else {
        plan.setDefaultPool(null);
      }
    }

    // Handle the status change.
    if (changes.isSetStatus()) {
      return switchStatus(name, plan,
          changes.getStatus().name(), canActivateDisabled, canDeactivate);
    }
    return null;
  }

  private WMFullResourcePlan handleAlterReplace(String name, String ns, WMNullableResourcePlan changes)
      throws InvalidOperationException, NoSuchObjectException, MetaException {
    // Verify that field changes are consistent with what Hive does. Note: we could handle this.
    if (changes.isSetQueryParallelism() || changes.isSetDefaultPoolPath()) {
      throw new InvalidOperationException("Cannot change values during replace.");
    }
    boolean isReplacingSpecific = changes.isSetName();
    boolean isReplacingActive = (changes.isSetStatus()
        && changes.getStatus() == WMResourcePlanStatus.ACTIVE);
    if (isReplacingActive == isReplacingSpecific) {
      throw new InvalidOperationException("Must specify a name, or the active plan; received "
          + changes.getName() + ", " + (changes.isSetStatus() ? changes.getStatus() : null));
    }
    if (name == null) {
      throw new InvalidOperationException("Invalid replace - no name specified");
    }
    ns = getNsOrDefault(ns);
    MWMResourcePlan replacedPlan = isReplacingSpecific
        ? getMWMResourcePlan(changes.getName(), ns, false) : getActiveMWMResourcePlan(ns);
    MWMResourcePlan plan = getMWMResourcePlan(name, ns, false);

    if (replacedPlan.getName().equals(plan.getName())) {
      throw new InvalidOperationException("A plan cannot replace itself");
    }
    String oldNs = getNsOrDefault(replacedPlan.getNs()), newNs = getNsOrDefault(plan.getNs());
    if (!oldNs.equals(newNs)) {
      throw new InvalidOperationException("Cannot change the namespace; replacing "
          + oldNs + " with " + newNs);
    }

    // We will inherit the name and status from the plan we are replacing.
    String newName = replacedPlan.getName();
    int i = 0;
    String copyName = generateOldPlanName(newName, i);
    while (true) {
      MWMResourcePlan dup = getMWMResourcePlan(copyName, ns, false, false);
      if (dup == null) {
        break;
      }
      // Note: this can still conflict with parallel transactions. We do not currently handle
      //       parallel changes from two admins (by design :().
      copyName = generateOldPlanName(newName, ++i);
    }
    replacedPlan.setName(copyName);
    plan.setName(newName);
    plan.setStatus(replacedPlan.getStatus());
    replacedPlan.setStatus(MWMResourcePlan.Status.DISABLED);
    // TODO: add a configurable option to skip the history and just drop it?
    return plan.getStatus() == MWMResourcePlan.Status.ACTIVE ? fullFromMResourcePlan(plan) : null;
  }

  private String generateOldPlanName(String newName, int i) {
    if (MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST)) {
      // Do not use datetime in tests to avoid result changes.
      return newName + "_old_" + i;
    } else {
      return newName + "_old_"
          + LocalDateTime.now().format(YMDHMS_FORMAT) + (i == 0 ? "" : ("_" + i));
    }
  }

  @Override
  public WMFullResourcePlan getActiveResourcePlan(String ns) throws MetaException {
    // Note: fullFromMResroucePlan needs to be called inside the txn, otherwise we could have
    //       deduplicated this with getActiveMWMResourcePlan.
    WMFullResourcePlan result = null;
    Query query = createActivePlanQuery();
    MWMResourcePlan mResourcePlan = (MWMResourcePlan) query.execute(
        MWMResourcePlan.Status.ACTIVE.toString(), getNsOrDefault(ns));
    if (mResourcePlan != null) {
      result = fullFromMResourcePlan(mResourcePlan);
    }
    return result;
  }

  private MWMResourcePlan getActiveMWMResourcePlan(String ns) {
    MWMResourcePlan result = null;
    Query query = createActivePlanQuery();
    result = (MWMResourcePlan) query.execute(
        MWMResourcePlan.Status.ACTIVE.toString(), getNsOrDefault(ns));
    if (result != null) {
      pm.retrieve(result);
    }
    return result;
  }

  private Query createActivePlanQuery() {
    Query query = pm.newQuery(MWMResourcePlan.class, "status == activeStatus && ns == nsname");
    query.declareParameters("java.lang.String activeStatus, java.lang.String nsname");
    query.setUnique(true);
    return query;
  }

  private WMFullResourcePlan switchStatus(String name, MWMResourcePlan mResourcePlan, String status,
      boolean canActivateDisabled, boolean canDeactivate) throws InvalidOperationException {
    MWMResourcePlan.Status currentStatus = mResourcePlan.getStatus();
    MWMResourcePlan.Status newStatus = null;
    try {
      newStatus = MWMResourcePlan.Status.valueOf(status);
    } catch (IllegalArgumentException e) {
      throw new InvalidOperationException("Invalid status: " + status);
    }

    if (newStatus == currentStatus) {
      return null;
    }

    boolean doActivate = false, doValidate = false;
    switch (currentStatus) {
    case ACTIVE: // No status change for active resource plan, first activate another plan.
      if (!canDeactivate) {
        throw new InvalidOperationException(
            "Resource plan " + name
                + " is active; activate another plan first, or disable workload management.");
      }
      break;
    case DISABLED:
      assert newStatus == MWMResourcePlan.Status.ACTIVE || newStatus == MWMResourcePlan.Status.ENABLED;
      doValidate = true;
      doActivate = (newStatus == MWMResourcePlan.Status.ACTIVE);
      if (doActivate && !canActivateDisabled) {
        throw new InvalidOperationException("Resource plan " + name
            + " is disabled and should be enabled before activation (or in the same command)");
      }
      break;
    case ENABLED:
      if (newStatus == MWMResourcePlan.Status.DISABLED) {
        mResourcePlan.setStatus(newStatus);
        return null; // A simple case.
      }
      assert newStatus == MWMResourcePlan.Status.ACTIVE;
      doActivate = true;
      break;
    default: throw new AssertionError("Unexpected status " + currentStatus);
    }
    if (doValidate) {
      // Note: this may use additional inputs from the caller, e.g. maximum query
      // parallelism in the cluster based on physical constraints.
      WMValidateResourcePlanResponse response = getResourcePlanErrors(mResourcePlan);
      if (!response.getErrors().isEmpty()) {
        throw new InvalidOperationException(
            "ResourcePlan: " + name + " is invalid: " + response.getErrors());
      }
    }
    if (doActivate) {
      // Deactivate currently active resource plan.
      deactivateActiveResourcePlan(mResourcePlan.getNs());
      mResourcePlan.setStatus(newStatus);
      return fullFromMResourcePlan(mResourcePlan);
    } else {
      mResourcePlan.setStatus(newStatus);
    }
    return null;
  }

  private void deactivateActiveResourcePlan(String ns) {
    Query query = createActivePlanQuery();
    MWMResourcePlan mResourcePlan = (MWMResourcePlan) query.execute(
        MWMResourcePlan.Status.ACTIVE.toString(), getNsOrDefault(ns));
    // We may not have an active resource plan in the start.
    if (mResourcePlan != null) {
      mResourcePlan.setStatus(MWMResourcePlan.Status.ENABLED);
    }
  }

  private static class PoolData {
    double totalChildrenAllocFraction = 0;
    boolean found = false;
    boolean hasChildren = false;
  }

  private PoolData getPoolData(Map<String, PoolData> poolInfo, String poolPath) {
    PoolData poolData = poolInfo.get(poolPath);
    if (poolData == null) {
      poolData = new PoolData();
      poolInfo.put(poolPath, poolData);
    }
    return poolData;
  }

  private WMValidateResourcePlanResponse getResourcePlanErrors(MWMResourcePlan mResourcePlan) {
    WMValidateResourcePlanResponse response = new WMValidateResourcePlanResponse();
    response.setErrors(new ArrayList());
    response.setWarnings(new ArrayList());
    Integer rpParallelism = mResourcePlan.getQueryParallelism();
    if (rpParallelism != null && rpParallelism < 1) {
      response.addToErrors("Query parallelism should for resource plan be positive. Got: " +
          rpParallelism);
    }
    int totalQueryParallelism = 0;
    Map<String, PoolData> poolInfo = new HashMap<>();
    for (MWMPool pool : mResourcePlan.getPools()) {
      PoolData currentPoolData = getPoolData(poolInfo, pool.getPath());
      currentPoolData.found = true;
      String parent = getParentPath(pool.getPath(), "");
      PoolData parentPoolData = getPoolData(poolInfo, parent);
      parentPoolData.hasChildren = true;
      parentPoolData.totalChildrenAllocFraction += pool.getAllocFraction();
      if (pool.getQueryParallelism() != null && pool.getQueryParallelism() < 1) {
        response.addToErrors("Invalid query parallelism for pool: " + pool.getPath());
      } else {
        totalQueryParallelism += pool.getQueryParallelism();
      }
      if (!MetaStoreUtils.isValidSchedulingPolicy(pool.getSchedulingPolicy())) {
        response.addToErrors("Invalid scheduling policy " + pool.getSchedulingPolicy() +
            " for pool: " + pool.getPath());
      }
    }
    if (rpParallelism != null) {
      if (rpParallelism < totalQueryParallelism) {
        response.addToErrors("Sum of all pools' query parallelism: " + totalQueryParallelism  +
            " exceeds resource plan query parallelism: " + rpParallelism);
      } else if (rpParallelism != totalQueryParallelism) {
        response.addToWarnings("Sum of all pools' query parallelism: " + totalQueryParallelism  +
            " is less than resource plan query parallelism: " + rpParallelism);
      }
    }
    for (Map.Entry<String, PoolData> entry : poolInfo.entrySet()) {
      final PoolData poolData = entry.getValue();
      final boolean isRoot = entry.getKey().isEmpty();
      // Special case for root parent
      if (isRoot) {
        poolData.found = true;
        if (!poolData.hasChildren) {
          response.addToErrors("Root has no children");
          // TODO: change fractions to use decimal? somewhat brittle
        } else if (Math.abs(1.0 - poolData.totalChildrenAllocFraction) > 0.00001) {
          response.addToErrors("Sum of root children pools' alloc fraction should be 1.0 got: " +
              poolData.totalChildrenAllocFraction + " for pool: " + entry.getKey());
        }
      }
      if (!poolData.found) {
        response.addToErrors("Pool does not exists but has children: " + entry.getKey());
      }
      if (poolData.hasChildren) {

        if (!isRoot && (poolData.totalChildrenAllocFraction - 1.0) > 0.00001) {
          response.addToErrors("Sum of children pools' alloc fraction should be less than 1 got: "
              + poolData.totalChildrenAllocFraction + " for pool: " + entry.getKey());
        }
      }
    }
    // trigger and action expressions are not validated here, since counters are not
    // available and grammar check is there in the language itself.
    return response;
  }

  @Override
  public WMValidateResourcePlanResponse validateResourcePlan(String name, String ns)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    name = normalizeIdentifier(name);
    Query query = createGetResourcePlanQuery();
    MWMResourcePlan mResourcePlan = (MWMResourcePlan) query.execute(name, ns);
    if (mResourcePlan == null) {
      throw new NoSuchObjectException("Cannot find resourcePlan: " + name + " in " + ns);
    }
    WMValidateResourcePlanResponse result = getResourcePlanErrors(mResourcePlan);
    return result;
  }

  @Override
  public void dropResourcePlan(String name, String ns) throws NoSuchObjectException, MetaException {
    name = normalizeIdentifier(name);
    Query query = createGetResourcePlanQuery();
    MWMResourcePlan resourcePlan = (MWMResourcePlan) query.execute(name, getNsOrDefault(ns));
    if (resourcePlan == null) {
      throw new NoSuchObjectException("There is no resource plan named: " + name + " in " + ns);
    }
    pm.retrieve(resourcePlan); // TODO: why do some codepaths call retrieve and some don't?
    if (resourcePlan.getStatus() == MWMResourcePlan.Status.ACTIVE) {
      throw new MetaException("Cannot drop an active resource plan");
    }
    // First, drop all the dependencies.
    resourcePlan.setDefaultPool(null);
    pm.deletePersistentAll(resourcePlan.getTriggers());
    pm.deletePersistentAll(resourcePlan.getMappings());
    pm.deletePersistentAll(resourcePlan.getPools());
    pm.deletePersistent(resourcePlan);
  }

  @Override
  public void createWMTrigger(WMTrigger trigger)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
      MetaException {
    try {
      MWMResourcePlan resourcePlan = getMWMResourcePlan(
          trigger.getResourcePlanName(), trigger.getNs(), true);
      MWMTrigger mTrigger = new MWMTrigger(resourcePlan,
          normalizeIdentifier(trigger.getTriggerName()), trigger.getTriggerExpression(),
          trigger.getActionExpression(), null,
          trigger.isSetIsInUnmanaged() && trigger.isIsInUnmanaged());
      pm.makePersistent(mTrigger);
    } catch (Exception e) {
      checkForConstraintException(e, "Trigger already exists, use alter: ");
      throw e;
    }
  }

  @Override
  public void alterWMTrigger(WMTrigger trigger)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    MWMResourcePlan resourcePlan = getMWMResourcePlan(
        trigger.getResourcePlanName(), trigger.getNs(), true);
    MWMTrigger mTrigger = getTrigger(resourcePlan, trigger.getTriggerName());
    // Update the object.
    if (trigger.isSetTriggerExpression()) {
      mTrigger.setTriggerExpression(trigger.getTriggerExpression());
    }
    if (trigger.isSetActionExpression()) {
      mTrigger.setActionExpression(trigger.getActionExpression());
    }
    if (trigger.isSetIsInUnmanaged()) {
      mTrigger.setIsInUnmanaged(trigger.isIsInUnmanaged());
    }
  }

  private MWMTrigger getTrigger(MWMResourcePlan resourcePlan, String triggerName)
      throws NoSuchObjectException {
    triggerName = normalizeIdentifier(triggerName);
    Query // Get the MWMTrigger object from DN
        query = pm.newQuery(MWMTrigger.class, "resourcePlan == rp && name == triggerName");
    query.declareParameters("MWMResourcePlan rp, java.lang.String triggerName");
    query.setUnique(true);
    MWMTrigger mTrigger = (MWMTrigger) query.execute(resourcePlan, triggerName);
    if (mTrigger == null) {
      throw new NoSuchObjectException("Cannot find trigger with name: " + triggerName);
    }
    pm.retrieve(mTrigger);
    return mTrigger;
  }

  @Override
  public void dropWMTrigger(String resourcePlanName, String triggerName, String ns)
      throws NoSuchObjectException, InvalidOperationException, MetaException  {
    resourcePlanName = normalizeIdentifier(resourcePlanName);
    triggerName = normalizeIdentifier(triggerName);

    Query query = null;
    MWMResourcePlan resourcePlan = getMWMResourcePlan(resourcePlanName, ns, true);
    query = pm.newQuery(MWMTrigger.class, "resourcePlan == rp && name == triggerName");
    query.declareParameters("MWMResourcePlan rp, java.lang.String triggerName");
    if (query.deletePersistentAll(resourcePlan, triggerName) != 1) {
      throw new NoSuchObjectException("Cannot delete trigger: " + triggerName);
    }
  }

  @Override
  public List<WMTrigger> getTriggersForResourcePlan(String resourcePlanName, String ns)
      throws NoSuchObjectException, MetaException {
    List<WMTrigger> triggers = new ArrayList();
    Query query = null;
    MWMResourcePlan resourcePlan;
    try {
      resourcePlan = getMWMResourcePlan(resourcePlanName, ns, false);
    } catch (InvalidOperationException e) {
      // Should not happen, edit check is false.
      throw new RuntimeException(e);
    }
    query = pm.newQuery(MWMTrigger.class, "resourcePlan == rp");
    query.declareParameters("MWMResourcePlan rp");
    List<MWMTrigger> mTriggers = (List<MWMTrigger>) query.execute(resourcePlan);
    pm.retrieveAll(mTriggers);
    if (mTriggers != null) {
      for (MWMTrigger trigger : mTriggers) {
        triggers.add(fromMWMTrigger(trigger, resourcePlanName));
      }
    }
    return triggers;
  }

  private WMTrigger fromMWMTrigger(MWMTrigger mTrigger, String resourcePlanName) {
    WMTrigger trigger = new WMTrigger();
    trigger.setResourcePlanName(resourcePlanName);
    trigger.setTriggerName(mTrigger.getName());
    trigger.setTriggerExpression(mTrigger.getTriggerExpression());
    trigger.setActionExpression(mTrigger.getActionExpression());
    trigger.setIsInUnmanaged(mTrigger.getIsInUnmanaged());
    trigger.setNs(mTrigger.getResourcePlan().getNs());
    return trigger;
  }

  @Override
  public void createPool(WMPool pool) throws AlreadyExistsException, NoSuchObjectException,
      InvalidOperationException, MetaException {
    try {
      MWMResourcePlan resourcePlan = getMWMResourcePlan(
          pool.getResourcePlanName(), pool.getNs(), true);

      if (!poolParentExists(resourcePlan, pool.getPoolPath())) {
        throw new NoSuchObjectException("Pool path is invalid, the parent does not exist");
      }
      String policy = pool.getSchedulingPolicy();
      if (!MetaStoreUtils.isValidSchedulingPolicy(policy)) {
        throw new InvalidOperationException("Invalid scheduling policy " + policy);
      }
      MWMPool mPool = new MWMPool(resourcePlan, pool.getPoolPath(), pool.getAllocFraction(),
          pool.getQueryParallelism(), policy);
      pm.makePersistent(mPool);
    } catch (Exception e) {
      checkForConstraintException(e, "Pool already exists: ");
      throw e;
    }
  }

  @Override
  public void alterPool(WMNullablePool pool, String poolPath) throws AlreadyExistsException,
      NoSuchObjectException, InvalidOperationException, MetaException {
    MWMResourcePlan resourcePlan = getMWMResourcePlan(
        pool.getResourcePlanName(), pool.getNs(), true);
    MWMPool mPool = getPool(resourcePlan, poolPath);
    pm.retrieve(mPool);
    if (pool.isSetAllocFraction()) {
      mPool.setAllocFraction(pool.getAllocFraction());
    }
    if (pool.isSetQueryParallelism()) {
      mPool.setQueryParallelism(pool.getQueryParallelism());
    }
    if (pool.isSetIsSetSchedulingPolicy() && pool.isIsSetSchedulingPolicy()) {
      if (pool.isSetSchedulingPolicy()) {
        String policy = pool.getSchedulingPolicy();
        if (!MetaStoreUtils.isValidSchedulingPolicy(policy)) {
          throw new InvalidOperationException("Invalid scheduling policy " + policy);
        }
        mPool.setSchedulingPolicy(pool.getSchedulingPolicy());
      } else {
        mPool.setSchedulingPolicy(null);
      }
    }
    if (pool.isSetPoolPath() && !pool.getPoolPath().equals(mPool.getPath())) {
      moveDescendents(resourcePlan, mPool.getPath(), pool.getPoolPath());
      mPool.setPath(pool.getPoolPath());
    }
  }

  private MWMPool getPool(MWMResourcePlan resourcePlan, String poolPath)
      throws NoSuchObjectException {
    poolPath = normalizeIdentifier(poolPath);
    Query query = pm.newQuery(MWMPool.class, "resourcePlan == rp && path == poolPath");
    query.declareParameters("MWMResourcePlan rp, java.lang.String poolPath");
    query.setUnique(true);
    MWMPool mPool = (MWMPool) query.execute(resourcePlan, poolPath);
    if (mPool == null) {
      throw new NoSuchObjectException("Cannot find pool: " + poolPath);
    }
    pm.retrieve(mPool);
    return mPool;
  }

  private void moveDescendents(MWMResourcePlan resourcePlan, String path, String newPoolPath)
      throws NoSuchObjectException {
    if (!poolParentExists(resourcePlan, newPoolPath)) {
      throw new NoSuchObjectException("Pool path is invalid, the parent does not exist");
    }
    Query query = pm.newQuery(MWMPool.class, "resourcePlan == rp && path.startsWith(poolPath)");
    query.declareParameters("MWMResourcePlan rp, java.lang.String poolPath");
    List<MWMPool> descPools = (List<MWMPool>) query.execute(resourcePlan, path + ".");
    pm.retrieveAll(descPools);
    for (MWMPool pool : descPools) {
      pool.setPath(newPoolPath + pool.getPath().substring(path.length()));
    }
  }

  private String getParentPath(String poolPath, String defValue) {
    int idx = poolPath.lastIndexOf('.');
    if (idx == -1) {
      return defValue;
    }
    return poolPath.substring(0, idx);
  }

  private boolean poolParentExists(MWMResourcePlan resourcePlan, String poolPath) {
    String parent = getParentPath(poolPath, null);
    if (parent == null) {
      return true;
    }
    try {
      getPool(resourcePlan, parent);
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  @Override
  public void dropWMPool(String resourcePlanName, String poolPath, String ns)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    poolPath = normalizeIdentifier(poolPath);
    Query query = null;
    try {
      MWMResourcePlan resourcePlan = getMWMResourcePlan(resourcePlanName, ns, true);
      if (resourcePlan.getDefaultPool() != null &&
          resourcePlan.getDefaultPool().getPath().equals(poolPath)) {
        throw new InvalidOperationException("Cannot drop default pool of a resource plan");
      }
      if (poolHasChildren(resourcePlan, poolPath)) {
        throw new InvalidOperationException("Cannot drop a pool that has child pools");
      }
      query = pm.newQuery(MWMPool.class, "resourcePlan == rp && path.startsWith(poolPath)");
      query.declareParameters("MWMResourcePlan rp, java.lang.String poolPath");
      if (query.deletePersistentAll(resourcePlan, poolPath) != 1) {
        throw new NoSuchObjectException("Cannot delete pool: " + poolPath);
      }
    } catch(Exception e) {
      if (getConstraintException(e) != null) {
        throw new InvalidOperationException("Please remove all mappings for this pool.");
      }
      throw e;
    }
  }

  private boolean poolHasChildren(MWMResourcePlan resourcePlan, String poolPath) {
    Query query = pm.newQuery(MWMPool.class, "resourcePlan == rp && path.startsWith(poolPath)");
    query.declareParameters("MWMResourcePlan rp, java.lang.String poolPath");
    query.setResult("count(this)");
    query.setUnique(true);
    Long count = (Long) query.execute(resourcePlan, poolPath + ".");
    return count != null && count > 0;
  }

  @Override
  public void createOrUpdateWMMapping(WMMapping mapping, boolean update)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
      MetaException {
    MWMMapping.EntityType entityType = MWMMapping.EntityType.valueOf(mapping.getEntityType().trim().toUpperCase());
    String entityName = normalizeIdentifier(mapping.getEntityName());
    Query query = null;
    MWMResourcePlan resourcePlan = getMWMResourcePlan(
        mapping.getResourcePlanName(), mapping.getNs(), true);
    MWMPool pool = null;
    if (mapping.isSetPoolPath()) {
      pool = getPool(resourcePlan, mapping.getPoolPath());
    }
    if (!update) {
      MWMMapping mMapping = new MWMMapping(resourcePlan, entityType, entityName, pool,
          mapping.getOrdering());
      pm.makePersistent(mMapping);
    } else {
      query = pm.newQuery(MWMMapping.class, "resourcePlan == rp && entityType == type " +
          "&& entityName == name");
      query.declareParameters(
          "MWMResourcePlan rp, java.lang.String type, java.lang.String name");
      query.setUnique(true);
      MWMMapping mMapping = (MWMMapping) query.execute(
          resourcePlan, entityType.toString(), entityName);
      if (mMapping == null) {
        throw new NoSuchObjectException("Cannot find mapping for " + entityType + ":" + entityName);
      }
      mMapping.setPool(pool);
    }
  }

  @Override
  public void dropWMMapping(WMMapping mapping)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    String entityType = mapping.getEntityType().trim().toUpperCase();
    String entityName = normalizeIdentifier(mapping.getEntityName());
    Query query = null;
    MWMResourcePlan resourcePlan = getMWMResourcePlan(
        mapping.getResourcePlanName(), mapping.getNs(), true);
    query = pm.newQuery(MWMMapping.class,
        "resourcePlan == rp && entityType == type && entityName == name");
    query.declareParameters("MWMResourcePlan rp, java.lang.String type, java.lang.String name");
    if (query.deletePersistentAll(resourcePlan, entityType, entityName) != 1) {
      throw new NoSuchObjectException("Cannot delete mapping.");
    }
  }

  @Override
  public void createWMTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath, String ns) throws AlreadyExistsException, NoSuchObjectException,
      InvalidOperationException, MetaException {
    MWMResourcePlan resourcePlan = getMWMResourcePlan(resourcePlanName, ns, true);
    MWMPool pool = getPool(resourcePlan, poolPath);
    MWMTrigger trigger = getTrigger(resourcePlan, triggerName);
    pool.getTriggers().add(trigger);
    trigger.getPools().add(pool);
    pm.makePersistent(pool);
    pm.makePersistent(trigger);
  }

  @Override
  public void dropWMTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath, String ns) throws NoSuchObjectException, InvalidOperationException, MetaException {
    MWMResourcePlan resourcePlan = getMWMResourcePlan(resourcePlanName, ns, true);
    MWMPool pool = getPool(resourcePlan, poolPath);
    MWMTrigger trigger = getTrigger(resourcePlan, triggerName);
    pool.getTriggers().remove(trigger);
    trigger.getPools().remove(pool);
    pm.makePersistent(pool);
    pm.makePersistent(trigger);
  }

  @Override
  public void addRuntimeStat(RuntimeStat stat) throws MetaException {
    LOG.debug("runtimeStat: {}", stat);
    MRuntimeStat mStat = MRuntimeStat.fromThrift(stat);
    pm.makePersistent(mStat);
  }

  @Override
  public int deleteRuntimeStats(int maxRetainSecs) throws MetaException {
    if (maxRetainSecs < 0) {
      LOG.warn("runtime stats retention is disabled");
      return 0;
    }
    int maxCreateTime = (int) (System.currentTimeMillis() / 1000) - maxRetainSecs;
    Query q = pm.newQuery(MRuntimeStat.class);
    q.setFilter("createTime <= maxCreateTime");
    q.declareParameters("int maxCreateTime");
    long deleted = q.deletePersistentAll(maxCreateTime);
    return (int) deleted;
  }

  @Override
  public List<RuntimeStat> getRuntimeStats(int maxEntries, int maxCreateTime) throws MetaException {
    return getMRuntimeStats(maxEntries, maxCreateTime);
  }

  private List<RuntimeStat> getMRuntimeStats(int maxEntries, int maxCreateTime) {
    try (QueryWrapper query = new QueryWrapper(pm.newQuery(MRuntimeStat.class))) {
      query.setOrdering("createTime descending");
      if (maxCreateTime > 0) {
        query.setFilter("createTime < " + maxCreateTime);
      }
      if (maxEntries < 0) {
        maxEntries = Integer.MAX_VALUE;
      }
      List<RuntimeStat> ret = new ArrayList<>();
      List<MRuntimeStat> res = (List<MRuntimeStat>) query.execute();
      int totalEntries = 0;
      for (MRuntimeStat mRuntimeStat : res) {
        pm.retrieve(mRuntimeStat);
        totalEntries += mRuntimeStat.getWeight();
        ret.add(MRuntimeStat.toThrift(mRuntimeStat));
        if (totalEntries >= maxEntries) {
          break;
        }
      }
      return ret;
    }
  }

  private Configuration conf;
  @Override
  public void setBaseStore(RawStore store) {
    super.setBaseStore(store);
    this.conf = store.getConf();
  }
}
