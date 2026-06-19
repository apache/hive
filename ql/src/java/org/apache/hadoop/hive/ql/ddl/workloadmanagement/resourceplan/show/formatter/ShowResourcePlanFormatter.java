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

package org.apache.hadoop.hive.ql.ddl.workloadmanagement.resourceplan.show.formatter;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMPoolTrigger;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Formats SHOW RESOURCE PLAN(S) results.
 */
public abstract class ShowResourcePlanFormatter {
  public static ShowResourcePlanFormatter getFormatter(HiveConf conf) {
    if (MetaDataFormatUtils.isJson(conf)) {
      return new JsonShowResourcePlanFormatter();
    } else {
      return new TextShowResourcePlanFormatter();
    }
  }

  public abstract void showResourcePlans(DataOutputStream out, List<WMResourcePlan> resourcePlans) throws HiveException;

  public abstract void showFullResourcePlan(DataOutputStream out, WMFullResourcePlan resourcePlan) throws HiveException;

  static void formatFullRP(RPFormatter rpFormatter, WMFullResourcePlan fullResourcePlan) throws HiveException {
    try {
      WMResourcePlan plan = fullResourcePlan.getPlan();
      Integer parallelism = plan.isSetQueryParallelism() ? plan.getQueryParallelism() : null;
      String defaultPool = plan.isSetDefaultPoolPath() ? plan.getDefaultPoolPath() : null;
      rpFormatter.startRP(plan.getName(), "status", plan.getStatus().toString(),
           "parallelism", parallelism, "defaultPool", defaultPool);
      rpFormatter.startPools();

      PoolTreeNode root = PoolTreeNode.makePoolTree(fullResourcePlan);
      root.sortChildren();
      for (PoolTreeNode pool : root.children) {
        pool.writePoolTreeNode(rpFormatter);
      }

      rpFormatter.endPools();
      rpFormatter.endRP();
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  /**
   * A n-ary tree for the pools, each node contains a pool and its children.
   */
  private static final class PoolTreeNode {
    private String nonPoolName;
    private WMPool pool;
    private boolean isDefault;

    private final List<PoolTreeNode> children = new ArrayList<>();
    private final List<WMTrigger> triggers = new ArrayList<>();
    private final Map<String, List<String>> mappings = new HashMap<>();

    private PoolTreeNode() {
    }

    private void writePoolTreeNode(RPFormatter rpFormatter) throws IOException {
      if (pool != null) {
        String path = pool.getPoolPath();
        int idx = path.lastIndexOf('.');
        if (idx != -1) {
          path = path.substring(idx + 1);
        }
        Double allocFraction = pool.getAllocFraction();
        String schedulingPolicy = pool.isSetSchedulingPolicy() ? pool.getSchedulingPolicy() : null;
        Integer parallelism = pool.getQueryParallelism();
        rpFormatter.startPool(path, "allocFraction", allocFraction,
            "schedulingPolicy", schedulingPolicy, "parallelism", parallelism);
      } else {
        rpFormatter.startPool(nonPoolName);
      }

      rpFormatter.startTriggers();
      for (WMTrigger trigger : triggers) {
        rpFormatter.formatTrigger(trigger.getTriggerName(), trigger.getActionExpression(),
            trigger.getTriggerExpression());
      }
      rpFormatter.endTriggers();

      rpFormatter.startMappings();
      for (Map.Entry<String, List<String>> mappingsOfType : mappings.entrySet()) {
        mappingsOfType.getValue().sort(String::compareTo);
        rpFormatter.formatMappingType(mappingsOfType.getKey(), mappingsOfType.getValue());
      }
      if (isDefault) {
        rpFormatter.formatMappingType("default", Lists.<String>newArrayList());
      }
      rpFormatter.endMappings();

      rpFormatter.startPools();
      for (PoolTreeNode node : children) {
        node.writePoolTreeNode(rpFormatter);
      }
      rpFormatter.endPools();
      rpFormatter.endPool();
    }

    private void sortChildren() {
      children.sort((PoolTreeNode p1, PoolTreeNode p2) -> {
        if (p2.pool == null) {
          return (p1.pool == null) ? 0 : -1;
        }
        if (p1.pool == null) {
          return 1;
        }
        return Double.compare(p2.pool.getAllocFraction(), p1.pool.getAllocFraction());
      });

      for (PoolTreeNode child : children) {
        child.sortChildren();
      }

      triggers.sort((WMTrigger t1, WMTrigger t2) -> t1.getTriggerName().compareTo(t2.getTriggerName()));
    }

    static PoolTreeNode makePoolTree(WMFullResourcePlan fullRp) {
      PoolTreeNode root = new PoolTreeNode();
      Map<String, PoolTreeNode> poolMap = new HashMap<>();
      buildPoolTree(fullRp, root, poolMap);

      Map<String, WMTrigger> triggerMap = new HashMap<>();
      List<WMTrigger> unmanagedTriggers = new ArrayList<>();
      Set<WMTrigger> unusedTriggers = new HashSet<>();
      sortTriggers(fullRp, poolMap, triggerMap, unmanagedTriggers, unusedTriggers);

      Map<String, List<String>> unmanagedMappings = new HashMap<>();
      Map<String, List<String>> invalidMappings = new HashMap<>();
      sortMappings(fullRp, poolMap, unmanagedMappings, invalidMappings);

      addNonPoolNodes(root, poolMap, unmanagedTriggers, unusedTriggers, unmanagedMappings, invalidMappings);

      return root;
    }

    private static void buildPoolTree(WMFullResourcePlan fullRp, PoolTreeNode root, Map<String, PoolTreeNode> poolMap) {
      for (WMPool pool : fullRp.getPools()) {
        // Create or add node for current pool.
        String path = pool.getPoolPath();
        PoolTreeNode curr = poolMap.get(path);
        if (curr == null) {
          curr = new PoolTreeNode();
          poolMap.put(path, curr);
        }
        curr.pool = pool;
        if (fullRp.getPlan().isSetDefaultPoolPath() && fullRp.getPlan().getDefaultPoolPath().equals(path)) {
          curr.isDefault = true;
        }

        // Add this node to the parent node.
        int ind = path.lastIndexOf('.');
        PoolTreeNode parent;
        if (ind == -1) {
          parent = root;
        } else {
          String parentPath = path.substring(0, ind);
          parent = poolMap.get(parentPath);
          if (parent == null) {
            parent = new PoolTreeNode();
            poolMap.put(parentPath, parent);
          }
        }
        parent.children.add(curr);
      }
    }

    private static void sortTriggers(WMFullResourcePlan fullRp, Map<String, PoolTreeNode> poolMap,
        Map<String, WMTrigger> triggerMap, List<WMTrigger> unmanagedTriggers, Set<WMTrigger> unusedTriggers) {
      if (fullRp.isSetTriggers()) {
        for (WMTrigger trigger : fullRp.getTriggers()) {
          triggerMap.put(trigger.getTriggerName(), trigger);
          if (trigger.isIsInUnmanaged()) {
            unmanagedTriggers.add(trigger);
          } else {
            unusedTriggers.add(trigger);
          }
        }
      }
      if (fullRp.isSetPoolTriggers()) {
        for (WMPoolTrigger pool2Trigger : fullRp.getPoolTriggers()) {
          PoolTreeNode node = poolMap.get(pool2Trigger.getPool());
          WMTrigger trigger = triggerMap.get(pool2Trigger.getTrigger());
          if (node == null || trigger == null) {
            throw new IllegalStateException("Invalid trigger to pool: " + pool2Trigger.getPool() +
                ", " + pool2Trigger.getTrigger());
          }
          unusedTriggers.remove(trigger);
          node.triggers.add(trigger);
        }
      }
    }

    private static void sortMappings(WMFullResourcePlan fullRp, Map<String, PoolTreeNode> poolMap,
        Map<String, List<String>> unmanagedMappings, Map<String, List<String>> invalidMappings) {
      if (fullRp.isSetMappings()) {
        for (WMMapping mapping : fullRp.getMappings()) {
          if (mapping.isSetPoolPath()) {
            PoolTreeNode destNode = poolMap.get(mapping.getPoolPath());
            addMappingToMap((destNode == null) ? invalidMappings : destNode.mappings, mapping);
          } else {
            addMappingToMap(unmanagedMappings, mapping);
          }
        }
      }
    }

    private static void addMappingToMap(Map<String, List<String>> map, WMMapping mapping) {
      List<String> list = map.get(mapping.getEntityType());
      if (list == null) {
        list = new ArrayList<>();
        map.put(mapping.getEntityType(), list);
      }
      list.add(mapping.getEntityName());
    }

    private static void addNonPoolNodes(PoolTreeNode root, Map<String, PoolTreeNode> poolMap,
        List<WMTrigger> unmanagedTriggers, Set<WMTrigger> unusedTriggers, Map<String, List<String>> unmanagedMappings,
        Map<String, List<String>> invalidMappings) {
      if (!unmanagedTriggers.isEmpty() || !unmanagedMappings.isEmpty()) {
        PoolTreeNode curr = createNonPoolNode(poolMap, "unmanaged queries", root);
        curr.triggers.addAll(unmanagedTriggers);
        curr.mappings.putAll(unmanagedMappings);
      }
      // TODO: perhaps we should also summarize the triggers pointing to invalid pools.
      if (!unusedTriggers.isEmpty()) {
        PoolTreeNode curr = createNonPoolNode(poolMap, "unused triggers", root);
        curr.triggers.addAll(unusedTriggers);
      }
      if (!invalidMappings.isEmpty()) {
        PoolTreeNode curr = createNonPoolNode(poolMap, "invalid mappings", root);
        curr.mappings.putAll(invalidMappings);
      }
    }

    private static PoolTreeNode createNonPoolNode(Map<String, PoolTreeNode> poolMap, String name, PoolTreeNode root) {
      PoolTreeNode result;
      do {
        name = "<" + name + ">";
        result = poolMap.get(name);
        // We expect this to never happen in practice. Can pool paths even have angled braces?
      } while (result != null);

      result = new PoolTreeNode();
      result.nonPoolName = name;
      poolMap.put(name, result);
      root.children.add(result);
      return result;
    }
  }

  /**
   * Interface to implement actual conversion to text or json of a resource plan.
   */
  public interface RPFormatter {
    void startRP(String rpName, Object ... kvPairs) throws IOException;
    void endRP() throws IOException;
    void startPools() throws IOException;
    void startPool(String poolName, Object ...kvPairs) throws IOException;
    void endPool() throws IOException;
    void endPools() throws IOException;
    void startTriggers() throws IOException;
    void formatTrigger(String triggerName, String actionExpression, String triggerExpression) throws IOException;
    void endTriggers() throws IOException;
    void startMappings() throws IOException;
    void formatMappingType(String type, List<String> names) throws IOException;
    void endMappings() throws IOException;
  }
}
