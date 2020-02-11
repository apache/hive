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

package org.apache.hadoop.hive.ql.plan.mapper;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.optimizer.signature.OpTreeSignature;
import org.apache.hadoop.hive.ql.optimizer.signature.OpTreeSignatureFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

/**
 * Enables to connect related objects to eachother.
 *
 * Most importantly it aids to connect Operators to OperatorStats and probably RelNodes.
 */
public class PlanMapper {

  Set<EquivGroup> groups = new HashSet<>();
  private Map<Object, EquivGroup> objectMap = new CompositeMap<>(OpTreeSignature.class, AuxOpTreeSignature.class);

  /**
   * Specialized class which can compare by identity or value; based on the key type.
   */
  private static class CompositeMap<K, V> implements Map<K, V> {

    Map<K, V> comparedMap = new HashMap<>();
    Map<K, V> identityMap = new IdentityHashMap<>();
    final Set<Class<?>> typeCompared;

    CompositeMap(Class<?>... comparedTypes) {
      for (Class<?> class1 : comparedTypes) {
        if (!Modifier.isFinal(class1.getModifiers())) {
          throw new RuntimeException(class1 + " is not final...for this to reliably work; it should be");
        }
      }
      typeCompared = Sets.newHashSet(comparedTypes);
    }

    @Override
    public int size() {
      return comparedMap.size() + identityMap.size();
    }

    @Override
    public boolean isEmpty() {
      return comparedMap.isEmpty() && identityMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
      return comparedMap.containsKey(key) || identityMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
      return comparedMap.containsValue(value) || identityMap.containsValue(value);
    }

    @Override
    public V get(Object key) {
      V v0 = comparedMap.get(key);
      if (v0 != null) {
        return v0;
      }
      return identityMap.get(key);
    }

    @Override
    public V put(K key, V value) {
      if (shouldCompare(key.getClass())) {
        return comparedMap.put(key, value);
      } else {
        return identityMap.put(key, value);
      }
    }

    @Override
    public V remove(Object key) {
      if (shouldCompare(key.getClass())) {
        return comparedMap.remove(key);
      } else {
        return identityMap.remove(key);
      }
    }

    private boolean shouldCompare(Class<?> key) {
      return typeCompared.contains(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
      for (Entry<? extends K, ? extends V> e : m.entrySet()) {
        put(e.getKey(), e.getValue());
      }
    }

    @Override
    public void clear() {
      comparedMap.clear();
      identityMap.clear();
    }

    @Override
    public Set<K> keySet() {
      return Sets.union(comparedMap.keySet(), identityMap.keySet());
    }

    @Override
    public Collection<V> values() {
      throw new UnsupportedOperationException("This method is not supported");
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
      return Sets.union(comparedMap.entrySet(), identityMap.entrySet());
    }

  }

  /**
   * A set of objects which are representing the same thing.
   *
   * A Group may contain different kind of things which are connected by their purpose;
   * For example currently a group may contain the following objects:
   * <ul>
   *   <li> Operator(s) - which are doing the actual work;
   *   there might be more than one, since an optimization may replace an operator with a new one
   *   <li> Signature - to enable inter-plan look up of the same data
   *   <li> OperatorStats - collected runtime information
   * </ul>
   */
  public class EquivGroup {
    Set<Object> members = new HashSet<>();

    public void add(Object o) {
      if (members.contains(o)) {
        return;
      }
      members.add(o);
      objectMap.put(o, this);
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> getAll(Class<T> clazz) {
      List<T> ret = new ArrayList<>();
      for (Object m : members) {
        if (clazz.isInstance(m)) {
          ret.add((T) m);
        }
      }
      return ret;
    }
  }

  /**
   * States that the two objects are representing the same.
   *
   * For example if during an optimization Operator_A is replaced by a specialized Operator_A1;
   * then those two can be linked.
   */
  public void link(Object o1, Object o2) {
    link(o1, o2, false);
  }

  /**
   * Links and optionally merges the groups identified by the two objects.
   */
  public void merge(Object o1, Object o2) {
    link(o1, o2, true);
  }

  private void link(Object o1, Object o2, boolean mayMerge) {

    Set<Object> keySet = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
    keySet.add(o1);
    keySet.add(o2);
    keySet.add(getKeyFor(o1));
    keySet.add(getKeyFor(o2));

    Set<EquivGroup> mGroups = Collections.newSetFromMap(new IdentityHashMap<EquivGroup, Boolean>());

    for (Object object : keySet) {
      EquivGroup group = objectMap.get(object);
      if (group != null) {
        mGroups.add(group);
      }
    }
    if (mGroups.size() > 1) {
      if (!mayMerge) {
        throw new RuntimeException("equivalence mapping violation");
      }
      EquivGroup newGrp = new EquivGroup();
      newGrp.add(o1);
      newGrp.add(o2);
      for (EquivGroup g : mGroups) {
        for (Object o : g.members) {
          newGrp.add(o);
        }
      }
      groups.add(newGrp);
      groups.removeAll(mGroups);
    } else {
      EquivGroup targetGroup = mGroups.isEmpty() ? new EquivGroup() : mGroups.iterator().next();
      groups.add(targetGroup);
      targetGroup.add(o1);
      targetGroup.add(o2);
    }

  }

  private OpTreeSignatureFactory signatureCache = OpTreeSignatureFactory.newCache();

  private Object getKeyFor(Object o) {
    if (o instanceof Operator) {
      Operator<?> operator = (Operator<?>) o;
      return signatureCache.getSignature(operator);
    }
    return o;
  }

  public <T> List<T> getAll(Class<T> clazz) {
    List<T> ret = new ArrayList<>();
    for (EquivGroup g : groups) {
      ret.addAll(g.getAll(clazz));
    }
    return ret;
  }

  public void runMapper(GroupTransformer mapper) {
    for (EquivGroup equivGroup : groups) {
      mapper.map(equivGroup);
    }
  }

  public <T> List<T> lookupAll(Class<T> clazz, Object key) {
    EquivGroup group = objectMap.get(key);
    if (group == null) {
      throw new NoSuchElementException(Objects.toString(key));
    }
    return group.getAll(clazz);
  }

  public <T> T lookup(Class<T> clazz, Object key) {
    List<T> all = lookupAll(clazz, key);
    if (all.size() != 1) {
      // FIXME: use a different exception type?
      throw new IllegalArgumentException("Expected match count is 1; but got:" + all);
    }
    return all.get(0);
  }

  @VisibleForTesting
  public Iterator<EquivGroup> iterateGroups() {
    return groups.iterator();

  }

  public OpTreeSignature getSignatureOf(Operator<?> op) {
    OpTreeSignature sig = signatureCache.getSignature(op);
    return sig;
  }

  public void clearSignatureCache() {
    //    auxSignatureCache.clear();
  }

  private OpTreeSignatureFactory auxSignatureCache = OpTreeSignatureFactory.newCache();

  public AuxOpTreeSignature getAuxSignatureOf(Operator<?> op) {
    OpTreeSignature x = auxSignatureCache.getSignature(op);
    return new AuxOpTreeSignature(x);
  }

}
