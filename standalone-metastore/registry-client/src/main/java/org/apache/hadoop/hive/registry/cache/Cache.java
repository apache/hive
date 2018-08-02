package org.apache.hadoop.hive.registry.cache;

import java.util.Collection;
import java.util.Map;


public interface Cache<K, V> {
  V get(K key);

  Map<K, V> getAll(Collection<? extends K> keys);

  void put(K key, V val);

  void putAll(Map<? extends K,? extends V> entries);

  void remove(K key);

  void removeAll(Collection<? extends K> keys);

  void clear();

  long size();

  ExpiryPolicy getExpiryPolicy();
}
