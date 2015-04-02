package org.apache.hadoop.hive.ql.exec;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.llap.io.api.LlapIoProxy;

public class GlobalWorkMapFactory {

  private ThreadLocal<Map<Path, BaseWork>> threadLocalWorkMap = null;

  private Map<Path, BaseWork> gWorkMap = null;

  private static class DummyMap<K, V> implements Map<K, V> {
    @Override
    public void clear() {
    }

    @Override
    public boolean containsKey(final Object key) {
      return false;
    }

    @Override
    public boolean containsValue(final Object value) {
      return false;
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
      return null;
    }

    @Override
    public V get(final Object key) {
      return null;
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public Set<K> keySet() {
      return null;
    }

    @Override
    public V put(final K key, final V value) {
      return null;
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> t) {
    }

    @Override
    public V remove(final Object key) {
      return null;
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public Collection<V> values() {
      return null;
    }
  }

  DummyMap<Path, BaseWork> dummy = new DummyMap<Path, BaseWork>();

  public Map<Path, BaseWork> get(Configuration conf) {
    if (LlapIoProxy.isDaemon()
        || HiveConf.getVar(conf, ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")) {
      if (threadLocalWorkMap == null) {
        threadLocalWorkMap = new ThreadLocal<Map<Path, BaseWork>>() {
          @Override
          protected Map<Path, BaseWork> initialValue() {
            return new HashMap<Path, BaseWork>();
          }
        };
      }
      return threadLocalWorkMap.get();
    }

    if (gWorkMap == null) {
      gWorkMap = new HashMap<Path, BaseWork>();
    }
    return gWorkMap;
  }

}
