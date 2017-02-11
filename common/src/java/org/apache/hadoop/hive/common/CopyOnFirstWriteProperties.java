package org.apache.hadoop.hive.common;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * A special subclass of Properties, designed to save memory when many identical
 * copies of Properties would otherwise be created. To achieve that, we use the
 * 'interned' field, which points to the same Properties object for all instances
 * of CopyOnFirstWriteProperties that were created with identical contents.
 * However, as soon as any mutating method is called, contents are copied from
 * the 'interned' properties into this instance.
 */
public class CopyOnFirstWriteProperties extends Properties {

  private Properties interned;

  private static Interner<Properties> INTERNER = Interners.newWeakInterner();
  private static Field defaultsField;
  static {
    try {
      defaultsField = Properties.class.getDeclaredField("defaults");
      defaultsField.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public CopyOnFirstWriteProperties(Properties p) {
    setInterned(p);
  }

  /*************   Public API of java.util.Properties   ************/

  @Override
  public String getProperty(String key) {
    if (interned != null) return interned.getProperty(key);
    else return super.getProperty(key);
  }

  @Override
  public String getProperty(String key, String defaultValue) {
    if (interned != null) return interned.getProperty(key, defaultValue);
    else return super.getProperty(key, defaultValue);
  }

  @Override
  public void list(PrintStream out) {
    if (interned != null) interned.list(out);
    else super.list(out);
  }

  @Override
  public void list(PrintWriter out) {
    if (interned != null) interned.list(out);
    else super.list(out);
  }

  @Override
  public synchronized void load(InputStream inStream) throws IOException {
    if (interned != null) copyFromInternedToThis();
    super.load(inStream);
  }

  @Override
  public synchronized void load(Reader reader) throws IOException {
    if (interned != null) copyFromInternedToThis();
    super.load(reader);
  }

  @Override
  public synchronized void loadFromXML(InputStream inStream) throws IOException {
    if (interned != null) copyFromInternedToThis();
    super.loadFromXML(inStream);
  }

  @Override
  public Enumeration<?> propertyNames() {
    if (interned != null) return interned.propertyNames();
    else return super.propertyNames();
  }

  @Override
  public synchronized Object setProperty(String key, String value) {
    if (interned != null) copyFromInternedToThis();
    return super.setProperty(key, value);
  }

  @Override
  public void store(OutputStream out, String comments) throws IOException {
    if (interned != null) interned.store(out, comments);
    else super.store(out, comments);
  }

  @Override
  public void storeToXML(OutputStream os, String comment) throws IOException {
    if (interned != null) interned.storeToXML(os, comment);
    else super.storeToXML(os, comment);
  }

  @Override
  public void storeToXML(OutputStream os, String comment, String encoding)
      throws IOException {
    if (interned != null) interned.storeToXML(os, comment, encoding);
    else super.storeToXML(os, comment, encoding);
  }

  @Override
  public Set<String> stringPropertyNames() {
    if (interned != null) return interned.stringPropertyNames();
    else return super.stringPropertyNames();
  }

  /*************   Public API of java.util.Hashtable   ************/

  // Note that in JDK 8, some methods were added to the public API of Hashtable.
  // Most of them cannot be compiled on JDK 7, since they need e.g. Function and
  // BiFunction JDK classes that are only available from JDK 8. Thus these methods,
  // that we should overridde to work correctly in CopyOnFirstWriteProperties, are
  // currently commented out. They should be implemented properly once Hive moves
  // to JDK8. However, it's highly unlikely that anyone would want to call these
  // methods on our Properties objects.

  @Override
  public synchronized void clear() {
    if (interned != null) copyFromInternedToThis();
    super.clear();
  }

  @Override
  public synchronized Object clone() {
    if (interned != null) return new CopyOnFirstWriteProperties(interned);
    else return super.clone();
  }

  /*
  @Override
  public synchronized Object compute(Object key, BiFunction remappingFunction)

  @Override
  public synchronized Object computeIfAbsent(Object key, Function mappingFunction)

  @Override
  public synchronized Object computeIfPresent(Object key, BiFunction remappingFunction)
  */

  @Override
  public synchronized boolean contains(Object value) {
    if (interned != null) return interned.contains(value);
    else return super.contains(value);
  }

  @Override
  public synchronized boolean containsKey(Object key) {
    if (interned != null) return interned.containsKey(key);
    else return super.containsKey(key);
  }

  @Override
  public synchronized boolean containsValue(Object value) {
    if (interned != null) return interned.containsValue(value);
    else return super.containsValue(value);
  }

  @Override
  public synchronized Enumeration<Object> elements() {
    if (interned != null) return interned.elements();
    else return super.elements();
  }

  @Override
  public Set<Map.Entry<Object, Object>> entrySet() {
    if (interned != null) return interned.entrySet();
    else return super.entrySet();
  }

  @Override
  public synchronized boolean equals(Object o) {
    if (interned != null) return interned.equals(o);
    else return super.equals(o);
  }

  /* Available starting from JDK 8
  @Override
  public synchronized void forEach(BiConsumer action)
  */

  @Override
  public synchronized Object get(Object key) {
    if (interned != null) return interned.get(key);
    else return super.get(key);
  }

  /* Available starting from JDK 8
  @Override
  public synchronized Object getOrDefault(Object key, Object defaultValue)
  */

  @Override
  public synchronized int hashCode() {
    if (interned != null) return interned.hashCode();
    else return super.hashCode();
  }

  @Override
  public synchronized boolean isEmpty() {
    if (interned != null) return interned.isEmpty();
    else return super.isEmpty();
  }

  @Override
  public synchronized Enumeration<Object> keys() {
    if (interned != null) return interned.keys();
    else return super.keys();
  }

  @Override
  public Set<Object> keySet() {
    if (interned != null) return interned.keySet();
    else return super.keySet();
  }

  /* Available starting from JDK 8
  @Override
  public synchronized V merge(K key, V value, BiFunction remappingFunction)
  */

  @Override
  public synchronized Object put(Object key, Object value) {
    if (interned != null) copyFromInternedToThis();
    return super.put(key, value);
  }

  @Override
  public synchronized void putAll(Map<? extends Object, ? extends Object> t) {
    if (interned != null) copyFromInternedToThis();
    super.putAll(t);
  }

  /* Available starting from JDK 8
  @Override
  public synchronized Object putIfAbsent(Object key, Object value)
  */

  @Override
  public synchronized Object remove(Object key) {
    if (interned != null) copyFromInternedToThis();
    return super.remove(key);
  }

  /* Available starting from JDK 8
  @Override
  public synchronized boolean remove(Object key, Object value)

  @Override
  public synchronized Object replace(Object key, Object value)

  @Override
  public synchronized boolean replace(Object key, Object oldValue, Object newValue)

  @Override
  public synchronized void replaceAll(BiFunction<? super K, ? super V, ? extends V> function)
  */

  @Override
  public synchronized int size() {
    if (interned != null) return interned.size();
    else return super.size();
  }

  @Override
  public synchronized String toString() {
    if (interned != null) return interned.toString();
    else return super.toString();
  }

  @Override
  public Collection<Object> values() {
    if (interned != null) return interned.values();
    else return super.values();
  }

  /*************   Private implementation ************/

  private void copyFromInternedToThis() {
    for (Map.Entry<?,?> e : interned.entrySet()) {
      super.put(e.getKey(), e.getValue());
    }
    try {
      // Unfortunately, we cannot directly read a protected field of non-this object
      this.defaults = (Properties) defaultsField.get(interned);
    } catch (IllegalAccessException e) {   // Shouldn't happen
      throw new RuntimeException(e);
    }
    setInterned(null);
  }

  public void setInterned(Properties p) {
    if (p != null) {
      this.interned = INTERNER.intern(p);
    } else {
      this.interned = p;
    }
  }

  // These methods are required by serialization

  public CopyOnFirstWriteProperties() {
  }

  public Properties getInterned() {
    return interned;
  }
}
