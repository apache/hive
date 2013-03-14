package org.apache.hadoop.hive.ql.cube.metadata;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CubeDimension implements Named {
  private final String name;
  private final List<BaseDimension> hierarchy;

  public CubeDimension(String name, List<BaseDimension> hierarchy) {
    this.name = name;
    this.hierarchy = hierarchy;
    assert (name != null);
    assert (hierarchy != null);
  }

  public String getName() {
    return name;
  }

  public List<BaseDimension> getHierarchy() {
    return hierarchy;
  }

  public void addProperties(Map<String, String> props) {
    for (int i =0; i < hierarchy.size(); i++) {
      BaseDimension dim = hierarchy.get(i);
      props.put(MetastoreUtil.getHierachyElementKeyName(name, i),
          getHierarchyElement(dim));
      dim.addProperties(props);
    }
  }

  public static String getHierarchyElement(BaseDimension dim) {
    return dim.getName() + "," + dim.getClass().getCanonicalName();
  }

  public CubeDimension(String name, Map<String, String> props) {
    this.name = name;
    this.hierarchy = getHiearachy(name, props);
  }

  public static List<BaseDimension> getHiearachy(String name,
      Map<String, String> props) {
    Map<Integer, String> hierarchyElements = new HashMap<Integer, String>();
    for (String param : props.keySet()) {
      if (param.startsWith(MetastoreUtil.getHierachyElementKeyPFX(name))) {
        hierarchyElements.put(MetastoreUtil.getHierachyElementIndex(name, param),
            props.get(param));
      }
    }
    List<BaseDimension> hierarchy = new ArrayList<BaseDimension>(
        hierarchyElements.size());
    for (int i = 0; i < hierarchyElements.size(); i++) {
      String hierarchyElement = hierarchyElements.get(i);
      String[] elements = hierarchyElement.split(",");
      String dimName = elements[0];
      String className = elements[1];
      BaseDimension dim;
      try {
        Class<?> clazz = Class.forName(className);
        Constructor<?> constructor;
        constructor = clazz.getConstructor(String.class, Map.class);
        dim = (BaseDimension) constructor.newInstance(new Object[]
            {dimName, props});
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Invalid Dimension", e);
      } catch (SecurityException e) {
        throw new IllegalArgumentException("Invalid Dimension", e);
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException("Invalid Dimension", e);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid Dimension", e);
      } catch (InstantiationException e) {
        throw new IllegalArgumentException("Invalid Dimension", e);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("Invalid Dimension", e);
      } catch (InvocationTargetException e) {
        throw new IllegalArgumentException("Invalid Dimension", e);
      }
      hierarchy.add(dim);
    }
    return hierarchy;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((getName() == null) ? 0 :
      getName().toLowerCase().hashCode());
    result = prime * result + ((getHierarchy() == null) ? 0 :
      getHierarchy().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    CubeDimension other = (CubeDimension)obj;
    if (this.getName() == null) {
      if (other.getName() != null) {
        return false;
      }
    } else if (!this.getName().equalsIgnoreCase(other.getName())) {
      return false;
    }
    if (this.getHierarchy() == null) {
      if (other.getHierarchy() != null) {
        return false;
      }
    } else if (!this.getHierarchy().equals(other.getHierarchy())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    String str = name;
    str += ", hierarchy:" + MetastoreUtil.getObjectStr(hierarchy);
    return str;
  }
}
