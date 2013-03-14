package org.apache.hadoop.hive.ql.cube.metadata;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;

public final class Cube extends AbstractCubeTable {
  private final Set<CubeMeasure> measures;
  private final Set<CubeDimension> dimensions;
  private static final List<FieldSchema> columns = new ArrayList<FieldSchema>();
  static {
    columns.add(new FieldSchema("dummy", "string", "dummy column"));
  }

  public Cube(String name, Set<CubeMeasure> measures,
      Set<CubeDimension> dimensions) {
    super(name, columns, new HashMap<String, String>());
    this.measures = measures;
    this.dimensions = dimensions;
    addProperties();
  }

  public Cube(Table tbl) {
    super(tbl);
    this.measures = getMeasures(getName(), getProperties());
    this.dimensions = getDimensions(getName(), getProperties());
  }

  public Set<CubeMeasure> getMeasures() {
    return measures;
  }

  public Set<CubeDimension> getDimensions() {
    return dimensions;
  }

  @Override
  public CubeTableType getTableType() {
    return CubeTableType.CUBE;
  }

  @Override
  public Set<String> getStorages() {
    return null;
  }

  @Override
  public void addProperties() {
    super.addProperties();
    getProperties().put(MetastoreUtil.getCubeMeasureListKey(getName()),
        MetastoreUtil.getNamedStr(measures));
    addMeasures(getProperties(), measures);
    getProperties().put(MetastoreUtil.getCubeDimensionListKey(getName()),
        MetastoreUtil.getNamedStr(dimensions));
    addDimensions(getProperties(), dimensions);
  }

  public static void addMeasures(Map<String, String> props,
      Set<CubeMeasure> measures) {
    for (CubeMeasure measure : measures) {
      measure.addProperties(props);
    }
  }

  public static void addDimensions(Map<String, String> props,
      Set<CubeDimension> dimensions) {
    for (CubeDimension dimension : dimensions) {
      dimension.addProperties(props);
    }
  }

  public static Set<CubeMeasure> getMeasures(String name,
      Map<String, String> props) {
    Set<CubeMeasure> measures = new HashSet<CubeMeasure>();
    String measureStr = props.get(MetastoreUtil.getCubeMeasureListKey(name));
    String[] names = measureStr.split(",");
    for (String measureName : names) {
      String className = props.get(MetastoreUtil.getMeasureClassPropertyKey(
          measureName));
      CubeMeasure measure;
      try {
        Class<?> clazz = Class.forName(className);
        Constructor<?> constructor;
          constructor = clazz.getConstructor(String.class, Map.class);
        measure = (CubeMeasure) constructor.newInstance(new Object[]
           {measureName, props});
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Invalid measure", e);
      } catch (SecurityException e) {
        throw new IllegalArgumentException("Invalid measure", e);
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException("Invalid measure", e);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid measure", e);
      } catch (InstantiationException e) {
        throw new IllegalArgumentException("Invalid measure", e);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("Invalid measure", e);
      } catch (InvocationTargetException e) {
        throw new IllegalArgumentException("Invalid measure", e);
      }
      measures.add(measure);
    }
    return measures;
  }

  public static Set<CubeDimension> getDimensions(String name,
      Map<String, String> props) {
    Set<CubeDimension> dimensions = new HashSet<CubeDimension>();
    String dimStr = props.get(MetastoreUtil.getCubeDimensionListKey(name));
    String[] names = dimStr.split(",");
    for (String dimName : names) {
      CubeDimension dim = new CubeDimension(dimName, props);
      dimensions.add(dim);
    }
    return dimensions;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    Cube other = (Cube) obj;
    if (this.getMeasures() == null) {
      if (other.getMeasures() != null) {
        return false;
      }
    } else if (!this.getMeasures().equals(other.getMeasures())) {
      return false;
    }
    if (this.getDimensions() == null) {
      if (other.getDimensions() != null) {
        return false;
      }
    } else if (!this.getDimensions().equals(other.getDimensions())) {
      return false;
    }
    return true;
  }
}
