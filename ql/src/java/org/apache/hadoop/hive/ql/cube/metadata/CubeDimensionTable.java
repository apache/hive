package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;

public final class CubeDimensionTable extends AbstractCubeTable {
  private final Map<String, TableReference> dimensionReferences;
  private final Map<String, UpdatePeriod> snapshotDumpPeriods;

  public CubeDimensionTable(String dimName, List<FieldSchema> columns) {
    this(dimName, columns, new HashMap<String, TableReference>());
  }

  public CubeDimensionTable(String dimName, List<FieldSchema> columns,
      Map<String, TableReference> dimensionReferences) {
    this(dimName, columns, dimensionReferences, null);
  }

  public CubeDimensionTable(String dimName, List<FieldSchema> columns,
      Map<String, TableReference> dimensionReferences,
      Map<String, UpdatePeriod> snapshotDumpPeriods) {
    this(dimName, columns, dimensionReferences, new HashMap<String, String>(),
        snapshotDumpPeriods);
  }

  public CubeDimensionTable(String dimName, List<FieldSchema> columns,
      Map<String, TableReference> dimensionReferences,
      Map<String, String> properties,
      Map<String, UpdatePeriod> snapshotDumpPeriods) {
    super(dimName, columns, properties);
    this.dimensionReferences = dimensionReferences;
    this.snapshotDumpPeriods = snapshotDumpPeriods;
    addProperties();
  }

  public CubeDimensionTable(Table tbl) {
    super(tbl);
    this.dimensionReferences = getDimensionReferences(getProperties());
    this.snapshotDumpPeriods = getDumpPeriods(getName(), getProperties());
  }

  @Override
  public CubeTableType getTableType() {
    return CubeTableType.DIMENSION;
  }

  @Override
  protected void addProperties() {
    super.addProperties();
    addDimensionReferenceProperties(getProperties(), dimensionReferences);
    addSnapshotPeriods(getName(), getProperties(), snapshotDumpPeriods);
  }

  public Map<String, TableReference> getDimensionReferences() {
    return dimensionReferences;
  }

  public Map<String, UpdatePeriod> getSnapshotDumpPeriods() {
    return snapshotDumpPeriods;
  }

  public static void addSnapshotPeriods(String name, Map<String, String> props,
      Map<String, UpdatePeriod> snapshotDumpPeriods) {
    if (snapshotDumpPeriods != null) {
      props.put(MetastoreUtil.getDimensionStorageListKey(name),
          MetastoreUtil.getStr(snapshotDumpPeriods.keySet()));
      for (Map.Entry<String, UpdatePeriod> entry : snapshotDumpPeriods.entrySet())
      {
        if (entry.getValue() != null) {
          props.put(MetastoreUtil.getDimensionDumpPeriodKey(name, entry.getKey()),
              entry.getValue().name());
        }
      }
    }
  }

  public static void addDimensionReferenceProperties(Map<String, String> props,
      Map<String, TableReference> dimensionReferences) {
    if (dimensionReferences != null) {
      for (Map.Entry<String, TableReference> entry : dimensionReferences.entrySet()) {
        props.put(MetastoreUtil.getDimensionSrcReferenceKey(entry.getKey()),
            MetastoreUtil.getDimensionDestReference(entry.getValue()));
      }
    }
  }

  public static Map<String, TableReference> getDimensionReferences(
      Map<String, String> params) {
    Map<String, TableReference> dimensionReferences =
        new HashMap<String, TableReference>();
    for (String param : params.keySet()) {
      if (param.startsWith(MetastoreConstants.DIM_KEY_PFX)) {
        String key = param.replace(MetastoreConstants.DIM_KEY_PFX, "");
        String toks[] = key.split("\\.+");
        String dimName = toks[0];
        String value = params.get(MetastoreUtil.getDimensionSrcReferenceKey(dimName));
        if (value != null) {
          dimensionReferences.put(dimName, new TableReference(value));
        }
      }
    }
    return dimensionReferences;
  }

  public static Map<String, UpdatePeriod> getDumpPeriods(String name,
      Map<String, String> params) {
    String storagesStr = params.get(MetastoreUtil.getDimensionStorageListKey(
        name));
    if (storagesStr != null) {
      Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
      String[] storages = storagesStr.split(",");
      for (String storage : storages) {
        String dumpPeriod = params.get(MetastoreUtil.getDimensionDumpPeriodKey(
            name, storage));
        if (dumpPeriod != null) {
          dumpPeriods.put(storage, UpdatePeriod.valueOf(dumpPeriod));
        } else {
          dumpPeriods.put(storage, null);
        }
      }
      return dumpPeriods;
    }
    return null;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    CubeDimensionTable other = (CubeDimensionTable) obj;

    if (this.getDimensionReferences() == null) {
      if (other.getDimensionReferences() != null) {
        return false;
      }
    } else {
      if (!this.getDimensionReferences().equals(
          other.getDimensionReferences())) {
        return false;
      }
    }
    if (this.getSnapshotDumpPeriods() == null) {
      if (other.getSnapshotDumpPeriods() != null) {
        return false;
      }
    } else {
      if (!this.getSnapshotDumpPeriods().equals(
          other.getSnapshotDumpPeriods())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Set<String> getStorages() {
    return snapshotDumpPeriods.keySet();
  }

  public boolean hasStorageSnapshots(String storage) {
    return (snapshotDumpPeriods.get(storage) != null);
  }
}
