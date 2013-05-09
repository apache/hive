package org.apache.hadoop.hive.ql.cube.metadata;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod.UpdatePeriodComparator;
import org.apache.hadoop.hive.ql.cube.parse.DateUtil;
import org.apache.hadoop.hive.ql.metadata.Table;

public final class CubeFactTable extends AbstractCubeTable {
  private final String cubeName;
  private final Map<String, List<UpdatePeriod>> storageUpdatePeriods;

  public CubeFactTable(String cubeName, String factName,
      List<FieldSchema> columns) {
    this(cubeName, factName, columns, new HashMap<String, List<UpdatePeriod>>());
  }

  public CubeFactTable(Table hiveTable) {
    super(hiveTable);
    this.storageUpdatePeriods = getUpdatePeriods(getName(), getProperties());
    this.cubeName = getProperties().get(
        MetastoreUtil.getFactCubeNameKey(getName()));
  }

  public CubeFactTable(String cubeName, String factName,
      List<FieldSchema> columns,
      Map<String, List<UpdatePeriod>> storageUpdatePeriods) {
    this(cubeName, factName, columns, storageUpdatePeriods,
        new HashMap<String, String>());
  }

  public CubeFactTable(String cubeName, String factName,
      List<FieldSchema> columns,
      Map<String, List<UpdatePeriod>> storageUpdatePeriods,
      Map<String, String> properties) {
    super(factName, columns, properties);
    this.cubeName = cubeName;
    this.storageUpdatePeriods = storageUpdatePeriods;
    addProperties();
  }

  @Override
  protected void addProperties() {
    super.addProperties();
    getProperties().put(MetastoreUtil.getFactCubeNameKey(getName()), cubeName);
    addUpdatePeriodProperies(getName(), getProperties(), storageUpdatePeriods);
  }

  public static void addUpdatePeriodProperies(String name,
      Map<String, String> props,
      Map<String, List<UpdatePeriod>> updatePeriods) {
    if (updatePeriods != null) {
      props.put(MetastoreUtil.getFactStorageListKey(name),
          MetastoreUtil.getStr(updatePeriods.keySet()));
      for (Map.Entry<String, List<UpdatePeriod>> entry : updatePeriods.entrySet()) {
        props.put(MetastoreUtil.getFactUpdatePeriodKey(name, entry.getKey()),
            MetastoreUtil.getNamedStr(entry.getValue()));
      }
    }
  }

  public static Map<String, List<UpdatePeriod>> getUpdatePeriods(String name,
      Map<String, String> props) {
    Map<String, List<UpdatePeriod>> storageUpdatePeriods = new HashMap<String,
        List<UpdatePeriod>>();
    String storagesStr = props.get(MetastoreUtil.getFactStorageListKey(name));
    String[] storages = storagesStr.split(",");
    for (String storage : storages) {
      String updatePeriodStr = props.get(MetastoreUtil.getFactUpdatePeriodKey(
          name, storage));
      String[] periods = updatePeriodStr.split(",");
      List<UpdatePeriod> updatePeriods = new ArrayList<UpdatePeriod>();
      for (String period : periods) {
        updatePeriods.add(UpdatePeriod.valueOf(period));
      }
      storageUpdatePeriods.put(storage, updatePeriods);
    }
    return storageUpdatePeriods;
  }

  public Map<String, List<UpdatePeriod>> getUpdatePeriods() {
    return storageUpdatePeriods;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }

    CubeFactTable other = (CubeFactTable) obj;
    if (this.getUpdatePeriods() == null) {
      if (other.getUpdatePeriods() != null) {
        return false;
      }
    } else {
      if (!this.getUpdatePeriods().equals(other.getUpdatePeriods())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public CubeTableType getTableType() {
    return CubeTableType.FACT;
  }

  public List<String> getPartitions(Date fromDate, Date toDate,
      UpdatePeriod interval) {
    String fmt = interval.format();
    if (fmt != null) {
      Calendar cal = Calendar.getInstance();
      cal.setTime(fromDate);
      List<String> partitions = new ArrayList<String>();
      Date dt = cal.getTime();
      while (dt.compareTo(toDate) < 0) {
        String part = new SimpleDateFormat(fmt).format(cal.getTime());
        partitions.add(part);
        cal.add(interval.calendarField(), 1);
        dt = cal.getTime();
      }
      return partitions;
    } else {
      return null;
    }
  }


  public UpdatePeriod maxIntervalInRange(Date from, Date to) {
    UpdatePeriod max = null;

    long diff = to.getTime() - from.getTime();
    if (diff < UpdatePeriod.MIN_INTERVAL) {
      return null;
    }

    Set<UpdatePeriod> updatePeriods = new HashSet<UpdatePeriod>();

    for (List<UpdatePeriod> value : storageUpdatePeriods.values()) {
      updatePeriods.addAll(value);
    }

    // Use weight only till UpdatePeriod.DAILY
    // Above Daily, check if at least one full update period is present between the dates
    UpdatePeriodComparator cmp = new UpdatePeriodComparator();
    for (UpdatePeriod i : updatePeriods) {
      if (UpdatePeriod.YEARLY == i || UpdatePeriod.QUARTERLY == i || UpdatePeriod.MONTHLY == i) {
        int intervals = 0;
        switch (i) {
        case YEARLY:
          intervals = DateUtil.getYearsBetween(from, to);
          break;
        case QUARTERLY:
          intervals = DateUtil.getQuartersBetween(from, to);
          break;
        case MONTHLY:
          intervals = DateUtil.getMonthsBetween(from, to);
          break;
        }

        if (intervals > 0) {
          if (cmp.compare(i, max) > 0) {
            max = i;
          }
        }
      } else {
        // Below MONTHLY, we can use weight to find out the correct period
        if (diff < i.weight()) {
          // interval larger than time diff
          continue;
        }

        if (cmp.compare(i, max) > 0) {
          max = i;
        }
      }
    }
    return max;
  }

  @Override
  public Set<String> getStorages() {
    return storageUpdatePeriods.keySet();
  }

  public String getCubeName() {
    return cubeName;
  }

}
