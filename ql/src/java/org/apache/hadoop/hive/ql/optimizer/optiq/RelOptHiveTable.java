package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableAccessRel;
import org.eigenbase.relopt.RelOptAbstractTable;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.reltype.RelDataType;

//Fix Me: use table meta data and stats util to get stats
public class RelOptHiveTable extends RelOptAbstractTable {
  private final Table       m_hiveTblMetadata;
  private double            m_rowCount           = -1;

  final Map<String, Double> m_columnIdxToSizeMap = new HashMap<String, Double>();

  Map<String, Integer>      m_bucketingColMap;
  Map<String, Integer>      m_bucketingSortColMap;

  Statistics                m_hiveStats;

  // NOTE: name here is the table alias which may or may not be the real name in
  // metadata. Use
  // m_hiveTblMetadata.getTableName() for table name and
  // m_hiveTblMetadata.getDbName() for db name.
  public RelOptHiveTable(RelOptSchema schema, String name, RelDataType rowType,
      Table hiveTblMetadata, Statistics stats) {
    super(schema, name, rowType);
    m_hiveTblMetadata = hiveTblMetadata;
    m_hiveStats = stats;

    m_rowCount = stats.getNumRows();
  }

  @Override
  public boolean isKey(BitSet arg0) {
    return false;
  }

  @Override
  public RelNode toRel(ToRelContext context) {
    return new TableAccessRel(context.getCluster(), this);
  }

  @Override
  public <T> T unwrap(Class<T> arg0) {
    return arg0.isInstance(this) ? arg0.cast(this) : null;
  }

  @Override
  public double getRowCount() {
    return m_rowCount;
  }

  public Table getHiveTableMD() {
    return m_hiveTblMetadata;
  }

  public Statistics getHiveStats() {
    return m_hiveStats;
  }
}
