package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableAccessRel;
import org.eigenbase.relopt.RelOptAbstractTable;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.reltype.RelDataType;

/*
 * Fix Me: 
 * 1. Column Pruning
 * 2. Partition Pruning
 * 3. Stats
 */

public class RelOptHiveTable extends RelOptAbstractTable {
  private final Table       m_hiveTblMetadata;
  private double            m_rowCount           = -1;

  final Map<String, Double> m_columnIdxToSizeMap = new HashMap<String, Double>();

  Map<String, Integer>      m_bucketingColMap;
  Map<String, Integer>      m_bucketingSortColMap;

  Statistics                m_hiveStats;
  List<ColStatistics>       m_hiveColStats = new ArrayList<ColStatistics>();

	protected static final Log LOG = LogFactory.getLog(RelOptHiveTable.class
			.getName());

  // NOTE: name here is the table alias which may or may not be the real name in
  // metadata. Use
  // m_hiveTblMetadata.getTableName() for table name and
  // m_hiveTblMetadata.getDbName() for db name.
  public RelOptHiveTable(RelOptSchema schema, String name, RelDataType rowType,
      Table hiveTblMetadata, Statistics stats) {
    super(schema, name, rowType);
    m_hiveTblMetadata = hiveTblMetadata;
  }

  public RelOptHiveTable(RelOptSchema optiqSchema, String name, RelDataType rowType,
      Table hiveTblMetadata, List<ColumnInfo> hiveSchema) {
    super(optiqSchema, name, rowType);
    m_hiveTblMetadata = hiveTblMetadata;
    
    List<String> neededColumns = new ArrayList<String>();
    for (ColumnInfo ci : hiveSchema) {
      neededColumns.add(ci.getInternalName());
    }
    
    //TODO: Fix below two stats
    m_hiveColStats = StatsUtils.getTableColumnStats(m_hiveTblMetadata, hiveSchema, neededColumns);
    m_rowCount = StatsUtils.getNumRows(m_hiveTblMetadata);
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

  private String getColNameList(Set<Integer> colLst) {
    StringBuffer sb = new StringBuffer();
    List<FieldSchema> schema = m_hiveTblMetadata.getAllCols();
    for (Integer i : colLst) {
      String colName = (i < schema.size()) ? m_hiveTblMetadata.getAllCols().get(i).getName() : "";
      if (i == 0)
        sb.append(colName);
      else
        sb.append(", " + colName);
    }
    return sb.toString();
  }

  public List<ColStatistics> getColStat(List<Integer> projIndxLst) {
    if (projIndxLst != null) {
      Set<Integer> colsWithoutStats = new HashSet<Integer>();
      List<ColStatistics> hiveColStatLst = new LinkedList<ColStatistics>();
      for (Integer i : projIndxLst) {
        if (i >= m_hiveColStats.size())
          colsWithoutStats.add(i);
        else
          hiveColStatLst.add(m_hiveColStats.get(i));
      }
      if (!colsWithoutStats.isEmpty()) {
        String logMsg = "No Stats for DB@Table " + m_hiveTblMetadata.getCompleteName()
            + ", Columns: " + getColNameList(colsWithoutStats);
        LOG.error(logMsg);
        throw new RuntimeException(logMsg);
      }

      return hiveColStatLst;
    } else {
      return m_hiveColStats;
    }
  }
}
