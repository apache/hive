package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.optiq.translator.ExprNodeConverter;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableAccessRel;
import org.eigenbase.relopt.RelOptAbstractTable;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public class RelOptHiveTable extends RelOptAbstractTable {
  private final Table                             m_hiveTblMetadata;
  private final ImmutableList<ColumnInfo>         m_hiveNonPartitionCols;
  private final ImmutableMap<Integer, ColumnInfo> m_hiveNonPartitionColsMap;
  private final ImmutableMap<Integer, ColumnInfo> m_hivePartitionColsMap;
  private final int                               m_noOfProjs;
  final HiveConf                                  m_hiveConf;

  private double                                  m_rowCount        = -1;
  Map<Integer, ColStatistics>                     m_hiveColStatsMap = new HashMap<Integer, ColStatistics>();
  private Integer                                 m_numPartitions;
  PrunedPartitionList                             partitionList;

  protected static final Log                      LOG               = LogFactory
                                                                        .getLog(RelOptHiveTable.class
                                                                            .getName());

  public RelOptHiveTable(RelOptSchema optiqSchema, String name, RelDataType rowType,
      Table hiveTblMetadata, List<ColumnInfo> hiveNonPartitionCols,
      List<ColumnInfo> hivePartitionCols, HiveConf hconf) {
    super(optiqSchema, name, rowType);
    m_hiveTblMetadata = hiveTblMetadata;
    m_hiveNonPartitionCols = ImmutableList.copyOf(hiveNonPartitionCols);
    m_hiveNonPartitionColsMap = getColInfoMap(hiveNonPartitionCols, 0);
    m_hivePartitionColsMap = getColInfoMap(hivePartitionCols, m_hiveNonPartitionColsMap.size());
    m_noOfProjs = hiveNonPartitionCols.size() + hivePartitionCols.size();
    m_hiveConf = hconf;
  }

  private static ImmutableMap<Integer, ColumnInfo> getColInfoMap(List<ColumnInfo> hiveCols,
      int startIndx) {
    Builder<Integer, ColumnInfo> bldr = ImmutableMap.<Integer, ColumnInfo> builder();

    int indx = startIndx;
    for (ColumnInfo ci : hiveCols) {
      bldr.put(indx, ci);
      indx++;
    }

    return bldr.build();
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
    if (m_rowCount == -1) {

      if (m_hiveTblMetadata.isPartitioned()) {
        if (partitionList == null) {
          try {
            List<Partition> parts = new ArrayList<Partition>(
                Hive.get().getAllPartitionsOf(m_hiveTblMetadata));
            List<Long> rowCounts = StatsUtils.getBasicStatForPartitions(
                m_hiveTblMetadata, parts, StatsSetupConst.ROW_COUNT);
            m_rowCount = StatsUtils.getSumIgnoreNegatives(rowCounts);

          } catch (HiveException he) {
            throw new RuntimeException(he);
          }
        } else {
          List<Long> rowCounts = StatsUtils.getBasicStatForPartitions(
              m_hiveTblMetadata, partitionList.getNotDeniedPartns(),
              StatsSetupConst.ROW_COUNT);
          m_rowCount = StatsUtils.getSumIgnoreNegatives(rowCounts);
        }
      } else {
        m_rowCount = StatsUtils.getNumRows(m_hiveTblMetadata);
      }
    }

    return m_rowCount;
  }

  public Table getHiveTableMD() {
    return m_hiveTblMetadata;
  }

  private String getColNamesForLogging(Set<String> colLst) {
    StringBuffer sb = new StringBuffer();
    boolean firstEntry = true;
    for (String colName : colLst) {
      if (firstEntry) {
        sb.append(colName);
        firstEntry = false;
      } else {
        sb.append(", " + colName);
      }
    }
    return sb.toString();
  }

  public void computePartitionList(HiveConf conf, RexNode pruneNode) throws HiveException {
    partitionList = null;
    if (pruneNode == null) {
      return;
    }

    ExprNodeDesc pruneExpr = pruneNode.accept(new ExprNodeConverter(getName(), getRowType(), true));

    partitionList = PartitionPruner.prune(m_hiveTblMetadata, pruneExpr, conf, getName(),
        new HashMap<String, PrunedPartitionList>());
  }

  private void updateColStats(Set<Integer> projIndxLst) {
    List<String> nonPartColNamesThatRqrStats = new ArrayList<String>();
    List<Integer> nonPartColIndxsThatRqrStats = new ArrayList<Integer>();
    List<String> partColNamesThatRqrStats = new ArrayList<String>();
    List<Integer> partColIndxsThatRqrStats = new ArrayList<Integer>();
    Set<String> colNamesFailedStats = new HashSet<String>();

    // 1. Separate required columns to Non Partition and Partition Cols
    ColumnInfo tmp;
    for (Integer pi : projIndxLst) {
      if (m_hiveColStatsMap.get(pi) == null) {
        if ((tmp = m_hiveNonPartitionColsMap.get(pi)) != null) {
          nonPartColNamesThatRqrStats.add(tmp.getInternalName());
          nonPartColIndxsThatRqrStats.add(pi);
        } else if ((tmp = m_hivePartitionColsMap.get(pi)) != null) {
          partColNamesThatRqrStats.add(tmp.getInternalName());
          partColIndxsThatRqrStats.add(pi);
        } else {
          String logMsg = "Unable to find Column Index: " + pi + ", in "
              + m_hiveTblMetadata.getCompleteName();
          LOG.error(logMsg);
          throw new RuntimeException(logMsg);
        }
      }
    }

    // 2. Obtain Col Stats for Non Partition Cols
    if (nonPartColNamesThatRqrStats.size() > 0) {
      List<ColStatistics> hiveColStats;

      // 2.1 Handle the case where we are scanning only a set of partitions
      if (partitionList == null) {
        hiveColStats = StatsUtils.getTableColumnStats(m_hiveTblMetadata, m_hiveNonPartitionCols,
            nonPartColNamesThatRqrStats);

        // 2.1.1 Record Column Names that we needed stats for but couldn't
        if (hiveColStats == null) {
          colNamesFailedStats.addAll(nonPartColNamesThatRqrStats);
        } else if (hiveColStats.size() != nonPartColNamesThatRqrStats.size()) {
          Set<String> setOfFiledCols = new HashSet<String>(nonPartColNamesThatRqrStats);

          Set<String> setOfObtainedColStats = new HashSet<String>();
          for (ColStatistics cs : hiveColStats) {
            setOfObtainedColStats.add(cs.getColumnName());
          }
          setOfFiledCols.removeAll(setOfObtainedColStats);

          colNamesFailedStats.addAll(setOfFiledCols);
        }
      } else {
        // 2.2 Obtain col stats for full table scan
        try {
          Statistics stats = StatsUtils.collectStatistics(m_hiveConf, partitionList,
              m_hiveTblMetadata, m_hiveNonPartitionCols, nonPartColNamesThatRqrStats, true, true);
          m_rowCount = stats.getNumRows();
          hiveColStats = new ArrayList<ColStatistics>();
          for (String c : nonPartColNamesThatRqrStats) {
            ColStatistics cs = stats.getColumnStatisticsFromColName(c);
            if (cs != null) {
              hiveColStats.add(cs);
            } else {
              colNamesFailedStats.add(c);
            }
          }
        } catch (HiveException e) {
          String logMsg = "Collecting stats failed.";
          LOG.error(logMsg);
          throw new RuntimeException(logMsg);
        }
      }

      if (hiveColStats != null && hiveColStats.size() == nonPartColNamesThatRqrStats.size()) {
        for (int i = 0; i < hiveColStats.size(); i++) {
          m_hiveColStatsMap.put(nonPartColIndxsThatRqrStats.get(i), hiveColStats.get(i));
        }
      }
    }

    // 3. Obtain Stats for Partition Cols
    // TODO: Just using no of partitions for NDV is a gross approximation for
    // multi col partitions; Hack till HIVE-7392 gets fixed.
    if (colNamesFailedStats.isEmpty() && !partColNamesThatRqrStats.isEmpty()) {
      if (m_numPartitions == null) {
        try {
          if (partitionList != null) {
            m_numPartitions = partitionList.getPartitions().size();
          } else {
            m_numPartitions = Hive
                .get()
                .getPartitionNames(m_hiveTblMetadata.getDbName(), m_hiveTblMetadata.getTableName(),
                    (short) -1).size();
          }
        } catch (HiveException e) {
          String logMsg = "Could not get stats, number of Partitions for "
              + m_hiveTblMetadata.getCompleteName();
          LOG.error(logMsg);
          throw new RuntimeException(logMsg);
        }
      }

      ColStatistics cStats = null;
      for (int i = 0; i < partColNamesThatRqrStats.size(); i++) {
        cStats = new ColStatistics(m_hiveTblMetadata.getTableName(),
            partColNamesThatRqrStats.get(i), m_hivePartitionColsMap.get(
                partColIndxsThatRqrStats.get(i)).getTypeName());
        cStats.setCountDistint(m_numPartitions);

        m_hiveColStatsMap.put(partColIndxsThatRqrStats.get(i), cStats);
      }
    }

    // 4. Warn user if we could get stats for required columns
    if (!colNamesFailedStats.isEmpty()) {
      String logMsg = "No Stats for " + m_hiveTblMetadata.getCompleteName() + ", Columns: "
          + getColNamesForLogging(colNamesFailedStats);
      LOG.error(logMsg);
      throw new RuntimeException(logMsg);
    }
  }

  public List<ColStatistics> getColStat(List<Integer> projIndxLst) {
    ImmutableList.Builder<ColStatistics> colStatsBldr = ImmutableList.<ColStatistics> builder();

    if (projIndxLst != null) {
      updateColStats(new HashSet<Integer>(projIndxLst));
      for (Integer i : projIndxLst) {
        colStatsBldr.add(m_hiveColStatsMap.get(i));
      }
    } else {
      List<Integer> pILst = new ArrayList<Integer>();
      for (Integer i = 0; i < m_noOfProjs; i++) {
        pILst.add(i);
      }
      updateColStats(new HashSet<Integer>(pILst));
      for (Integer pi : pILst) {
        colStatsBldr.add(m_hiveColStatsMap.get(pi));
      }
    }

    return colStatsBldr.build();
  }

  /*
   * use to check if a set of columns are all partition columns. 
   * true only if:
   * - there is a prunedPartList in place
   * - all columns in BitSet are partition
   * columns.
   */
  public boolean containsPartitionColumnsOnly(BitSet cols) {
    if (partitionList == null) {
      return false;
    }
    for (int i = cols.nextSetBit(0); i >= 0; i++, i = cols.nextSetBit(i + 1)) {
      if (!m_hivePartitionColsMap.containsKey(i)) {
        return false;
      }
    }
    return true;
  }
}
