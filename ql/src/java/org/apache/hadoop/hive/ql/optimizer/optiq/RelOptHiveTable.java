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
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableAccessRel;
import org.eigenbase.relopt.RelOptAbstractTable;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.reltype.RelDataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

/*
 * Fix Me: 
 * 1. Column Pruning
 * 2. Partition Pruning
 * 3. Stats
 */

public class RelOptHiveTable extends RelOptAbstractTable {
	private final Table m_hiveTblMetadata;
	private double m_rowCount = -1;
	private final ImmutableList<ColumnInfo> m_hiveNonPartitionCols;
	private final ImmutableMap<Integer, ColumnInfo> m_hiveNonPartitionColsMap;
	private final ImmutableMap<Integer, ColumnInfo> m_hivePartitionColsMap;
	Map<Integer, ColStatistics> m_hiveColStatsMap = new HashMap<Integer, ColStatistics>();
	private Integer m_numPartitions;
	private final int m_noOfProjs;

	protected static final Log LOG = LogFactory.getLog(RelOptHiveTable.class
			.getName());

	public RelOptHiveTable(RelOptSchema optiqSchema, String name,
			RelDataType rowType, Table hiveTblMetadata,
			List<ColumnInfo> hiveNonPartitionCols,
			List<ColumnInfo> hivePartitionCols) {
		super(optiqSchema, name, rowType);
		m_hiveTblMetadata = hiveTblMetadata;
		m_hiveNonPartitionCols = ImmutableList.copyOf(hiveNonPartitionCols);
		m_hiveNonPartitionColsMap = getColInfoMap(hiveNonPartitionCols, 0);
		m_hivePartitionColsMap = getColInfoMap(hivePartitionCols,
				m_hiveNonPartitionColsMap.size());
		m_noOfProjs = hiveNonPartitionCols.size() + hivePartitionCols.size();
	}

	private static ImmutableMap<Integer, ColumnInfo> getColInfoMap(
			List<ColumnInfo> hiveCols, int startIndx) {
		Builder<Integer, ColumnInfo> bldr = ImmutableMap
				.<Integer, ColumnInfo> builder();

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
		if (m_rowCount == -1)
			m_rowCount = StatsUtils.getNumRows(m_hiveTblMetadata);

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
					String logMsg = "Unable to find Column Index: " + pi
							+ ", in " + m_hiveTblMetadata.getCompleteName();
					LOG.error(logMsg);
					throw new RuntimeException(logMsg);
				}
			}
		}

		// 2. Obtain Col Stats for Non Partition Cols
		if (nonPartColNamesThatRqrStats.size() > 0) {
			List<ColStatistics> colStats = StatsUtils.getTableColumnStats(
					m_hiveTblMetadata, m_hiveNonPartitionCols,
					nonPartColNamesThatRqrStats);
			if (colStats != null
					&& colStats.size() == nonPartColNamesThatRqrStats.size()) {
				for (int i = 0; i < colStats.size(); i++) {
					m_hiveColStatsMap.put(nonPartColIndxsThatRqrStats.get(i),
							colStats.get(i));
				}
			} else {
				// TODO: colNamesFailedStats is designed to be used for both non
				// partitioned & partitioned cols; currently only used for non
				// partitioned cols.
				colNamesFailedStats.addAll(nonPartColNamesThatRqrStats);
			}
		}

		// 3. Obtain Stats for Partition Cols
		// TODO: Fix this as part of Partition Pruning
		if (!partColNamesThatRqrStats.isEmpty()) {
			if (m_numPartitions == null) {
				try {
					m_numPartitions = Hive
							.get()
							.getPartitionNames(m_hiveTblMetadata.getDbName(),
									m_hiveTblMetadata.getTableName(),
									(short) -1).size();
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
						partColNamesThatRqrStats.get(i), m_hivePartitionColsMap
								.get(partColIndxsThatRqrStats.get(i))
								.getTypeName());
				cStats.setCountDistint(m_numPartitions);

				m_hiveColStatsMap.put(partColIndxsThatRqrStats.get(i), cStats);
			}
		}

		// 4. Warn user if we could get stats for required columns
		if (!colNamesFailedStats.isEmpty()) {
			String logMsg = "No Stats for "
					+ m_hiveTblMetadata.getCompleteName() + ", Columns: "
					+ getColNamesForLogging(colNamesFailedStats);
			LOG.error(logMsg);
			throw new RuntimeException(logMsg);
		}
	}

	public List<ColStatistics> getColStat(List<Integer> projIndxLst) {
		List<ColStatistics> hiveColStatLst = new LinkedList<ColStatistics>();

		if (projIndxLst != null) {
			updateColStats(new HashSet<Integer>(projIndxLst));
			for (Integer i : projIndxLst) {
				hiveColStatLst.add(m_hiveColStatsMap.get(i));
			}
		} else {
			List<Integer> pILst = new ArrayList<Integer>();
			for (Integer i = 0; i < m_noOfProjs; i++) {
				pILst.add(i);
			}
			updateColStats(new HashSet<Integer>(pILst));
			for (Integer pi : pILst) {
				hiveColStatLst.add(m_hiveColStatsMap.get(pi));
			}
		}

		return hiveColStatLst;
	}
}
