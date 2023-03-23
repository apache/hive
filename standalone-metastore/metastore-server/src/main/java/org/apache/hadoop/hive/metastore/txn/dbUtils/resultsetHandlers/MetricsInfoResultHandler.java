package org.apache.hadoop.hive.metastore.txn.dbUtils.resultsetHandlers;

import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.BeanProcessor;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.MetricsInfo;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class MetricsInfoResultHandler implements ResultSetHandler {


    public Object handle(final ResultSet rs) throws SQLException {

        MetricsInfo metrics = new MetricsInfo();
        if (rs.next()) {
            metrics.setTxnToWriteIdCount(rs.getInt(1));
            metrics.setCompletedTxnsCount(rs.getInt(2));
            metrics.setOpenReplTxnsCount(rs.getInt(3));
            metrics.setOldestOpenReplTxnId(rs.getInt(4));
            metrics.setOldestOpenReplTxnAge(rs.getInt(5));
            metrics.setOpenNonReplTxnsCount(rs.getInt(6));
            metrics.setOldestOpenNonReplTxnId(rs.getInt(7));
            metrics.setOldestOpenNonReplTxnAge(rs.getInt(8));
            metrics.setAbortedTxnsCount(rs.getInt(9));
            metrics.setOldestAbortedTxnId(rs.getInt(10));
            metrics.setOldestAbortedTxnAge(rs.getInt(11));
            metrics.setLocksCount(rs.getInt(12));
            metrics.setOldestLockAge(rs.getInt(13));
            metrics.setOldestReadyForCleaningAge(rs.getInt(14));
        }
        return metrics;
    }
}
