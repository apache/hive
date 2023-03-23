package org.apache.hadoop.hive.metastore.txn.dbUtils.resultsetHandlers;

import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.hadoop.hive.metastore.api.*;

import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.LockTypeUtil;


import java.sql.ResultSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ShowLockResultsetHandler implements ResultSetHandler {


    public Object handle(final ResultSet rs) throws SQLException {

        List<TxnUtils.LockInfoExt> sortedList = new ArrayList<>();
        while (rs.next()) {
            ShowLocksResponseElement e = new ShowLocksResponseElement();
            e.setLockid(rs.getLong(1));
            long txnid = rs.getLong(2);
            if (!rs.wasNull()) e.setTxnid(txnid);
            e.setDbname(rs.getString(3));
            e.setTablename(rs.getString(4));
            String partition = rs.getString(5);
            if (partition != null) e.setPartname(partition);
            switch (rs.getString(6).charAt(0)) {
                case TxnUtils.LOCK_ACQUIRED:
                    e.setState(LockState.ACQUIRED);
                    break;
                case TxnUtils.LOCK_WAITING:
                    e.setState(LockState.WAITING);
                    break;
                default:

                    throw new SQLException("Unknown lock state " + rs.getString(6).charAt(0));
            }
            char lockChar = rs.getString(7).charAt(0);
            LockType lockType = LockTypeUtil.getLockTypeFromEncoding(lockChar)
                    .orElseThrow(() -> new SQLException("Unknown lock type: " + lockChar));
            e.setType(lockType);

            e.setLastheartbeat(rs.getLong(8));
            long acquiredAt = rs.getLong(9);
            if (!rs.wasNull()) e.setAcquiredat(acquiredAt);
            e.setUser(rs.getString(10));
            e.setHostname(rs.getString(11));
            e.setLockIdInternal(rs.getLong(12));
            long id = rs.getLong(13);
            if (!rs.wasNull()) {
                e.setBlockedByExtId(id);
            }
            id = rs.getLong(14);
            if (!rs.wasNull()) {
                e.setBlockedByIntId(id);
            }
            e.setAgentInfo(rs.getString(15));
            sortedList.add(new TxnUtils.LockInfoExt(e));
        }
        return sortedList;
    }


}

