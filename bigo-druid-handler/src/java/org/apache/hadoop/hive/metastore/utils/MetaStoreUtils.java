package org.apache.hadoop.hive.metastore.utils;

import org.apache.hadoop.hive.metastore.api.Table;

import java.util.Map;

public class MetaStoreUtils {
    public static final String EXTERNAL_TABLE_PURGE = "external.table.purge";

    /**
     * Determines whether an table needs to be purged or not.
     *
     * @param table table of interest
     *
     * @return true if external table needs to be purged
     */
    public static boolean isExternalTablePurge(Table table) {
        if (table == null) {
            return false;
        }
        Map<String, String> params = table.getParameters();
        if (params == null) {
            return false;
        }

        return isPropertyTrue(params, EXTERNAL_TABLE_PURGE);
    }

    public static boolean isPropertyTrue(Map<String, String> tableParams, String prop) {
        return "TRUE".equalsIgnoreCase(tableParams.get(prop));
    }
}
