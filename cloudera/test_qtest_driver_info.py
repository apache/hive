#!/usr/bin/python

import subprocess

DRIVERS = (
    "../itests/qtest/src/test/java/org/apache/hadoop/hive/cli/TestBeeLineDriver.java",
    "../itests/qtest/src/test/java/org/apache/hadoop/hive/cli/TestBlobstoreCliDriver.java",
    "../itests/qtest/src/test/java/org/apache/hadoop/hive/cli/TestBlobstoreNegativeCliDriver.java",
    "../itests/qtest/src/test/java/org/apache/hadoop/hive/cli/TestCliDriver.java",
    "../itests/qtest/src/test/java/org/apache/hadoop/hive/cli/TestCompareCliDriver.java",
    "../itests/qtest/src/test/java/org/apache/hadoop/hive/cli/TestContribCliDriver.java",
    "../itests/qtest/src/test/java/org/apache/hadoop/hive/cli/TestContribNegativeCliDriver.java",
    "../itests/qtest/src/test/java/org/apache/hadoop/hive/cli/TestEncryptedHDFSCliDriver.java",
    "../itests/qtest/src/test/java/org/apache/hadoop/hive/cli/TestHBaseCliDriver.java",
    "../itests/qtest/src/test/java/org/apache/hadoop/hive/cli/TestHBaseNegativeCliDriver.java",
    "../itests/qtest/src/test/java/org/apache/hadoop/hive/cli/TestHdfsBlobstoreCliDriver.java",
    "../itests/qtest/src/test/java/org/apache/hadoop/hive/cli/TestMinimrCliDriver.java",
    "../itests/qtest/src/test/java/org/apache/hadoop/hive/cli/TestMiniTezCliDriver.java",
    "../itests/qtest/src/test/java/org/apache/hadoop/hive/cli/TestNegativeCliDriver.java",
    "../itests/qtest/src/test/java/org/apache/hadoop/hive/cli/TestNegativeMinimrCliDriver.java",
    "../itests/qtest/src/test/java/org/apache/hadoop/hive/cli/TestTezPerfCliDriver.java",
    "../itests/qtest/src/test/java/org/apache/hadoop/hive/ql/parse/TestParseNegativeDriver.java",
    "../itests/qtest-spark/src/test/java/org/apache/hadoop/hive/cli/TestMiniSparkOnYarnCliDriver.java",
    "../itests/qtest-spark/src/test/java/org/apache/hadoop/hive/cli/TestSparkCliDriver.java",
    "../itests/qtest-spark/src/test/java/org/apache/hadoop/hive/cli/TestSparkNegativeCliDriver.java",
    "../itests/qtest-spark/src/test/java/org/apache/hadoop/hive/cli/TestSparkPerfCliDriver.java",
    "../ql/src/test/org/apache/hadoop/hive/ql/exec/TestExecDriver.java",
)

DRIVER_TEST_ARGS = (
    "python",
    "../cloudera/qtest-driver-info.py",
    "--hadoopVersion",
    '"hadoop-23"',
    "--properties",
    "../itests/src/test/resources/testconfiguration.properties",
    "--cliConfigsPath",
    "../itests/util/src/main/java/org/apache/hadoop/hive/cli/control/CliConfigs.java",
    "--driverClassPaths",
    ",".join(DRIVERS),
    "--paths",
)

TEST_CASES = {
    "ql/src/test/queries/clientpositive/alter_partition_coltype.q":
        [
            "TestCliDriver:alter_partition_coltype.q"
        ],
    "ql/src/test/results/clientpositive/exim_05_some_part.q.out":
        [
            "TestCliDriver:exim_05_some_part.q"
        ],
    "ql/src/test/queries/clientcompare/vectorized_math_funcs.q":
        [
            "TestCompareCliDriver:vectorized_math_funcs.q"
        ],
    "contrib/src/test/queries/clientpositive/udaf_example_group_concat.q":
        [
            "TestContribCliDriver:udaf_example_group_concat.q"
        ],
    "contrib/src/test/results/clientpositive/serde_typedbytes2.q.out":
        [
            "TestContribCliDriver:serde_typedbytes2.q"
        ],
    "contrib/src/test/queries/clientnegative/case_with_row_sequence.q":
        [
            "TestContribNegativeCliDriver:case_with_row_sequence.q"
        ],
    "contrib/src/test/results/clientnegative/invalid_row_sequence.q.out":
        [
            "TestContribNegativeCliDriver:invalid_row_sequence.q"
        ],
    "ql/src/test/queries/clientpositive/encryption_join_unencrypted_tbl.q":
        [
            "TestEncryptedHDFSCliDriver:encryption_join_unencrypted_tbl.q"
        ],
    "ql/src/test/results/clientpositive/encrypted/encryption_select_read_only_unencrypted_tbl.q.out":
        [
            "TestEncryptedHDFSCliDriver:encryption_select_read_only_unencrypted_tbl.q"
        ],
    "hbase-handler/src/test/queries/positive/hbase_custom_key.q":
        [
            "TestHBaseCliDriver:hbase_custom_key.q"
        ],
    "hbase-handler/src/test/results/positive/hbase_scan_params.q.out":
        [
            "TestHBaseCliDriver:hbase_scan_params.q"
        ],
    "hbase-handler/src/test/queries/negative/cascade_dbdrop.q":
        [
            "TestHBaseNegativeCliDriver:cascade_dbdrop.q"
        ],
    "hbase-handler/src/test/queries/negative/generatehfiles_require_family_path.q":
        [
            "TestHBaseNegativeCliDriver:generatehfiles_require_family_path.q"
        ],
    "ql/src/test/queries/clientpositive/bucket_num_reducers.q":
        [
            "TestMinimrCliDriver:bucket_num_reducers.q"
        ],
    "ql/src/test/results/clientpositive/table_nonprintable.q.out":
        [
            "TestMinimrCliDriver:table_nonprintable.q"
        ],
    "ql/src/test/queries/clientpositive/tez_bmj_schema_evolution.q":
        [
            ""
        ],
    "ql/src/test/results/clientpositive/tez/tez_union_decimal.q.out":
        [
            ""
        ],
    "ql/src/test/queries/clientnegative/select_charliteral.q":
        [
            "TestNegativeCliDriver:select_charliteral.q"
        ],
    "ql/src/test/results/clientnegative/exim_07_nonpart_noncompat_ifof.q.out":
        [
            "TestNegativeCliDriver:exim_07_nonpart_noncompat_ifof.q"
        ],
    "ql/src/test/queries/clientnegative/mapreduce_stack_trace.q":
        [
            "TestNegativeMinimrCliDriver:mapreduce_stack_trace.q"
        ],
    "ql/src/test/results/clientnegative/minimr_broken_pipe.q.out":
        [
            "TestNegativeMinimrCliDriver:minimr_broken_pipe.q"
        ],
    "ql/src/test/queries/clientpositive/constprog_semijoin.q":
        (
            "TestCliDriver:constprog_semijoin.q",
            "TestMiniSparkOnYarnCliDriver:constprog_semijoin.q"
        ),
    "ql/src/test/results/clientpositive/spark/orc_merge4.q.out":
        [
            "TestMiniSparkOnYarnCliDriver:orc_merge4.q"
        ],
    "ql/src/test/results/clientpositive/spark/auto_join20.q.out":
        [
            "TestSparkCliDriver:auto_join20.q"
        ],
    "ql/src/test/queries/clientnegative/groupby3_map_skew_multi_distinct.q":
        (
            "TestSparkNegativeCliDriver:groupby3_map_skew_multi_distinct.q",
            "TestNegativeCliDriver:groupby3_map_skew_multi_distinct.q"
        ),
    "ql/src/test/queries/clientpositive/skewjoinopt14.q":
        (
            "TestCliDriver:skewjoinopt14.q",
            "TestSparkCliDriver:skewjoinopt14.q"
        ),
    "ql/src/test/results/clientnegative/groupby3_multi_distinct.q.out":
        [
            "TestNegativeCliDriver:groupby3_multi_distinct.q"
        ],
    "ql/src/test/queries/clientpositive/union34.q,contrib/src/test/queries/clientpositive/udtf_output_on_close.q,ql/src/test/queries/clientpositive/encryption_join_unencrypted_tbl.q,hbase-handler/src/test/queries/positive/hbase_bulk.q,ql/src/test/queries/clientpositive/udf_using.q,ql/src/test/queries/clientpositive/tez_union_decimal.q,ql/src/test/queries/clientnegative/create_table_failure1.q,ql/src/test/queries/clientpositive/constprog_semijoin.q,ql/src/test/queries/clientpositive/groupby_multi_single_reducer3.q":
        (
            "TestEncryptedHDFSCliDriver:encryption_join_unencrypted_tbl.q",
            "TestHBaseCliDriver:hbase_bulk.q",
            "TestNegativeCliDriver:create_table_failure1.q",
            "TestMiniSparkOnYarnCliDriver:constprog_semijoin.q",
            "TestContribCliDriver:udtf_output_on_close.q",
            "TestCliDriver:union34.q,constprog_semijoin.q,groupby_multi_single_reducer3.q",
            "TestSparkCliDriver:union34.q,groupby_multi_single_reducer3.q",
            "TestMinimrCliDriver:udf_using.q"
        ),
    "ql/src/test/queries/clientpositive/groupby2.q":
        (
            "TestSparkCliDriver:groupby2.q",
            "TestMinimrCliDriver:groupby2.q"
        ),
    "ql/src/test/results/clientpositive/join1.q.out":
        [
            "TestMinimrCliDriver:join1.q"
        ],
    "ql/src/test/queries/clientpositive/union_remove_8.q":
        (
            "TestCliDriver:union_remove_8.q",
            "TestSparkCliDriver:union_remove_8.q"
        ),
    "ql/src/test/results/clientpositive/limit_partition_metadataonly.q.out":
        [
            "TestCliDriver:limit_partition_metadataonly.q"
        ],
    "ql/src/test/queries/clientpositive/bucketmapjoin7.q":
        (
            "TestMinimrCliDriver:bucketmapjoin7.q",
            "TestSparkCliDriver:bucketmapjoin7.q",
            "TestMiniSparkOnYarnCliDriver:bucketmapjoin7.q"
        ),
    "ql/src/test/results/clientpositive/stats_counter.q.out":
        [
            "TestMinimrCliDriver:stats_counter.q"
        ],
    "ql/src/test/queries/clientpositive/char_join1.q":
        [
            "TestCliDriver:char_join1.q"
        ],
    "ql/src/test/queries/clientpositive/varchar_join1.q":
        (
            "TestCliDriver:varchar_join1.q",
            "TestSparkCliDriver:varchar_join1.q"
        ),
    "ql/src/test/queries/clientpositive/ctas.q":
        (
            "TestCliDriver:ctas.q",
            "TestSparkCliDriver:ctas.q"
        ),
    "ql/src/test/queries/clientpositive/encryption_ctas.q":
        [
            "TestEncryptedHDFSCliDriver:encryption_ctas.q"
        ],
    "ql/src/test/queries/clientpositive/drop_table.q":
        [
            "TestCliDriver:drop_table.q"
        ],
    "ql/src/test/queries/clientpositive/encryption_drop_table.q":
        [
            "TestEncryptedHDFSCliDriver:encryption_drop_table.q"
        ],
    "ql/src/test/queries/clientpositive/join6.q":
        (
            "TestCliDriver:join6.q",
            "TestSparkCliDriver:join6.q"
        ),
    "ql/src/test/queries/clientpositive/bucketmapjoin6.q":
        (
            "TestMiniSparkOnYarnCliDriver:bucketmapjoin6.q",
            "TestMinimrCliDriver:bucketmapjoin6.q"
        ),
    "ql/src/test/queries/clientpositive/join_nulls.q":
        [
            "TestCliDriver:join_nulls.q"
        ],
    "ql/src/test/queries/clientpositive/auto_join_nulls.q":
        (
            "TestCliDriver:auto_join_nulls.q",
            "TestSparkCliDriver:auto_join_nulls.q"
        ),
    "ql/src/test/queries/clientpositive/merge2.q":
        (
            "TestCliDriver:merge2.q",
            "TestSparkCliDriver:merge2.q"
        ),
    "ql/src/test/queries/clientpositive/orc_merge2.q":
        (
            "TestCliDriver:orc_merge2.q",
            "TestMiniSparkOnYarnCliDriver:orc_merge2.q"
        ),
    "ql/src/test/queries/clientpositive/source.q":
        [
            "TestCliDriver:source.q"
        ],
    "ql/src/test/queries/clientpositive/perf/query11.q":
        [
            "TestSparkPerfCliDriver:query11.q"
        ],
    "ql/src/test/results/clientpositive/perf/spark/query4.q.out":
        [
            "TestSparkPerfCliDriver:query4.q"
        ],
    "ql/src/test/queries/clientnegative/udf_local_resource.q":
        [
            "TestNegativeMinimrCliDriver:udf_local_resource.q"
        ]
}

def verify_output(test_output, expected_output):
    parsed_output = test_output.split("\n")
    sorted_actual = sorted(parsed_output)
    sorted_expected = sorted(expected_output)
    if sorted_actual == sorted_expected:
        return "OK"
    else:
        return "FAILED - actual: %s, expected: %s" % (",".join(sorted_actual), ",".join(sorted_expected))

def run_test(test_input, expected_output):
    cmd = []
    cmd.extend(DRIVER_TEST_ARGS)
    cmd.append(test_input)
    test_output = subprocess.check_output(cmd, universal_newlines=True)
    print "\nTest '%s': %s" % (test_input, verify_output(test_output.strip(), expected_output))

def test_all():
    for test_input, test_output in TEST_CASES.iteritems():
        run_test(test_input, test_output)

def main():
    test_all()

if __name__ == "__main__":
    main()
