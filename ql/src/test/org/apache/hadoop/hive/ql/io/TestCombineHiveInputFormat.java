/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Unittest for CombineHiveInputFormat.
 */
public class TestCombineHiveInputFormat extends TestCase {
    public void testAvoidSplitCombination() throws Exception {
        Configuration conf = new Configuration();
        JobConf job = new JobConf(conf);

        TableDesc tblDesc = Utilities.defaultTd;
        tblDesc.setInputFileFormatClass(TestSkipCombineInputFormat.class);
        PartitionDesc partDesc = new PartitionDesc(tblDesc, null);
        LinkedHashMap<Path, PartitionDesc> pt = new LinkedHashMap<>();
        pt.put(new Path("/tmp/testfolder1"), partDesc);
        pt.put(new Path("/tmp/testfolder2"), partDesc);
        MapredWork mrwork = new MapredWork();
        mrwork.getMapWork().setPathToPartitionInfo(pt);
        Path mapWorkPath = new Path("/tmp/" + System.getProperty("user.name"), "hive");
        Utilities.setMapRedWork(conf, mrwork,
            mapWorkPath);

        try {
            Path[] paths = new Path[2];
            paths[0] = new Path("/tmp/testfolder1");
            paths[1] = new Path("/tmp/testfolder2");
            CombineHiveInputFormat combineInputFormat =
                ReflectionUtils.newInstance(CombineHiveInputFormat.class, conf);
            combineInputFormat.pathToPartitionInfo =
                Utilities.getMapWork(conf).getPathToPartitionInfo();
            Set results = combineInputFormat.getNonCombinablePathIndices(job, paths, 2);
            assertEquals("Should have both path indices in the results set", 2, results.size());
        } finally {
            // Cleanup the mapwork path
            FileSystem.get(conf).delete(mapWorkPath, true);
        }
    }

    public static class TestSkipCombineInputFormat extends FileInputFormat
        implements CombineHiveInputFormat.AvoidSplitCombination {
        @Override public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf,
            Reporter reporter) throws IOException {
            return null;
        }

        @Override public boolean shouldSkipCombine(Path path, Configuration conf)
            throws IOException {
            // Skip combine for all paths
            return true;
        }
    }
}
