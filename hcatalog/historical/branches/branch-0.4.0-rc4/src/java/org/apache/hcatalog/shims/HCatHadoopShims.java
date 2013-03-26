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
package org.apache.hcatalog.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * Shim layer to abstract differences between Hadoop 0.20 and 0.23 (HCATALOG-179).
 * This mirrors Hive shims, but is kept separate for HCatalog dependencies.
 **/
public interface HCatHadoopShims {

	public static abstract class Instance {
		static HCatHadoopShims instance = selectShim();
		public static HCatHadoopShims get() {
			return instance;
		}

		private static HCatHadoopShims selectShim() {
			// piggyback on Hive's detection logic
			String major = ShimLoader.getMajorVersion();
			String shimFQN = "org.apache.hcatalog.shims.HCatHadoopShims20S";
			if (major.startsWith("0.23")) {
				shimFQN = "org.apache.hcatalog.shims.HCatHadoopShims23";
			}
			try {
				Class<? extends HCatHadoopShims> clasz =
						Class.forName(shimFQN).asSubclass(HCatHadoopShims.class);
				return clasz.newInstance();
			} catch (Exception e) {
				throw new RuntimeException("Failed to instantiate: " + shimFQN, e);
			}
		}
	}

    public TaskAttemptContext createTaskAttemptContext(Configuration conf,
                                TaskAttemptID taskId);

    public JobContext createJobContext(Configuration conf,
            JobID jobId);

}
