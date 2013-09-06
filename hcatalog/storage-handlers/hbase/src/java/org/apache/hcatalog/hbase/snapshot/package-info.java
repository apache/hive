/**
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * Provides a revision manager for data stored in HBase that can be used to implement repeatable reads.
 * The component is designed to be usable for revision management of data stored in HBase in general,
 * independent and not limited to HCatalog. It is used by the HCatalog HBase storage handler, implementation depends on HBase 0.92+.
 * <p>
 * For more information please see 
 * <a href="https://cwiki.apache.org/confluence/display/HCATALOG/Snapshots+and+Repeatable+reads+for+HBase+Tables">Snapshots and Repeatable reads for HBase Tables</a>.
 * @since 0.4
 */
package org.apache.hcatalog.hbase.snapshot;
