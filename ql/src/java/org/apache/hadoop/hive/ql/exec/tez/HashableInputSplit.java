/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.tez;

/**
 * An InputSplit type that has a custom/specific way of producing its serialized content, to be later used as input
 * of a hash function. Used for LLAP cache affinity, so that a certain split always ends up on the same executor.
 */
public interface HashableInputSplit {

  byte[] getBytesForHash();

}
