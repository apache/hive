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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * Marker work for Replication - behaves similar to CopyWork, but maps to ReplCopyTask,
 * which will have mechanics to list the files in source to write to the destination,
 * instead of copying them, if specified, falling back to copying if needed.
 */
@Explain(displayName = "Copy for Replication", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ReplCopyWork extends CopyWork {

  protected boolean copyFiles = true; // governs copy-or-list-files behaviour.
  // If set to true, behaves identically to a CopyWork
  // If set to false, ReplCopyTask does a file-list of the things to be copied instead, and puts them in a file called _files.
  // Default is set to mimic CopyTask, with the intent that any Replication code will explicitly flip this.

  /**
   * TODO : Refactor
   *
   * There is an upcoming patch that refactors this bit of code. Currently, the idea is the following:
   *
   * By default, ReplCopyWork will behave similarly to CopyWork, and simply copy
   * along data from the source to destination. If, however, listFilesOnOutput is set,
   * then, instead of copying the individual files to the destination, it simply creates
   * a file called _files on destination that contains the list of the original files
   * that were intended to be copied. Thus, we do not actually copy the files at CopyWork
   * time.
   *
   * The flip side of this behaviour happens when, instead, readListFromInput is set. This
   * flag, if set, changes the source behaviour of this CopyTask, and instead of copying
   * explicit files, this will then fall back to a behaviour wherein an _files is read from
   * the source, and the files specified by the _files are then copied to the destination.
   *
   * This allows us a lazy-copy-on-source and a pull-from destination semantic that we want
   * to use from replication.
   *
   * ==
   *
   * The refactor intent, however, is to simplify this, so that we have only 1 flag that we set,
   * called isLazy. If isLazy is set, then this is the equivalent of the current listFilesOnOutput,
   * and will generate a _files file.
   *
   * As to the input, we simply decide on whether to use the lazy mode or not depending on the
   * presence of a _files file on the input. If we see a _files on the input, we simply expand it
   * to copy as needed. If we do not, we copy as normal.
   *
   */

  protected boolean listFilesOnOutput = false; // governs copy-or-list-files behaviour
  // If set to true, it'll iterate over input files, and for each file in the input,
  //   it'll write out an additional line in a _files file in the output.
  // If set to false, it'll behave as a traditional CopyTask.

  protected boolean readListFromInput = false; // governs remote-fetch-input behaviour
  // If set to true, we'll assume that the input has a _files file present which lists
  //   the actual input files to copy, and we'll pull each of those on read.
  // If set to false, it'll behave as a traditional CopyTask.

  public ReplCopyWork() {
  }

  public ReplCopyWork(final Path fromPath, final Path toPath) {
    super(fromPath, toPath, true);
  }

  public ReplCopyWork(final Path fromPath, final Path toPath, boolean errorOnSrcEmpty) {
    super(fromPath, toPath, errorOnSrcEmpty);
  }

  public void setListFilesOnOutputBehaviour(boolean listFilesOnOutput){
    this.listFilesOnOutput = listFilesOnOutput;
  }

  public boolean getListFilesOnOutputBehaviour(){
    return this.listFilesOnOutput;
  }

  public void setReadListFromInput(boolean readListFromInput){
    this.readListFromInput = readListFromInput;
  }

  public boolean getReadListFromInput(){
    return this.readListFromInput;
  }

  // specialization of getListFilesOnOutputBehaviour, with a filestatus arg
  // we can default to the default getListFilesOnOutputBehaviour behaviour,
  // or, we can do additional pattern matching to decide that certain files
  // should not be listed, and copied instead, _metadata files, for instance.
  // Currently, we use this to skip _metadata files, but we might decide that
  // this is not the right place for it later on.
  public boolean getListFilesOnOutputBehaviour(FileStatus f) {
    if (f.getPath().toString().contains("_metadata")){
      return false; // always copy _metadata files
    }
    return this.listFilesOnOutput;
  }
}
