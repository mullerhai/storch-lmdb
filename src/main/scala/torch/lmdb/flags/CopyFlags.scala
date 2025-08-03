/*
 * Copyright © 2016-2025 The torch.lmdb Open Source Project
 *
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
package torch
package lmdb.flags

/** Flags for use when performing a {@link Env# copy ( java.io.File, org.torch.lmdb.CopyFlags...)}. */
enum CopyFlags (mask: Int) extends MaskedFlag {
  /**
   * Obtains the integer value for this enum which can be included in a mask.
   *
   * @return the integer value for combination into a mask
   */
  case MDB_CP_COMPACT extends CopyFlags(0x1)
  override def getMask: Int = mask

//  override def ordinal: Int = ???
} 
  /** Copy the data file. */

 

