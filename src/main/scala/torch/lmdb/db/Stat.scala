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
package lmdb.db

/** Statistics, as returned by {@link Env# stat ( )} and {@link Dbi# stat ( org.torch.lmdb.Txn )}. */
final class Stat private[torch](/** Size of a database page. This is currently the same for all databases. */
                                   val pageSize: Int,

                                   /** Depth (height) of the B-tree. */
                                   val depth: Int,

                                   /** Number of internal (non-leaf) pages. */
                                   val branchPages: Long,

                                   /** Number of leaf pages. */
                                   val leafPages: Long,

                                   /** Number of overflow pages. */
                                   val overflowPages: Long,

                                   /** Number of data items. */
                                   val entries: Long) {
  override def toString: String = "Stat{" + "branchPages=" + branchPages + ", depth=" + depth + ", entries=" + entries + ", leafPages=" + leafPages + ", overflowPages=" + overflowPages + ", pageSize=" + pageSize + '}'
}