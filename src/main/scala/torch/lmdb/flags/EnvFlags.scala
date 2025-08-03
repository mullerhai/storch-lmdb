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

/** Flags for use when opening the {@link Env}. */
enum EnvFlags(val mask: Int) extends MaskedFlag {
  case MDB_FIXEDMAP extends EnvFlags(0x01)
  case MDB_NOSUBDIR extends EnvFlags(0x4000)
  case MDB_RDONLY_ENV extends EnvFlags(0x20000)
  case MDB_WRITEMAP extends EnvFlags(0x80000)
  case MDB_NOMETASYNC extends EnvFlags(0x40000)
  case MDB_NOSYNC extends EnvFlags(0x10000)
  case MDB_MAPASYNC extends EnvFlags(0x100000)
  case MDB_NOTLS extends EnvFlags(0x200000)
  case MDB_NOLOCK extends EnvFlags(0x400000)
  case MDB_NORDAHEAD extends EnvFlags(0x800000)
  case MDB_NOMEMINIT extends EnvFlags(0x1000000)

  override def getMask: Int = mask
}