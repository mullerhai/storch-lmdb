/*
 * Copyright © 2016-2025 The LmdbJava Open Source Project
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
package torch.lmdb

import torch.lmdb.Library.LIB
import jnr.ffi.byref.IntByReference

/** LMDB metadata functions. */
object Meta {
  /**
   * Fetches the LMDB error code description.
   *
   * <p>End users should not need this method, as LmdbJava converts all LMDB exceptions into a typed
   * Java exception that incorporates the error code. However it is provided here for verification
   * and troubleshooting (eg if the user wishes to see the original LMDB description of the error
   * code, or there is a newer library version etc).
   *
   * @param err the error code returned from LMDB
   * @return the description
   */
    def error(err: Int): String = LIB.mdb_strerror(err)

  /**
   * Obtains the LMDB C library version information.
   *
   * @return the version data
   */
  def version: Meta.Version = {
    val major = new IntByReference
    val minor = new IntByReference
    val patch = new IntByReference
    LIB.mdb_version(major, minor, patch)
    new Meta.Version(major.intValue, minor.intValue, patch.intValue)
  }

  /** Immutable return value from {@link # version ( )}. */
  final class Version private[torch](
                                         /** LMDC native library major version number. */
                                         val major: Int,

                                         /** LMDC native library patch version number. */
                                         val minor: Int,

                                         /** LMDC native library patch version number. */
                                         val patch: Int) {
  }
}

final class Meta private {
}