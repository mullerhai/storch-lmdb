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
package lmdb.exceptions

import java.lang.String.format

/** Superclass for all exceptions that originate from a native C call. */
@SerialVersionUID(1L)
object LmdbNativeException {
  /** Exception raised from a system constant table lookup. */
  @SerialVersionUID(1L)
  final class ConstantDerivedException (rc: Int, message: String) extends LmdbNativeException(rc, "Platform constant error code: " + message) {
  }

  /** Located page was wrong type. */
  @SerialVersionUID(1L)
  object PageCorruptedException {
    val MDB_CORRUPTED = -30_796
  }

  @SerialVersionUID(1L)
  final class PageCorruptedException extends LmdbNativeException(PageCorruptedException.MDB_CORRUPTED, "located page was wrong type") {
  }

  /** Page has not enough space - internal error. */
  @SerialVersionUID(1L)
  object PageFullException {
    val MDB_PAGE_FULL = -30_786
  }

  @SerialVersionUID(1L)
  final class PageFullException extends LmdbNativeException(PageFullException.MDB_PAGE_FULL, "Page has not enough space - internal error") {
  }

  /** Requested page not found - this usually indicates corruption. */
  @SerialVersionUID(1L)
  object PageNotFoundException {
    val MDB_PAGE_NOTFOUND = -30_797
  }

  @SerialVersionUID(1L)
  final class PageNotFoundException extends LmdbNativeException(PageNotFoundException.MDB_PAGE_NOTFOUND, "Requested page not found - this usually indicates corruption") {
  }

  /** Update of meta page failed or environment had fatal error. */
  @SerialVersionUID(1L)
  object PanicException {
    val MDB_PANIC = -30_795
  }

  @SerialVersionUID(1L)
  final class PanicException extends LmdbNativeException(PanicException.MDB_PANIC, "Update of meta page failed or environment had fatal error") {
  }

  /** Too many TLS keys in use - Windows only. */
  @SerialVersionUID(1L)
  object TlsFullException {
    val MDB_TLS_FULL = -30_789
  }

  @SerialVersionUID(1L)
  final class TlsFullException extends LmdbNativeException(TlsFullException.MDB_TLS_FULL, "Too many TLS keys in use - Windows only") {
  }
}

@SerialVersionUID(1L)
class LmdbNativeException (/** Result code returned by the LMDB C function. */
                                            private val rc: Int, msg: String)

/**
 * Constructs an instance with the provided detailed message.
 *
 * @param msg the detail message.
 * @param rc  the result code.
 */
  extends LmdbException(format(msg + " (%d)", rc)) {
  /**
   * Obtain the LMDB C-side result code.
   *
   * @return the C-side result code
   */
  final def getResultCode: Int = rc
}