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

import java.lang.Boolean.getBoolean
import java.lang.reflect.Field
import sun.misc.Unsafe

/** Provides access to Unsafe. */
object UnsafeAccess {
  /** Java system property name that can be set to disable unsafe. */
  val DISABLE_UNSAFE_PROP = "lmdbjava.disable.unsafe"
  /** Indicates whether unsafe use is allowed. */
  val ALLOW_UNSAFE: Boolean = !getBoolean(DISABLE_UNSAFE_PROP)
  /**
   * The actual unsafe. Guaranteed to be non-null if this class can access unsafe and {@link 
 * #ALLOW_UNSAFE} is true. In other words, this entire class will fail to initialize if unsafe is
   * unavailable. This avoids callers from needing to deal with null checks.
   */
  private[torch] var UNSAFE: Unsafe = null
  /** Unsafe field name (used to reflectively obtain the unsafe instance). */
  private val FIELD_NAME_THE_UNSAFE = "theUnsafe"
  try if (!ALLOW_UNSAFE) throw new LmdbException("Unsafe disabled by user")
  try {
    val field = classOf[Unsafe].getDeclaredField(FIELD_NAME_THE_UNSAFE)
    field.setAccessible(true)
    UNSAFE = field.get(null).asInstanceOf[Unsafe]
  } catch {
    case e@(_: NoSuchFieldException | _: SecurityException | _: IllegalArgumentException | _: IllegalAccessException) =>
      throw new LmdbException("Unsafe unavailable", e)
  }
}

final class UnsafeAccess private {
}