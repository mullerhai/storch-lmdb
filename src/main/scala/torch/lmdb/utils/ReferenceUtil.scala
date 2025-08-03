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
package torch.lmdb.utils

/** Supports creating strong references in manner compatible with Java 8. */
object ReferenceUtil {
  /**
   * Ensures that the object referenced by the given reference remains <em>strongly reachable</em>,
   * regardless of any prior actions of the program that might otherwise cause the object to become
   * unreachable. Thus, the referenced object is not reclaimable by garbage collection at least
   * until after the invocation of this method.
   *
   * <p>Recent versions of the JDK have a nasty habit of prematurely deciding objects are
   * unreachable (eg <a href="https://tinyurl.com/so26642153">StackOverflow question 26642153</a>.
   *
   * <p><code>java.lang.ref.Reference.reachabilityFence</code> offers a solution to this problem,
   * but it was only introduced in Java 9. torch.lmdb presently supports Java 8 and therefore this
   * method provides an alternative.
   *
   * <p>This method is always implemented as a synchronization on {@code ref}. <b>It is the caller's
   * responsibility to ensure that this synchronization will not cause deadlock.</b>
   *
   * @param ref the reference (null is acceptable but has no effect)
   * @see <a href="https://github.com/netty/netty/pull/8410">Netty PR 8410</a>
   */
    def reachabilityFence0(ref: AnyRef): Unit = {
      if (ref != null) ref.synchronized {

        // Empty synchronized is ok: https://stackoverflow.com/a/31933260/1151521
      }
    }
}

final class ReferenceUtil private {
}