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
package lmdbsuite

import org.hamcrest.CoreMatchers.is
import org.hamcrest.CoreMatchers.not
import org.hamcrest.CoreMatchers.nullValue
import org.hamcrest.MatcherAssert.assertThat
import torch.lmdb.exceptions.LmdbNativeException.PageCorruptedException.MDB_CORRUPTED
import torch.lmdb.Meta.{Meta}
import lmdbsuite.TestUtils.invokePrivateConstructor
import org.junit.Test

/** Test {@link Meta}. */
final class MetaTest {
  @Test def coverPrivateConstructors(): Unit = {
    invokePrivateConstructor(classOf[Meta])
  }

  @Test def errCode(): Unit = {
    assertThat(Meta.error(MDB_CORRUPTED), is("MDB_CORRUPTED: Located page was wrong type"))
  }

  @Test def version(): Unit = {
    val v: Meta.Version = Meta.version
    assertThat(v, not(nullValue))
    assertThat(v.major, is(0))
    assertThat(v.minor, is(9))
  }
}