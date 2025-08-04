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
import org.hamcrest.MatcherAssert.assertThat
import torch.lmdb.TargetName
import torch.lmdb.TargetName.isExternal
import torch.lmdb.TargetName.resolveFilename
//import torch.lmdb.TestUtils.invokePrivateConstructor
import org.junit.Test

/** Test {@link TargetName}. */
object TargetNameTest {
  private val NONE = ""
}

final class TargetNameTest {
  @Test def coverPrivateConstructors(): Unit = {
    TestUtils.invokePrivateConstructor(classOf[TargetName])
  }

  @Test def customEmbedded(): Unit = {
    assertThat(resolveFilename(TargetNameTest.NONE, "x/y.so", TargetNameTest.NONE, TargetNameTest.NONE), is("x/y.so"))
    assertThat(isExternal(TargetNameTest.NONE), is(false))
  }

  @Test def embeddedNameResolution(): Unit = {
    embed("aarch64-linux-gnu.so", "aarch64", "Linux")
    embed("aarch64-macos-none.so", "aarch64", "Mac OS")
    embed("x86_64-linux-gnu.so", "x86_64", "Linux")
    embed("x86_64-macos-none.so", "x86_64", "Mac OS")
    embed("x86_64-windows-gnu.dll", "x86_64", "Windows")
  }

  @Test def externalLibrary(): Unit = {
    assertThat(resolveFilename("/l.so", TargetNameTest.NONE, TargetNameTest.NONE, TargetNameTest.NONE), is("/l.so"))
    assertThat(TargetName.isExternal("/l.so"), is(true))
  }

  @Test def externalTakesPriority(): Unit = {
    assertThat(resolveFilename("/lm.so", "x/y.so", TargetNameTest.NONE, TargetNameTest.NONE), is("/lm.so"))
    assertThat(isExternal("/lm.so"), is(true))
  }

  private def embed(lib: String, arch: String, os: String): Unit = {
    assertThat(resolveFilename(TargetNameTest.NONE, TargetNameTest.NONE, arch, os), is("torch/lmdb/" + lib))
    assertThat(isExternal(TargetNameTest.NONE), is(false))
  }
}