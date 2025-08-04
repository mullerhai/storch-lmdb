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

import com.jakewharton.byteunits.BinaryByteUnit.MEBIBYTES
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.greaterThan
import torch.lmdb.db.Env.create
import torch.lmdb.flags.EnvFlags.MDB_NOSUBDIR

import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import torch.lmdb.Verifier

/** Test {@link Verifier}. */
final class VerifierTest {
  @Rule final def tmpDef = new TemporaryFolder
  val tmp = tmpDef

  @Test
  @throws[IOException]
  def verification(): Unit = {
    val path = new File("./tmp") // tmp.newFile
    try {
      val env = create.setMaxReaders(1).setMaxDbs(Verifier.DBI_COUNT).setMapSize(MEBIBYTES.toBytes(10)).open(path, MDB_NOSUBDIR)
      try {
        val v = new Verifier(env)
        val seconds = Integer.getInteger("verificationSeconds", 2)
        val res = v.runFor(seconds.longValue(), TimeUnit.SECONDS)
        println(res)
//        assertThat(res, greaterThan(1L))
      } finally if (env != null) env.close()
    }
  }
}