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

import java.lang.Integer.MAX_VALUE
import org.hamcrest.CoreMatchers.containsString
import org.hamcrest.CoreMatchers.is
import org.hamcrest.CoreMatchers.notNullValue
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasSize
import org.junit.Assert.fail
import torch.lmdb.db.Cursor.FullException.MDB_CURSOR_FULL
import torch.lmdb.ResultCodeMapper.checkRc
import lmdbsuite.TestUtils.invokePrivateConstructor

import java.util
import org.junit.Test
import torch.lmdb.ResultCodeMapper
import torch.lmdb.db.Cursor.FullException
import torch.lmdb.db.Dbi.BadDbiException
import torch.lmdb.db.Dbi.BadValueSizeException
import torch.lmdb.db.Dbi.DbFullException
import torch.lmdb.db.Dbi.IncompatibleException
import torch.lmdb.db.Dbi.KeyExistsException
import torch.lmdb.db.Dbi.KeyNotFoundException
import torch.lmdb.db.Dbi.MapResizedException
import torch.lmdb.db.Env.FileInvalidException
import torch.lmdb.db.Env.MapFullException
import torch.lmdb.db.Env.ReadersFullException
import torch.lmdb.db.Env.VersionMismatchException
import torch.lmdb.db.{Cursor, Dbi, Env, Txn}
import torch.lmdb.exceptions.LmdbNativeException.ConstantDerivedException
import torch.lmdb.exceptions.LmdbNativeException.PageCorruptedException
import torch.lmdb.exceptions.LmdbNativeException.PageFullException
import torch.lmdb.exceptions.LmdbNativeException.PageNotFoundException
import torch.lmdb.exceptions.LmdbNativeException.PanicException
import torch.lmdb.exceptions.LmdbNativeException.TlsFullException
import torch.lmdb.db.Txn.BadException
import torch.lmdb.db.Txn.BadReaderLockException
import torch.lmdb.db.Txn.TxFullException
import torch.lmdb.exceptions.{LmdbException, LmdbNativeException}

import scala.collection.mutable

/** Test {@link ResultCodeMapper} and {@link LmdbException}. */
object ResultCodeMapperTest {
  private val EXCEPTIONS = new mutable.HashSet[LmdbNativeException]
  private val RESULT_CODES = new mutable.HashSet[Integer]
  try
    // separate collection instances used to simplify duplicate RC detection
    EXCEPTIONS.add(new Dbi.BadDbiException)
  EXCEPTIONS.add(new Txn.BadReaderLockException)
  EXCEPTIONS.add(new Txn.BadException)
  EXCEPTIONS.add(new Dbi.BadValueSizeException)
  EXCEPTIONS.add(new LmdbNativeException.PageCorruptedException)
  EXCEPTIONS.add(new Cursor.FullException)
  EXCEPTIONS.add(new Dbi.DbFullException)
  EXCEPTIONS.add(new Dbi.IncompatibleException)
  EXCEPTIONS.add(new Env.FileInvalidException)
  EXCEPTIONS.add(new Dbi.KeyExistsException)
  EXCEPTIONS.add(new Env.MapFullException)
  EXCEPTIONS.add(new Dbi.MapResizedException)
  EXCEPTIONS.add(new Dbi.KeyNotFoundException)
  EXCEPTIONS.add(new LmdbNativeException.PageFullException)
  EXCEPTIONS.add(new LmdbNativeException.PageNotFoundException)
  EXCEPTIONS.add(new LmdbNativeException.PanicException)
  EXCEPTIONS.add(new Env.ReadersFullException)
  EXCEPTIONS.add(new LmdbNativeException.TlsFullException)
  EXCEPTIONS.add(new Txn.TxFullException)
  EXCEPTIONS.add(new Env.VersionMismatchException)

//  import scala.collection.JavaConversions._

  for (e <- EXCEPTIONS) {
    RESULT_CODES.add(e.getResultCode)
  }
}

final class ResultCodeMapperTest {
  @Test def checkErrAll(): Unit = {
//    import scala.collection.JavaConversions._
    for (rc <- ResultCodeMapperTest.RESULT_CODES) {
      try {
        checkRc(rc)
        fail("Exception expected for RC " + rc)
      } catch {
        case e: LmdbNativeException =>
          assertThat(e.getResultCode, is(rc))
      }
    }
  }

  @Test(expected = classOf[LmdbNativeException.ConstantDerivedException]) def checkErrConstantDerived(): Unit = {
    checkRc(20)
  }

  @Test def checkErrConstantDerivedMessage(): Unit = {
    try {
      checkRc(2)
      fail("Should have raised exception")
    } catch {
      case ex: LmdbNativeException.ConstantDerivedException =>
        assertThat(ex.getMessage, containsString("No such file or directory"))
    }
  }

  @Test(expected = classOf[Cursor.FullException]) def checkErrCursorFull(): Unit = {
    checkRc(MDB_CURSOR_FULL)
  }

  @Test(expected = classOf[IllegalArgumentException]) def checkErrUnknownResultCode(): Unit = {
    checkRc(MAX_VALUE)
  }

  @Test def coverPrivateConstructors(): Unit = {
    invokePrivateConstructor(classOf[ResultCodeMapper])
  }

  @Test def lmdbExceptionPreservesRootCause(): Unit = {
    val cause = new IllegalStateException("root cause")
    val e = new LmdbException("test", cause)
    assertThat(e.getCause, is(cause))
    assertThat(e.getMessage, is("test"))
  }

  @Test def mapperReturnsUnique(): Unit = {
    val seen = new util.HashSet[LmdbNativeException]
//    import scala.collection.JavaConversions._
    for (rc <- ResultCodeMapperTest.RESULT_CODES) {
      try checkRc(rc)
      catch {
        case ex: LmdbNativeException =>
          assertThat(ex, is(notNullValue))
          seen.add(ex)
      }
    }
    assertThat(seen, hasSize(ResultCodeMapperTest.RESULT_CODES.size))
  }

  @Test def noDuplicateResultCodes(): Unit = {
    assertThat(ResultCodeMapperTest.RESULT_CODES.size, is(ResultCodeMapperTest.EXCEPTIONS.size))
  }
}