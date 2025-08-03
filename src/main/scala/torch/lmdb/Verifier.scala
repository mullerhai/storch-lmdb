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
package torch.lmdb

import torch.lmdb.db.{Dbi, Env, Txn}

import java.util.Objects.requireNonNull
import torch.lmdb.flags.DbiFlags.MDB_CREATE
import torch.lmdb.exceptions.LmdbException

import java.nio.{ByteBuffer, ByteOrder}
import java.util
import java.util.Random
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.CRC32

/**
 * Verifies correct operation of torch.lmdb in a given environment.
 *
 * <p>Due to the large variety of operating systems and Java platforms typically used with torch.lmdb,
 * this class provides a convenient verification of correct operating behavior through a potentially
 * long duration set of tests that carefully verify correct storage and retrieval of successively
 * larger database entries.
 *
 * <p>The verifier currently operates by incrementing a <code>long</code> identifier that
 * deterministically maps to a given {@link Dbi} and value size. The key is simply the <code>long
 * </code> identifier. The value commences with a CRC that includes the identifier and the random
 * bytes of the value. Each entry is written out, and then the prior entry is retrieved using its
 * key. The prior entry's value is evaluated for accuracy and then deleted. Transactions are
 * committed in batches to ensure successive transactions correctly retrieve the results of earlier
 * transactions.
 *
 * <p>Please note the verification approach may be modified in the future.
 *
 * <p>If an exception is raised by this class, please:
 *
 * <ol>
 * <li>Ensure the {@link Env} passed at construction time complies with the requirements specified
 * at {@link # Verifier ( org.torch.lmdb.Env )}
 * <li>Attempt to use a different file system to store the database (be especially careful to not
 * use network file systems, remote file systems, read-only file systems etc)
 * <li>Record the full exception message and stack trace, then run the verifier again to see if it
 * fails at the same or a different point
 * <li>Raise a ticket on the torch.lmdb Issue Tracker that confirms the above details along with the
 * failing operating system and Java version
 * </ol>
 */
object Verifier {
  /** Number of DBIs the created environment should allow. */
  val DBI_COUNT = 5
  private val BATCH_SIZE = 64
  private val BUFFER_LEN = 1_024 * BATCH_SIZE
  private val CRC_LENGTH = java.lang.Long.BYTES
  private val KEY_LENGTH = java.lang.Long.BYTES
}

final class Verifier(private val env: Env[ByteBuffer]) extends Callable[Long] {
  requireNonNull(env)
  key.order(ByteOrder.BIG_ENDIAN)
  deleteDbis()
  createDbis()
  final private val ba = new Array[Byte](Verifier.BUFFER_LEN)
  final private val crc = new CRC32
  final private val dbis = new util.ArrayList[Dbi[ByteBuffer]](Verifier.DBI_COUNT)
  private var id = 0L
  final private val key = ByteBuffer.allocateDirect(Verifier.KEY_LENGTH)
  final private val proceed = new AtomicBoolean(true)
  final private val rnd = new Random
  private var txn: Txn[ByteBuffer] = null
  final private val vals = ByteBuffer.allocateDirect(Verifier.BUFFER_LEN)

  /**
   * Run the verifier until {@link # stop ( )} is called or an exception occurs.
   *
   * <p>Successful return of this method indicates no faults were detected. If any fault was
   * detected the exception message will detail the exact point that the fault was encountered.
   *
   * @return number of database rows successfully verified
   */
  override def call: Long = {
    try while (proceed.get) {
      transactionControl()
      write(id)
      if (id > 0) fetchAndDelete(id - 1)
      id += 1
    }
    finally if (txn != null) txn.close()
    id
  }

  /**
   * Execute the verifier for the given duration.
   *
   * <p>This provides a simple way to execute the verifier for those applications which do not wish
   * to manage threads directly.
   *
   * @param duration amount of time to execute
   * @param unit     units used to express the duration
   * @return number of database rows successfully verified
   */
  def runFor(duration: Long, unit: TimeUnit): Long = {
    val deadline = System.currentTimeMillis + unit.toMillis(duration)
    val es = Executors.newSingleThreadExecutor
    val future = es.submit(this)
    try while (System.currentTimeMillis < deadline && !future.isDone) Thread.sleep(unit.toMillis(1))
    catch {
      case ignored: InterruptedException =>
    } finally stop()
    var result = 0L
    try result = future.get
    catch {
      case ex@(_: InterruptedException | _: ExecutionException) =>
        throw new IllegalStateException(ex)
    } finally es.shutdown()
    result
  }

  private def createDbis(): Unit = {
    for (i <- 0 until Verifier.DBI_COUNT) {
      dbis.add(env.openDbi(classOf[Verifier].getSimpleName + i, MDB_CREATE))
    }
  }

  private def deleteDbis(): Unit = {

    for (existingDbiName <- env.getDbiNames) {
      val existingDbi = env.openDbi(existingDbiName)
      try {
        val txn = env.txnWrite
        try {
          existingDbi.drop(txn, true)
          txn.commit()
        } finally if (txn != null) txn.close()
      }
    }
  }

  private def fetchAndDelete(forId: Long): Unit = {
    val dbi = getDbi(forId)
    updateKey(forId)
    var fetchedValue: ByteBuffer = null
    try fetchedValue = dbi.get(txn, key).get
    catch {
      case ex: LmdbException =>
        throw new IllegalStateException("DB get id=" + forId, ex)
    }
    if (fetchedValue == null) throw new IllegalStateException("DB not found id=" + forId)
    verifyValue(forId, fetchedValue)
    try dbi.delete(txn, key)
    catch {
      case ex: LmdbException =>
        throw new IllegalStateException("DB del id=" + forId, ex)
    }
  }

  private def getDbi(forId: Long) = dbis.get((forId % dbis.size).toInt)

  /** Request the verifier to stop execution. */
  private def stop(): Unit = {
    proceed.set(false)
  }

  private def transactionControl(): Unit = {
    if (id % Verifier.BATCH_SIZE == 0) {
      if (txn != null) {
        txn.commit()
        txn.close()
      }
      rnd.nextBytes(ba)
      txn = env.txnWrite
    }
  }

  private def updateKey(forId: Long): Unit = {
    key.clear
    key.putLong(forId)
    key.flip
  }

  private def updateValue(forId: Long): Unit = {
    val rndSize = valueSize(forId)
    crc.reset()
    crc.update(forId.toInt)
    crc.update(ba, Verifier.CRC_LENGTH, rndSize)
    val crcVal = crc.getValue
    vals.clear
    vals.putLong(crcVal)
    vals.put(ba, Verifier.CRC_LENGTH, rndSize)
    vals.flip
  }

  private def valueSize(forId: Long) = {
    val mod = (forId % Verifier.BATCH_SIZE).toInt
    val base = 1_024 * mod
    val value = if (base == 0) 512
    else base
    value - Verifier.CRC_LENGTH - Verifier.KEY_LENGTH // aim to minimise partial pages
  }

  private def verifyValue(forId: Long, bb: ByteBuffer): Unit = {
    val rndSize = valueSize(forId)
    val expected = rndSize + Verifier.CRC_LENGTH
    if (bb.limit != expected) throw new IllegalStateException("Limit error id=" + forId + " exp=" + expected + " limit=" + bb.limit)
    val crcRead = bb.getLong
    crc.reset()
    crc.update(forId.toInt)
    crc.update(bb)
    val crcVal = crc.getValue
    if (crcRead != crcVal) throw new IllegalStateException("CRC error id=" + forId)
  }

  private def write(forId: Long): Unit = {
    val dbi = getDbi(forId)
    updateKey(forId)
    updateValue(forId)
    try dbi.put(txn, key, vals)
    catch {
      case ex: LmdbException =>
        throw new IllegalStateException("DB put id=" + forId, ex)
    }
  }
}