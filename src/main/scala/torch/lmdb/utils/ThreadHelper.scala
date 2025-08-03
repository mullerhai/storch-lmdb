package torch.lmdb.utils

import java.util.concurrent.{ExecutorService, Executors}

class ThreadHelper {
  final private val mExecutorService = Executors.newFixedThreadPool(1)

  def sendTask(runnable: Runnable): Unit = {
    try mExecutorService.submit(runnable)
    catch {
      case err: Exception =>
        err.printStackTrace()
    }
  }
}