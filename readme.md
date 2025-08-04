# torch-lmdb

## Overview
`torch-lmdb` is an open - source project designed to provide an interface for interacting with LMDB (Lightning Memory - Mapped Database) in a Scala environment. The project is licensed under the Apache License 2.0.

## Features
1. **Low - Latency Focus**: Critical paths such as releasing and renewing read - only transactions and cursor operations are optimized for low latency.
2. **Flexible Concurrency**: The classes in `torch.lmdb` do not provide built - in concurrency guarantees, allowing users to implement threading according to their application requirements without unnecessary locking overhead.
3. **JNR - FFI Integration**: It uses JNR - FFI to interface with the LMDB system library, and the library can be extracted from the JAR file to a specified directory.

## License
This project is licensed under the Apache License 2.0. You can obtain a copy of the License at [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0).

## How to Use
### Prerequisites
- Ensure you have a Scala development environment set up.
- The LMDB system library needs to be available. You can set the `java` system property to specify the directory where the LMDB library is extracted from the `torch.lmdb` JAR. If not specified, it will be extracted to `java.io.tmpdir`.

### Installation
1. Add the `torch-lmdb` dependency to your `build.sbt` file:
```scala
## example
dependencies += "io.github.mullerhai" %% "torch-lmdb" % "0.0.5-1.15.2"
```
Replace `<version>` with the actual version number of `torch-lmdb`.

### Basic Usage Example
Here is a simple example of using `torch-lmdb` to interact with LMDB:
```scala
import torch.lmdb.db.{Env, Dbi}

object LmdbExample {
  def main(args: Array[String]): Unit = {
    // Create an environment
    val env = Env.create()
    env.setMapSize(10485760) // Set the map size
    env.open(new java.io.File("./lmdb-data"))

    // Open a database
    val db = env.openDbi("myDatabase")

    // TODO: Add code to perform operations such as put, get, etc.

    // Close resources
    db.close()
    env.close()
  }
}
```

### Notes
- Do not share transactions between threads. You must follow LMDB's specific thread rules.
