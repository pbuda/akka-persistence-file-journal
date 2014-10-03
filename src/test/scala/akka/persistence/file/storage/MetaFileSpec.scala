package akka.persistence.file.storage

import java.io.{File, RandomAccessFile, FileOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.Charset

import org.iq80.leveldb.util.FileUtils
import org.scalatest.{FunSuite, Matchers}

class MetaFileSpec extends FunSuite with Matchers {

  test("should find a block for persistenceId") {
    val path = "target/tests.txt"
    FileUtils.deleteRecursively(new File(path))
    FileUtils2.writeSampleFile(path)
    val block = new MetaFile(path).findBlock("P1")
    block.persistenceId should equal("P1")
  }
}


object FileUtils2 {
  def writeSampleFile(path: String) {
    val charset = Charset.forName("utf-8")
    val pid = "P1".getBytes(charset)
    val blockSize = (24 + pid.size).toShort
    val buffer = ByteBuffer.allocate(blockSize + 2)
    buffer.putShort(blockSize)
    buffer.putLong(16)
    buffer.putLong(-1)
    buffer.putLong(1)
    buffer.put(pid)

    val header = ByteBuffer.allocate(12)
    header.put("AKFM".getBytes(charset))
    header.putLong(blockSize)

    val stream = new RandomAccessFile(path, "rw")
    stream.write(header.array())
    stream.write(buffer.array())
  }
}
