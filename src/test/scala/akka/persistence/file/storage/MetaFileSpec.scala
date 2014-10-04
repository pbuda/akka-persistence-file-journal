package akka.persistence.file.storage

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.charset.Charset

import org.iq80.leveldb.util.FileUtils
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class MetaFileSpec extends FunSuite with Matchers with BeforeAndAfter {
  val path = "target/tests.txt"
  var meta:MetaFile = _

  test("should find a block for persistenceId") {
    meta.findBlock("P1").get.persistenceId should equal("P1")
    meta.findBlock("P2").get.persistenceId should equal("P2")
  }

  test("should not find a block for inexisting persistence id") {
    meta.findBlock("P999") should be(None)
  }

  before {
    FileUtils2.writeSampleFile(path)
    meta = new MetaFile(path)
  }

  after {
    FileUtils.deleteRecursively(new File(path))
  }
}


object FileUtils2 {
  def writeSampleFile(path: String) {
    val charset = Charset.forName("utf-8")

    val buffers = Array(
      generateBuffer("P1", charset),
      generateBuffer("P2", charset)
    )


    val header = ByteBuffer.allocate(12)
    header.put("AKFM".getBytes(charset))
    header.putLong(buffers.map(_.capacity()).sum)

    val stream = new RandomAccessFile(path, "rw")
    stream.write(header.array())
    buffers.foreach(buf => stream.write(buf.array()))
  }

  private def generateBuffer(persistenceId: String, charset: Charset) = {
    val pid = persistenceId.getBytes(charset)
    val blockSize = (24 + pid.size).toShort
    val buffer = ByteBuffer.allocate(blockSize + 2)
    buffer.putShort(blockSize)
    buffer.putLong(16)
    buffer.putLong(-1)
    buffer.putLong(1)
    buffer.put(pid)
  }
}
