package akka.persistence.file.storage

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.charset.Charset

import org.iq80.leveldb.util.FileUtils
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class MetaFileSpec extends FunSuite with Matchers with BeforeAndAfter {
  val path = "target/tests.txt"
  var meta: MetaFile = _

  test("should find a block for persistenceId") {
    meta.findBlock("P1").get.persistenceId should equal("P1")
    meta.findBlock("P2").get.persistenceId should equal("P2")
  }

  test("should not find a block for inexisting persistence id") {
    meta.findBlock("P999") should be(None)
  }

  test("should store a block and then find it") {
    val block = Block(16, 0, 1, "P0")
    meta.append(block)
    //reinitialize meta
    meta = new MetaFile(path)
    meta.findBlock("P0").get should equal(block)
  }

  test("should complain when trying to store an already existing block") {
    intercept[IllegalArgumentException] {
      meta.append(Block(16, 0, 1, "P1"))
    }
  }

  before {
    MetaFileSpecUtils.writeSampleFile(path)
    meta = new MetaFile(path)
  }

  after {
    FileUtils.deleteRecursively(new File(path))
  }
}


object MetaFileSpecUtils {
  def writeSampleFile(path: String) {
    val charset = Charset.forName("utf-8")

    val bytes = Array(
      Block(16, 0, 1, "P1").bytes,
      Block(16, 0, 1, "P2").bytes
    )


    val header = ByteBuffer.allocate(8)
    header.put("AKFM".getBytes(charset))
    header.putInt(bytes.map(_.size).sum)

    val stream = new RandomAccessFile(path, "rw")
    stream.write(header.array())
    stream.write(bytes.flatten)
  }

}
