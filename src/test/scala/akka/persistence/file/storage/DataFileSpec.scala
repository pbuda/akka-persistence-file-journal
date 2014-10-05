package akka.persistence.file.storage

import java.io.{File, RandomAccessFile}

import org.iq80.leveldb.util.FileUtils
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class DataFileSpec extends FunSuite with Matchers with BeforeAndAfter {
  private val path = "target/datafile.txt"
  private var data: DataFile = _
  private val testBlockP11 = DataBlock(0, 29, 1, Array[Byte](11))
  private val testBlockP12 = DataBlock(8, 0, 2, Array[Byte](12))

  test("should collect all blocks for meta block") {
    val dataBlocks = data.collect(MetaBlock(8, 29, 2, "P1"))
    dataBlocks should have size 2
    dataBlocks should contain(testBlockP11)
    dataBlocks should contain(testBlockP12)
  }

  before {
    data = new DataFile(path)
    writeFile()
  }

  after {
    FileUtils.deleteRecursively(new File(path))
  }

  private def writeFile() {
    val blocks = Array(testBlockP11, testBlockP12)//, testBlockP22, testBlockP12)

    val output = new RandomAccessFile(path, "rw")
    output.seek(8)
    output.write(blocks.flatMap(_.bytes))
  }
}
