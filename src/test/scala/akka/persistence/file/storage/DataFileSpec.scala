package akka.persistence.file.storage

import java.io.{File, RandomAccessFile}

import org.iq80.leveldb.util.FileUtils
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class DataFileSpec extends FunSuite with Matchers with BeforeAndAfter {
  private val path = "target/datafile.txt"
  private var data: DataFile = _
  private val testBlockP11 = DataBlock(0, 33, 1, Array[Byte](11))
  private val testBlockP12 = DataBlock(8, 0, 2, Array[Byte](12))

  test("should collect all blocks for meta block") {
    val dataBlocks = data.collect(MetaBlock(8, 29, 2, "P1"))

    dataBlocks should have size 2
    dataBlocks should contain(DataBlock(25, 8, 0, 33, 1, Array[Byte](11)))
    dataBlocks should contain(DataBlock(25, 33, 8, 0, 2, Array[Byte](12)))
  }

  test("should update data block length after appending") {
    val reader = new RandomAccessFile(path, "rw")
    reader.seek(4)
    val oldLength = reader.readInt()
    val dataBlock = DataBlock(0, 0, 1, Array[Byte](21))

    data.append(dataBlock)

    reader.seek(4)
    val newLength = reader.readInt()
    newLength should equal(oldLength + dataBlock.size)
  }

  test("should grow backing file during appending") {
    val file = new File(path)
    val size = file.length()
    val dataBlock = DataBlock(0, 0, 1, Array[Byte](21))

    data.append(dataBlock)

    val newSize = file.length()
    newSize should equal(size + dataBlock.size)
  }

  test("should append a block and then collect it") {
    val dataBlock = DataBlock(0, 0, 1, Array[Byte](21))
    val metaBlock = MetaBlock(58, 0, 1, "P2")
    val updatedBlock = data.append(dataBlock)

    val appendedBlocks = data.collect(metaBlock)

    appendedBlocks should have size 1
    appendedBlocks should contain(updatedBlock)
  }

  test("should update previous block after appending a new one") {
    val newBlock = DataBlock(33, 0, 3, Array[Byte](13))

    data.append(newBlock)

    val blocks = data.collect(MetaBlock(8, 58, 3, "P1"))
    blocks(1) should equal(DataBlock(25, 33, 8, 58, 2, Array[Byte](12)))
  }

  before {
    data = new DataFile(path)
    writeFile()
  }

  after {
    FileUtils.deleteRecursively(new File(path))
  }

  private def writeFile() {
    Array(testBlockP11, testBlockP12).foreach(data.append)
  }
}
