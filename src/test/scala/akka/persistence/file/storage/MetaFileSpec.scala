package akka.persistence.file.storage

import java.io.File

import org.iq80.leveldb.util.FileUtils
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class MetaFileSpec extends FunSuite with Matchers with BeforeAndAfter {
  val path = "target/metafile.txt"
  var meta: MetaFile = _

  test("should find a block for persistenceId") {
    meta.findBlock("P1").get.persistenceId should equal("P1")
    meta.findBlock("P2").get.persistenceId should equal("P2")
  }

  test("should not find a block for inexisting persistence id") {
    meta.findBlock("P999") should be(None)
  }

  test("should store a block and then find it") {
    val block = MetaBlock(16, 0, 1, "P0")
    meta.append(block)
    //reinitialize meta
    meta = new MetaFile(path)
    meta.findBlock("P0").get should equal(block)
  }

  test("should complain when trying to store an already existing block") {
    intercept[IllegalArgumentException] {
      meta.append(MetaBlock(16, 0, 1, "P1"))
    }
  }

  test("should complain when trying to update a block that doesn't exist") {
    intercept[IllegalStateException] {
      meta.update(MetaBlock(16, 160, 2, "P0"))
    }
  }

  test("should update an already existing block in cache") {
    val updatedBlock = MetaBlock(16, 160, 2, "P1")
    meta.update(updatedBlock)
    val foundBlock = meta.findBlock("P1")
    foundBlock.get should equal(updatedBlock)
  }

  test("should update an already existing block in file") {
    val updatedBlock = MetaBlock(16, 160, 2, "P1")
    meta.update(updatedBlock)
    meta = new MetaFile(path)
    val foundBlock = meta.findBlock("P1")
    foundBlock.get should equal(updatedBlock)
  }

  before {
    meta = new MetaFile(path)
    writeSampleFile(path)
  }

  after {
    FileUtils.deleteRecursively(new File(path))
  }

  private def writeSampleFile(path: String) {
    Array(
      MetaBlock(16, 0, 1, "P1"),
      MetaBlock(16, 0, 1, "P2")
    ).foreach(meta.append)
  }
}
