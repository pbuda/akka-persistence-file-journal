package akka.persistence.file.storage

import java.io.RandomAccessFile
import java.nio.channels.FileChannel.MapMode

class MetaFile(path: String) {
  private val dataPositionOffset = 16
  private val lengthOffset = 8
  private val persistenceIdOffset = 24
  private val meta = new RandomAccessFile(path, "rw").getChannel.map(MapMode.READ_WRITE, 0, 65535)

  def findBlock(persistenceId: String): Block = {
    meta.rewind()
    val markerArray = Array.fill[Byte](4) {
      0
    }
    meta.get(markerArray, 0, 4)
    val marker = new String(markerArray)
    println(s"marker: $marker")
    val length = meta.getLong
    println(s"length: $length")
    var pid = ""
    var block:Block = null
    while (meta.position() < length && pid != persistenceId) {
      block = readCurrentBlock()
      pid = block.persistenceId
    }
    block
  }

  private def readCurrentBlock(): Block = {
    val blockSize = meta.getShort
    println(s"blockSize: $blockSize")
    val first = meta.getLong
    println(s"first: $first")
    val last = meta.getLong
    println(s"last: $last")
    val highest = meta.getLong
    println(s"highest: $highest")
    val persistenceIdLength = blockSize - persistenceIdOffset
    val persistenceIdArray = Array.fill[Byte](persistenceIdLength) {
      0
    }
    meta.get(persistenceIdArray, 0, persistenceIdLength)
    val persistenceId = new String(persistenceIdArray)

    Block(blockSize, first, last, highest, persistenceId)
  }
}

case class Block(size: Short, first: Long, last: Long, highestSequenceNr: Long, persistenceId: String)
