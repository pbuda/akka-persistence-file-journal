package akka.persistence.file.storage

import java.io.RandomAccessFile
import java.nio.channels.FileChannel.MapMode

import scala.collection.mutable.ListBuffer

class MetaFile(path: String) {
  private val lengthOffset = 4
  private val persistenceIdOffset = 24
  private val meta = new RandomAccessFile(path, "rw").getChannel.map(MapMode.READ_WRITE, 0, 0x01400000)

  private val blocks: ListBuffer[Block] = {
    meta.position(lengthOffset)
    val length = meta.getLong
    val buffer = ListBuffer[Block]()
    while (meta.position < length) {
      buffer.append(readCurrentBlock())
    }
    meta.rewind()
    buffer
  }

  def findBlock(persistenceId: String): Option[Block] = {
    blocks.find(_.persistenceId == persistenceId)
  }

  private def readCurrentBlock(): Block = {
    val startPosition = meta.position()
    val blockSize = meta.getShort
    val first = meta.getLong
    val last = meta.getLong
    val highest = meta.getLong
    val persistenceIdLength = blockSize - persistenceIdOffset
    val persistenceId = readString(persistenceIdLength)

    Block(startPosition, blockSize, first, last, highest, persistenceId)
  }

  private def readString(size: Int) = {
    val stringArray = Array.fill[Byte](size) {
      0
    }
    meta.get(stringArray, 0, size)
    new String(stringArray)
  }
}

case class Block(startPosition: Int, size: Short, first: Long, last: Long, highestSequenceNr: Long, persistenceId: String)
