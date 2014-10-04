package akka.persistence.file.storage

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode
import java.nio.charset.Charset

import scala.collection.mutable.ListBuffer

class MetaFile(path: String) {
  private val LengthOffset = 4
  private val DataOffset = 8

  private val meta = new RandomAccessFile(path, "rw").getChannel.map(MapMode.READ_WRITE, 0, 0x01400000)

  private val blocks: ListBuffer[Block] = {
    meta.position(LengthOffset)
    val length = meta.getInt
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
    meta.mark()
    val blockSize = meta.getShort
    val bytes = Array.fill[Byte](blockSize) {
      0
    }
    meta.reset()
    meta.get(bytes, 0, blockSize)
    Block.apply(bytes)
  }

}

case class Block(size: Short, first: Long, last: Long, highestSequenceNr: Long, persistenceId: String) {
  def bytes: Array[Byte] = {
    val charset = Charset.forName("utf-8")
    val pid = persistenceId.getBytes(charset)
    val buffer = ByteBuffer.allocate(size)
    buffer.putShort(size)
    buffer.putLong(first)
    buffer.putLong(last)
    buffer.putLong(highestSequenceNr)
    buffer.put(pid)
    buffer.array()
  }
}

object Block {
  private val PersistenceIdOffset = 26

  def apply(first: Long, last: Long, highestSequenceNr: Long, persistenceId: String): Block =
  // can such conversion to short crash the storage? Maybe expect that and write a test?
    new Block((PersistenceIdOffset + persistenceId.length).toShort, first, last, highestSequenceNr, persistenceId)

  def apply(bytes: Array[Byte]): Block = {
    val buffer = ByteBuffer.wrap(bytes)
    val blockSize = buffer.getShort
    val first = buffer.getLong
    val last = buffer.getLong
    val highest = buffer.getLong
    val persistenceIdLength = blockSize - PersistenceIdOffset
    val persistenceId = readString(buffer, persistenceIdLength)

    Block(blockSize, first, last, highest, persistenceId)
  }

  private def readString(buffer: ByteBuffer, size: Int) = {
    val stringArray = Array.fill[Byte](size) {
      0
    }
    buffer.get(stringArray, 0, size)
    new String(stringArray)
  }
}
