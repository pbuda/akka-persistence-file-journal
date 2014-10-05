package akka.persistence.file.storage

import java.io.{File, FileOutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode

class DataFile(path: String) {
  private val LengthOffset = 4
  private val DataOffset = 8
  val backingFile = {
    val file = new File(path)
    if (!file.exists()) {
      file.getParentFile.mkdirs()
      val header = ByteBuffer.allocate(8)
      header.put("AKFD".getBytes(MetaBlock.CharactedSet))
      header.putInt(0)
      new FileOutputStream(file).write(header.array())
    }
    new RandomAccessFile(file, "rw")
  }
  private var data = initData()

  private def initData() = backingFile.getChannel.map(MapMode.READ_WRITE, 0, backingFile.length())

  /**
   * Appends a new data block at the end of storage. After appending the new data block, the previous block
   * is updated with the new position of the next data block.
   *
   * @param block block to append.
   * @return the same [[DataBlock]] but with updated [[DataBlock.position]] field with actual insert position
   */
  def append(block: DataBlock): DataBlock = {
    if (block.position != -1) throw new IllegalArgumentException(s"The specified block $block already exists. Maybe try update() instead?")
    val length = dataLength
    val newLength = length + block.size
    //grow file
    backingFile.setLength(DataOffset + newLength)
    data = initData()
    //write block
    val position: Int = DataOffset + length
    data.position(position)
    val updatedBlock = block.copy(position = position)
    data.put(updatedBlock.bytes)
    updateLength(newLength)

    //update previous' block `next` position if this is not first block
    if (!block.isFirst) {
      data.position(block.previous)
      val prevBlock = readBlock()
      update(prevBlock.copy(next = position))
    }

    updatedBlock
  }

  private def dataLength = {
    data.position(LengthOffset)
    data.getInt
  }

  private def updateLength(newLength: Int) {
    data.position(LengthOffset)
    data.putInt(newLength)
  }

  private def update(block: DataBlock) {
    if (block.position == -1) throw new IllegalStateException(s"The specified block $block doesn't exist. Maybe try append() instead?")
    data.position(block.position)
    data.put(block.bytes)
  }

  /**
   * Reads all data blocks identified by the specified [[MetaBlock]].
   *
   * @param meta meta information.
   * @return a list of all data blocks corresponding to the [[MetaBlock.persistenceId]]
   */
  def collect(meta: MetaBlock): List[DataBlock] = {
    def readAll(acc: List[DataBlock]): List[DataBlock] = {
      val block = readBlock()
      if (!block.isLast) {
        data.position(block.next)
        readAll(acc :+ block)
      }
      else acc :+ block
    }
    data.position(meta.first.toInt)
    readAll(List())
  }

  private def readBlock(): DataBlock = {
    data.mark()
    val size = data.getInt
    val blockArray = Array.fill[Byte](size) {
      0
    }
    data.reset()
    data.get(blockArray, 0, size)
    DataBlock(blockArray)
  }

}

case class DataBlock(size: Int, position: Int, previous: Int, next: Int, sequenceNr: Long, payload: Seq[Byte]) {
  def isFirst = previous == 0

  def isLast = next == 0

  def bytes = {
    val buffer = ByteBuffer.allocate(size)
    buffer.putInt(size)
    buffer.putInt(position)
    buffer.putInt(previous)
    buffer.putInt(next)
    buffer.putLong(sequenceNr)
    buffer.put(payload.toArray)
    buffer.array()
  }
}

object DataBlock {
  private val DescriptorLength = 24

  def apply(previous: Int, next: Int, sequenceNr: Long, payload: Seq[Byte]): DataBlock =
    DataBlock(DescriptorLength + payload.size, -1, previous, next, sequenceNr, payload)

  def apply(bytes: Array[Byte]): DataBlock = {
    val buffer = ByteBuffer.wrap(bytes)
    val size = buffer.getInt
    val position = buffer.getInt
    val previous = buffer.getInt
    val next = buffer.getInt
    val sequenceNr = buffer.getLong
    val payloadSize = size - DescriptorLength
    val payloadArray = Array.fill[Byte](payloadSize) {
      0
    }
    buffer.get(payloadArray, 0, payloadSize)
    DataBlock(size, position, previous, next, sequenceNr, payloadArray)
  }

}
