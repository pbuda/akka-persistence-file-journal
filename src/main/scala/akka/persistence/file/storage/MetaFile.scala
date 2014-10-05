package akka.persistence.file.storage

import java.io.{File, FileOutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode
import java.nio.charset.Charset

import scala.collection.mutable.ListBuffer

class MetaFile(path: String) {
  private val LengthOffset = 4
  private val DataOffset = 8
  val backingFile = {
    val file = new File(path)
    if (!file.exists()) {
      file.getParentFile.mkdirs()
      val header = ByteBuffer.allocate(8)
      header.put("AKFM".getBytes(MetaBlock.CharactedSet))
      header.putInt(0)
      new FileOutputStream(file).write(header.array())
    }
    new RandomAccessFile(file, "rw")
  }
  private var meta = initMeta()

  private def initMeta() = backingFile.getChannel.map(MapMode.READ_WRITE, 0, backingFile.length())

  private val blocks: ListBuffer[CachedBlock] = {
    val length = dataLength
    val buffer = ListBuffer[CachedBlock]()
    while (meta.position < length) {
      buffer.append(CachedBlock(meta.position(), readCurrentBlock()))
    }
    meta.rewind()
    buffer
  }

  private def dataLength = {
    meta.position(LengthOffset)
    meta.getInt
  }

  /**
   * Searches for a block corresponding to the specified persistence id.
   *
   * @param persistenceId persistence id of the block to find
   * @return found block or None
   */
  def findBlock(persistenceId: String): Option[MetaBlock] = {
    blocks.find(_.block.persistenceId == persistenceId).map(_.block)
  }

  private def readCurrentBlock(): MetaBlock = {
    meta.mark()
    val blockSize = meta.getShort
    val bytes = Array.fill[Byte](blockSize) {
      0
    }
    meta.reset()
    meta.get(bytes, 0, blockSize)
    MetaBlock.apply(bytes)
  }

  /**
   * Appends the specified block to the end of file and cache only if it doesn't already exist.
   * If it exists an [[IllegalArgumentException]] is thrown.
   *
   * This operation will add the specified block at the end of block list, updating the length
   * counter by the value of [[MetaBlock.size]].
   *
   * @param block block to store
   * @throws IllegalArgumentException if the block already exists
   */
  def append(block: MetaBlock) {
    findBlock(block.persistenceId) match {
      case Some(_) => throw new IllegalArgumentException("This block already exists. Maybe try using update() instead?")
      case None =>
        val length = dataLength
        val newLength = length + block.size
        backingFile.setLength(DataOffset + newLength)
        meta = initMeta()
        meta.position(DataOffset + length)
        meta.put(block.bytes)
        meta.position(LengthOffset)
        meta.putInt(newLength)
        blocks.append(CachedBlock(DataOffset + length, block))
    }
  }

  /**
   * Updates an existing block with the specified block. The update is first made to file and then to cache.
   * If the specified block doesn't exist in cache, and [[IllegalStateException]] is thrown.
   *
   * @param block updated block
   * @throws IllegalStateException if the specified block is not found in cache
   */
  def update(block: MetaBlock) {
    findCachableBlock(block) match {
      case None => throw new IllegalStateException(s"The specified block $block does not exist. Maybe try using append() instead?")
      case Some(cachedBlock) =>
        meta.position(cachedBlock.position)
        meta.put(block.bytes)
        blocks.update(blocks.indexOf(cachedBlock), cachedBlock.copy(block = block))
    }
  }

  private def findCachableBlock(block: MetaBlock): Option[CachedBlock] = {
    blocks.find(_.block.persistenceId == block.persistenceId)
  }

}

case class MetaBlock(size: Short, first: Long, last: Long, highestSequenceNr: Long, persistenceId: String) {
  def bytes: Array[Byte] = {
    val pid = persistenceId.getBytes(MetaBlock.CharactedSet)
    val buffer = ByteBuffer.allocate(size)
    buffer.putShort(size)
    buffer.putLong(first)
    buffer.putLong(last)
    buffer.putLong(highestSequenceNr)
    buffer.put(pid)
    buffer.array()
  }
}

object MetaBlock {
  private val PersistenceIdOffset = 26
  val CharactedSet = Charset.forName("utf-8")

  def apply(first: Long, last: Long, highestSequenceNr: Long, persistenceId: String): MetaBlock = {
    // can such conversion to short crash the storage? Maybe expect that and write a test?
    new MetaBlock((PersistenceIdOffset + persistenceId.getBytes(CharactedSet).length).toShort, first, last, highestSequenceNr, persistenceId)
  }

  def apply(bytes: Array[Byte]): MetaBlock = {
    val buffer = ByteBuffer.wrap(bytes)
    val blockSize = buffer.getShort
    val first = buffer.getLong
    val last = buffer.getLong
    val highest = buffer.getLong
    val persistenceIdLength = blockSize - PersistenceIdOffset
    val persistenceId = readString(buffer, persistenceIdLength)

    MetaBlock(blockSize, first, last, highest, persistenceId)
  }

  private def readString(buffer: ByteBuffer, size: Int) = {
    val stringArray = Array.fill[Byte](size) {
      0
    }
    buffer.get(stringArray, 0, size)
    new String(stringArray)
  }
}

private case class CachedBlock(position: Int, block: MetaBlock)
