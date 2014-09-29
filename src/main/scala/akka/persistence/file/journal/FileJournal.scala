package akka.persistence.file.journal

import akka.actor.ActorLogging
import akka.persistence._
import akka.persistence.journal.AsyncWriteJournal
import akka.serialization.SerializationExtension

import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


class FileJournal extends AsyncWriteJournal with ActorLogging {
  lazy val serialization = SerializationExtension(context.system)

  override def asyncWriteMessages(messages: Seq[PersistentRepr]): Future[Unit] = {
    Future {}
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long, permanent: Boolean): Future[Unit] = {
    Future(())
  }

  @scala.deprecated("writeConfirmations will be removed, since Channels will be removed.")
  override def asyncWriteConfirmations(confirmations: Seq[PersistentConfirmation]): Future[Unit] = {
    Future(())
  }

  @scala.deprecated("asyncDeleteMessages will be removed.")
  override def asyncDeleteMessages(messageIds: Seq[PersistentId], permanent: Boolean): Future[Unit] = {
    Future(())
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    Future {
      1
    }
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] = {
    Future {}
  }
}