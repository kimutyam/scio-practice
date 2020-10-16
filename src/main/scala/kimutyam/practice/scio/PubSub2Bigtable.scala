package kimutyam.practice.scio

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigtable._
import kimutyam.practice.scio.adaptor.bigtable.Row
import kimutyam.practice.scio.domain.PurchaseEvent
import kimutyam.practice.scio.support.{DeserializeError, PubSubTopic}
import kimutyam.practice.scio.support.SCollectionOps._

object PubSub2Bigtable {

  private def deserialize(jsonString: String): Either[DeserializeError, PurchaseEvent] = ???

  private def serialize(event: PurchaseEvent): Row = {
    import adaptor.bigtable.PurchaseEventTableMap._
    event.createEventColumnRow
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val projectId = args("projectId")
    val topicKey = args("pubSubTopicKey")
    val instanceId = args("bigtableInstanceId")

    // TODO デッドキュー
    val (_, eventSc) = sc
      .pubsubTopic[String](PubSubTopic.name(projectId, topicKey))
      .map(deserialize)
      .separate()

    eventSc
      .filter { event =>
        event.inCampaignTerm &&
          !event.isLimitedTimeItem
      }
      .map(serialize)
      .saveAsBigtable(
        projectId,
        instanceId,
        "tableId"
      )
    sc.run()
  }
}
