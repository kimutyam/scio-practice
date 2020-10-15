package kimutyam.practice.scio

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigtable._
import kimutyam.practice.scio.adaptor.bigtable.Row
import kimutyam.practice.scio.domain.PurchaseEvent
import kimutyam.practice.scio.support.SCollectionOps._
import kimutyam.practice.scio.support.{DeserializeError, PubSubTopic}

// Pipelineに独自の名前をつけるためのサンプル
object PubSub2BigtableEx {

  def deserialize(jsonString: String): Either[DeserializeError, PurchaseEvent] = ???
  def serialize(event: PurchaseEvent): Row = ???

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val projectId = args("projectId")
    val topicKey = args("pubSubTopicKey")
    val instanceId = args("bigtableInstanceId")

    val (_, eventSc) = sc
      .withName("Input PubSub")
      .pubsubTopic[String](PubSubTopic.name(projectId, topicKey))
      .transform("Deserialize")(
        _.map(deserialize)
      )
      .separate()

    eventSc
      .transform("Filter Valid Item")(
      _.filter { event =>
        event.inCampaignTerm &&
          !event.isLimitedTimeItem
      }
    ).transform("Serialize Bigtable Row")(
      _.map(serialize)
    ).saveAsBigtable(
      projectId,
      instanceId,
      "tableId"
    )
    sc.run()
  }
}
