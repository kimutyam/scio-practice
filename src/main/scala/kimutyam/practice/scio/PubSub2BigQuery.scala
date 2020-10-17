package kimutyam.practice.scio

import com.google.api.services.bigquery.model.TableSchema
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigquery._
import com.spotify.scio.values.{SCollection, SideOutput}
import kimutyam.practice.scio.adaptor.bigquery.{TableMap, TableSpec}
import kimutyam.practice.scio.domain.PurchaseEvent
import kimutyam.practice.scio.support.{DeserializeError, PubSubTopic}
import kimutyam.practice.scio.support.SCollectionOps._


object PubSub2BigQuery {

  case class TableInfo(
                        tableSpec: TableSpec,
                        schema: TableSchema
                      )

  def deserialize(jsonString: String): Either[DeserializeError, PurchaseEvent] = ???

  def saveAsMultiBigQueryTable(
            results: SCollection[TableMap],
            tableInfos: Seq[TableInfo]
          ): Unit = {
    val tableToSideOutPut: Map[TableSpec, SideOutput[TableRow]] =
      tableInfos.map(_.tableSpec -> SideOutput[TableRow]()).toMap

    // TableSchemaがSerializableでないため、ExceptionになるためTransformに渡すのはTableSpecのみ
    val specs = tableInfos.map(_.tableSpec)

    val (_, sideOutputs) = results
      .withSideOutputs(tableToSideOutPut.values.toSeq: _*)
      .map { case (tableMap, ctx) =>
        for {
          spec <- specs
          tableRow <- tableMap.getOrElse(spec, Seq.empty)
        } ctx.output(tableToSideOutPut(spec), tableRow)
        tableMap
      }

    tableInfos.foreach { info =>
      sideOutputs(tableToSideOutPut(info.tableSpec))
        .saveAsBigQueryTable(
          Table.Spec(info.tableSpec.stringValue),
          info.schema,
          WRITE_APPEND,
          CREATE_IF_NEEDED
        )
    }
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    import adaptor.bigquery.PurchaseEventTableMap._

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val projectId = args("projectId")
    val topicKey = args("pubSubTopicKey")

    val (_, eventSc) = sc
      .pubsubTopic[String](PubSubTopic.name(projectId, topicKey))
      .map(deserialize)
      .separate()

    val tableMapSc = eventSc
      .map(_.tableMap(projectId))

    saveAsMultiBigQueryTable(
      tableMapSc,
      Seq(
        TableInfo(purchasedItemTableSpec(projectId), purchasedItemSchema),
        TableInfo(usedCouponTableSpec(projectId), usedCouponSchema)
      )
    )
  }
}
