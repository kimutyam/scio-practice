package kimutyam.practice.scio

import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.{CREATE_IF_NEEDED, TableRow, WRITE_APPEND}
import com.spotify.scio.values.SideInput
import kimutyam.practice.scio.adaptor.bigquery.PurchaseEventTableMap.{purchasedItemSchema, purchasedItemTableSpec}
import kimutyam.practice.scio.domain.PurchaseEvent
import kimutyam.practice.scio.support.DeserializeError
import kimutyam.practice.scio.support.SCollectionOps._
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy
import org.apache.beam.sdk.options.{PipelineOptionsFactory, ValueProvider}
import org.apache.beam.sdk.io.gcp.{bigquery => beamBigquery}

// 設定ファイルとインプットファイルのSideInput
object Storage2BigQuery {

  case class Setting(hoo: String)


  private def deserializeEvent(
                                fileString: String,
                                setting: Either[DeserializeError, Setting],
                              ): Either[DeserializeError, PurchaseEvent] = ???

  private def deserializeSetting(fileString: String): Either[DeserializeError, Setting] = ???

  private def serialize(event: PurchaseEvent): TableRow = {
    import adaptor.bigquery.PurchaseEventTableMap._
    event.purchasedItemTableRow
  }

  private def settingInput(
                            sc: ScioContext,
                            settingFilePath: ValueProvider[String]
                          ): SideInput[Either[DeserializeError, Setting]] = {
    sc
      .customInput(
        "Read CustomAttributeSettings",
        FileIO.`match`().filepattern(
          settingFilePath
        )
      )
      .applyTransform(FileIO.readMatches())
      .withName("Decode CustomAttributeSettings")
      .map { file =>
        deserializeSetting(file.readFullyAsUTF8String())
      }
      .asSingletonSideInput
  }


  def main(cmdlineArgs: Array[String]): Unit = {

    val options = PipelineOptionsFactory
      .fromArgs(cmdlineArgs: _*)
      .withValidation
      .as(classOf[ValueOptions])

    val sc = ScioContext(options)

    val side = settingInput(sc, options.getSettingFilePath)

    val (_, eventSc) =
    // テンプレート機能を使う場合はcustomInput/customOutputになる。
      sc.customInput(
        "Read GCS file",
        FileIO.`match`().filepattern(options.getInputGcsPath)
      )
        .applyTransform(FileIO.readMatches())
        .withSideInputs(side)
        .map { (inputFile, ctx) =>
          deserializeEvent(inputFile.readFullyAsUTF8String, ctx(side))
        }
        .toSCollection
        .separate()

    eventSc
      .map(serialize)
      .saveAsCustomOutput(
        "Write Bigtable",
        beamBigquery.BigQueryIO.write()
          .to(purchasedItemTableSpec(options.getProjectId.get()).stringValue)
          .withSchema(purchasedItemSchema)
          .withCreateDisposition(CREATE_IF_NEEDED)
          .withWriteDisposition(WRITE_APPEND)
          .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
      )
    sc.run()
  }
}
