package kimutyam.practice.scio

import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import com.spotify.scio.ContextAndArgs
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.{FileIO, TextIO}
import org.apache.beam.sdk.transforms.SerializableFunction

object PubSub2Storage {

  private object FileNamer extends SerializableFunction[String, FileIO.Write.FileNaming] {
    override def apply(input: String): FileIO.Write.FileNaming = {
      FileIO.Write.defaultNaming(s"$input/part", ".jsonl")
    }
  }

  private object DestinationKeyFn extends SerializableFunction[String, String] {

    override def apply(input: String): String = {
      destinationKey(input).getOrElse("notIdentified")
    }

    private def extractCompanyId(input: String): Option[String] = ???
    private def extractEventDateTime(input: String): Option[Instant] = ???

    private def destinationKey(input: String): Option[String] = {
      for {
        companyId <- extractCompanyId(input)
        eventDateTime <- extractEventDateTime(input)
      } yield {
        val localDateTime = LocalDateTime.ofInstant(eventDateTime, ZoneOffset.UTC)
        val dtf = DateTimeFormatter.ofPattern("uuuu/MM/dd/HH")
        s"$companyId/${dtf.format(localDateTime)}"
      }
    }
  }

  def main(commandLineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(commandLineArgs)

    val inputPath = args("inputPath")
    val outputPath = args("outputPath")
    val numShards = args.int("numShards", 3)
    sc.pubsubTopic[String](
      inputPath
    )
      .windowByDays(1)
      // DynamicSCollectionOps#saveAsDynamicTextFileでは細かいファイルの命名規則まではカバーできないためCustomOutputを使っている。
      // CustomOutputはApache BeamのPTransformを使うことができるため、Apache BeamでできることはScioでもできる
      .saveAsCustomOutput(
        "Write JSONL File",
        FileIO
          .writeDynamic[String, String]()
          .to(outputPath)
          .by(DestinationKeyFn)
          .via(TextIO.sink())
          .withNumShards(numShards)
          .withDestinationCoder(StringUtf8Coder.of())
          .withNaming(FileNamer)
      )
    sc.run()
  }
}
