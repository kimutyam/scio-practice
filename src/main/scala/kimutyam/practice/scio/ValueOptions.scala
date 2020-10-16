package kimutyam.practice.scio

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.options.Validation.Required
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider

trait ValueOptions extends DataflowPipelineOptions with StreamingOptions {

  @Description("The Google Cloud Storage Path to read from (ex: gs://example-bucket/files/)")
  @Required
  def getInputGcsPath: ValueProvider[String]

  def setInputGcsPath(value: ValueProvider[String]): Unit

  @Description("The ProjectId to write to (ex: example-bucket)")
  @Required
  def getProjectId: ValueProvider[String]

  def setProjectId(value: ValueProvider[String]): Unit

  @Description("Setting file path to write to (ex: gs://example-bucket/files/)")
  @Required
  def getSettingFilePath: ValueProvider[String]

  def setSettingFilePath(value: ValueProvider[String]): Unit

}

object ValueOptions {

  implicit class ValueOptionsOps(private val opts: ValueOptions) {

    def bigqueryDataSet: ValueProvider[String] = {
      NestedValueProvider.of[String, String](
        opts.getProjectId,
        input => s"$input-stream"
      )
    }
  }
}
