package kimutyam.practice.scio.support

object PubSubTopic {
  def name(projectId: String, topicKey: String): String =
    s"projects/$projectId/topics/$topicKey"
}
