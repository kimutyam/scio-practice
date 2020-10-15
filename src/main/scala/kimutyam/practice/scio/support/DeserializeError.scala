package kimutyam.practice.scio.support

import kimutyam.practice.scio.domain.PurchaseEvent

trait DeserializeError {
  val message: String
  val targetEvent: PurchaseEvent
}
