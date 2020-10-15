package kimutyam.practice.scio.domain

import java.time.Instant

case class PurchaseEvent(
                          ts: Instant,
                          item: Item,
                          userAccountId: String,
                          couponCode: String
                        ) {
  def inCampaignTerm: Boolean = ???
  def isLimitedTimeItem: Boolean = ???
}


case class Item(
                 itemId: String,
                 sku: String
               ) {
}
