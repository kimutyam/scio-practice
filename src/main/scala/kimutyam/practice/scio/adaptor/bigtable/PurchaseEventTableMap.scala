package kimutyam.practice.scio.adaptor.bigtable

import kimutyam.practice.scio.domain.PurchaseEvent

object PurchaseEventTableMap {

  implicit class PurchaseEventOps(event: PurchaseEvent)
    extends MutationFactory {

    private val tableName: TableName = "purchaseEvent"
    private val familyName: FamilyName = "raw"

    private val rowKey = KeyNamePolicy.create(
      event.ts.toEpochMilli.toString,
      event.item.itemId
    )

    def createTableMap: TableMap = {
      Map(
        tableName -> Seq(createEventColumnRow)
      )
    }

    def createEventColumnRow: Row = {
      val f = createMutation _
      val columns = Iterable(
        f(event.ts.toEpochMilli.toString, "eventTimestamp"),
        f(event.userAccountId, "userAccount"),
        f(event.item.itemId, "itemId"),
        f(event.item.sku, "sku"),
        f(event.couponCode, "couponCode"),
      ).map(_(familyName))

      (rowKey, columns)
    }
  }

}
