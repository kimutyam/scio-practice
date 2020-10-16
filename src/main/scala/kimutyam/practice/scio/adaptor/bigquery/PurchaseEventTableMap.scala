package kimutyam.practice.scio.adaptor.bigquery

import com.google.api.services.bigquery.model.TableSchema
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.bigquery.types.BigQueryType
import kimutyam.practice.scio.domain.PurchaseEvent
import org.joda.time.{Instant => JodaInstant}

object PurchaseEventTableMap {

  @BigQueryType.toTable
  case class PurchasedItemRow(
                               eventDateTime: JodaInstant,
                               userAccountId: String,
                               itemId: String,
                               sku: String
                             )

  @BigQueryType.toTable
  case class UsedCouponRow(
                     eventDateTime: JodaInstant,
                     userAccountId: String,
                     couponCode: String
                   )

  private val purchasedItemQueryType = BigQueryType[PurchasedItemRow]
  private val usedCouponQueryType = BigQueryType[UsedCouponRow]

  val purchasedItemSchema: TableSchema = purchasedItemQueryType.schema
  val usedCouponSchema: TableSchema = usedCouponQueryType.schema

  def purchasedItemTableSpec(projectId: String): TableSpec =
    TableSpec(
      Some(projectId),
      "purchasedItem",
      "stream"
    )

  def usedCouponTableSpec(projectId: String): TableSpec =
    TableSpec(
      Some(projectId),
      "usedCoupon",
      "stream"
    )


  implicit class WidgetEventOps(event: PurchaseEvent) {

    val purchasedItemTableRow: TableRow = {
      purchasedItemQueryType.toTableRow(
        PurchasedItemRow(
          new JodaInstant(event.ts.toEpochMilli),
          event.userAccountId,
          event.item.itemId,
          event.item.sku
        )
      )
    }

    val usedCouponTableRow: TableRow = {
      usedCouponQueryType.toTableRow(
        UsedCouponRow(
          new JodaInstant(event.ts.toEpochMilli),
          event.userAccountId,
          event.couponCode
        )
      )
    }

    def tableMap(projectId: String): TableMap = {
      Map(
        purchasedItemTableSpec(projectId) -> Seq(purchasedItemTableRow),
        usedCouponTableSpec(projectId) -> Seq(usedCouponTableRow)
      )
    }
  }
}
