package kimutyam.practice.scio.adaptor

import com.google.bigtable.v2.Mutation
import org.apache.beam.sdk.values.KV

package object bigtable {
  type RowKey = com.google.protobuf.ByteString
  type Columns = Iterable[Mutation]
  type Row = (RowKey, Columns)

  type RowKV = KV[RowKey, java.lang.Iterable[Mutation]]

  type TableMap = Map[TableName, Seq[Row]]

  type Value = String
  type ColumnQualifier = String
  type FamilyName = String
  type TableName = String
}
