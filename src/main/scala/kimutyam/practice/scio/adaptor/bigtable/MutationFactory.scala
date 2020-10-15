package kimutyam.practice.scio.adaptor.bigtable

import com.google.bigtable.v2.Mutation
import com.google.protobuf.ByteString
import com.spotify.scio.bigtable.Mutations

trait MutationFactory {
  def createMutation(
                      value: Value,
                      columnQualifier: ColumnQualifier,
                    )(
                      familyName: FamilyName
                    ): Mutation = {
    Mutations.newSetCell(
      familyName,
      ByteString.copyFromUtf8(columnQualifier),
      ByteString.copyFromUtf8(value),
    )
  }
}

object MutationFactory extends MutationFactory