package kimutyam.practice.scio.adaptor.bigtable

import com.google.protobuf.ByteString

final private class KeyByteSizeOverException(message: String = null, cause: Throwable = null)
  extends RuntimeException(message, cause)

private[bigtable] object KeyNamePolicy {

  private val Separator = ":"
  private val MaxBufferSize = 4096

  private[bigtable] def stringKey(args: String*): String = {
    args.mkString(Separator)
  }

  private[bigtable] def create(args: String*): RowKey = {
    val key = args.mkString(Separator)
    val byteStringKey = ByteString.copyFromUtf8(key)

    if(byteStringKey.size() > MaxBufferSize) {
      throw new KeyByteSizeOverException(s"Key size is greater than 4096. key: $key")
    }
    byteStringKey
  }
}
