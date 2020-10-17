package kimutyam.practice.scio.support

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

trait SCollectionOps {

  implicit class EitherPartitionSCollection[A, B](@transient private val self: SCollection[Either[A, B]]) {
    def separate(
                  leftCollectName: String = "Get Errors",
                  rightCollectName: String = "Get Successes"
                )(implicit leftCoder: Coder[A], rightCoder: Coder[B]): (SCollection[A], SCollection[B]) = {
      (
        self.withName(leftCollectName).collect { case Left(e) => e},
        self.withName(rightCollectName).collect { case Right(v) => v}
      )
    }
  }

  implicit class OptionPartitionSCollection[A](@transient private val self: SCollection[Option[A]])
    extends Serializable {
    def collectDefined(name: String)(implicit leftCoder: Coder[A]): SCollection[A] = {
      self.withName(name).collect {
        case Some(a) => a
      }
    }
  }

}

object SCollectionOps extends SCollectionOps