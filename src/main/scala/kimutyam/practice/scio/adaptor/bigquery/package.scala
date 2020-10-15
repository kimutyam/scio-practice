package kimutyam.practice.scio.adaptor

import com.spotify.scio.bigquery.TableRow

package object bigquery {

  case class TableSpec(projectId: Option[String], dataSetId: String, tableId: String) {
    def stringValue: String = {
      val specWithoutProjectId = s"$dataSetId.$tableId"
      projectId.fold(specWithoutProjectId) { projectId =>
        s"$projectId:$specWithoutProjectId"
      }
    }
  }


  type TableMap = Map[TableSpec, Seq[TableRow]]

  object TableMap {
    def empty: TableMap = Map.empty

    def combine(source: TableMap, target: TableMap): TableMap = {
      target.foldLeft(source) { case (acc, (tableName, thatColumnRows)) =>
        val value = acc.get(tableName).fold(thatColumnRows) { thisColumnRows =>
          thisColumnRows ++: thatColumnRows
        }
        acc.updated(tableName, value)
      }
    }
  }
}
