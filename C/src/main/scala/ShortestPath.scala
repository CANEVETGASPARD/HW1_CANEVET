import org.apache.flink.api.scala._
import org.apache.flink.util.Collector


object ShortestPath {
    def main(args: Array[String]): Unit = {
      val env = ExecutionEnvironment.getExecutionEnvironment

      //problem variables
      val initial_vertice = "17038"
      var clusterFullyExplored = false
      val INFINITE = 5243 // max possible length path 5242 => 5243 == INFINITE
      val inputPath = args(0) + "/ca-GrQc.txt"

      val textFile = env.readTextFile(inputPath)

      val groupedFile = textFile.map(row => {
        val arrayrRow = row.split("\t")
        (arrayrRow(0),arrayrRow(1))
      }).groupBy(0).reduce((neihbour1,neighbour2) => (neihbour1._1,neihbour1._2 + "|" + neighbour2._2))

      val solutionSet = groupedFile.map(row => {
        if (row._1 == initial_vertice) {
          (row._1,0,"UNDISCOVER",row._2,initial_vertice)
        } else {
          (row._1,INFINITE,"UNDISCOVER",row._2,"")
        }
      })

      val initialWorkSet = solutionSet.filter(row => row._2<INFINITE && row._3 == "UNDISCOVER")

      initialWorkSet.print()
    }
}
