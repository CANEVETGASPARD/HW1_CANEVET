import org.apache.flink.api.scala._
import org.apache.flink.util.Collector


object ShortestPath {
    def main(args: Array[String]): Unit = {
      val env = ExecutionEnvironment.getExecutionEnvironment

      //problem variables
      val initial_vertice = "17038"
      val maxIteration = 15
      val INFINITE = 5243 // max possible length path 5242 => 5243 == INFINITE
      val inputPath = args(0) + "/ca-GrQc.txt"
      val outputPath = args(1) + "/task_I"

      //load file into dataset
      val textFile = env.readTextFile(inputPath)

      //group neighbours with their sources with | as a separator
      val groupedFile = textFile.map(row => {
        val arrayrRow = row.split("\t")
        (arrayrRow(0),arrayrRow(1))
      }).groupBy(0).reduce((neihbour1,neighbour2) => (neihbour1._1,neihbour1._2 + "|" + neighbour2._2))

      //there we map all values. thus we have tuples that look like (source, weight, State -> discover of not,
      // String made of all neigbours, path)
      val solutionSet = groupedFile.map(row => {
        if (row._1 == initial_vertice) {
          (row._1,0,"UNDISCOVERED",row._2,initial_vertice)
        } else {
          (row._1,INFINITE,"UNDISCOVERED",row._2,"")
        }
      })

      // first workset is made of initial vertice tuple
      val initialWorkSet = solutionSet.filter(row => row._2<INFINITE && row._3 == "UNDISCOVERED")

      // step function
      def propagateThroughMaze(s:DataSet[(String,Int,String,String,String)],ws:DataSet[(String,Int,String,String,String)]) : (DataSet[(String,Int,String,String,String)],DataSet[(String,Int,String,String,String)])= {

        //we change the state of reached vertice to DISCOVER
        val intermediateSolutionSet = s.map(row => {
          if (row._3 == "UNDISCOVERED" && row._2 < INFINITE){
            (row._1,row._2,"DISCOVERED",row._4,row._5)
          } else {
            row
          }
        })

        // dataset made of tuple of all new reached vertices tuple form (new_reached_node,weight,"UNDISCOVER",emptyNeigbours,updatedpath)
        val newReachedNodes = ws.flatMap(source => source._4.split("\\|").map(neighbour => (neighbour, source._2 +1,"UNDISCOVERED","",source._5 + "->" + neighbour)))

        //union solution set with new reached nodes and set weight and path of reached nodes with reduce function
        val newS = intermediateSolutionSet.union(newReachedNodes).groupBy(0).reduce((node1,node2) => {
          if(node1._4 =="") {
            if(node2._3 =="UNDISCOVERED") {
              (node1._1,node1._2,node1._3,node2._4,node1._5)
            } else {
              node2
            }
          } else {
            if(node1._3 =="UNDISCOVERED") {
              (node2._1,node2._2,node2._3,node1._4,node2._5)
            } else {
              node1
            }
          }
        })
        //filter newS to have next work set
        val newWs = newS.filter(row => row._2<INFINITE && row._3 == "UNDISCOVERED")
        //return new solutionSet and new WorkSet
        (newS,newWs)
      }
      //apply iteration delta function with propagateThroughMaze as step function
      val output = solutionSet.iterateDelta(initialWorkSet,maxIteration,Array(0)) {(solution,workset) => propagateThroughMaze(solution,workset)}
    }
}
