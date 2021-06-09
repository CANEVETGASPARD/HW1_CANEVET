import org.apache.flink.api.scala._
import org.apache.flink.api.java.io.CsvOutputFormat

object ShortestPath {
    def main(args: Array[String]): Unit = {
      val t0 = System.nanoTime()
      val env = ExecutionEnvironment.getExecutionEnvironment

      //problem variables
      val initial_vertice = "17038"
      val maxIteration = 15
      val INFINITE = 5243 // max possible length path 5242 => 5243 == INFINITE
      val inputPath = args(0) + "/ca-GrQc.txt"
      val outputPath = args(1) + "/output_taskI"

      //load file into dataset
      val textFile = env.readTextFile(inputPath)

      //group neighbours with their sources with | as a separator
      val groupedFile = textFile.map(row => {
        val arrayrRow = row.split("\t")
        (arrayrRow(0),arrayrRow(1))
      }).groupBy(0).reduce((neihbour1,neighbour2) => (neihbour1._1,neihbour1._2 + "|" + neighbour2._2))

      //there we map all values. thus we have tuples that look like (source, weight, State -> discover of not,
      // String made of all neigbours, path)
      var solutionSet = groupedFile.map(row => {
        if (row._1 == initial_vertice) {
          (row._1,0,"UNDISCOVERED",row._2,initial_vertice)
        } else {
          (row._1,INFINITE,"UNDISCOVERED",row._2,"")
        }
      })

      // first workset is made of initial vertice tuple
      var workSet = solutionSet.filter(row => row._2<INFINITE && row._3 == "UNDISCOVERED")


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

      // I have made my own iteration loop because I can't use the proper one
      var iteration = 0
      while (workSet.count() != 0 && iteration <= maxIteration) {
        print("iteration: " + iteration +"\n")
        val nextStep = propagateThroughMaze(solutionSet,workSet)
        solutionSet = nextStep._1
        workSet = nextStep._2
        iteration += 1
      }

      val FinalsolutionSet = solutionSet.filter(row => row._2 < INFINITE).map(row => row._5 +  ": " + row._2).setParallelism(1)
      FinalsolutionSet.writeAsText(outputPath)
      env.execute()
      val t1 = System.nanoTime()

      print("execution time = " + (t1-t0) + "ns")
      //apply iteration delta function with propagateThroughMaze as step function do not work
      //val output = solutionSet.iterateDelta(initialWorkSet,maxIteration,keyFields = Array(0))(stepFunction = {(solution,workset) => propagateThroughMaze(solution,workset)})
    }
}
