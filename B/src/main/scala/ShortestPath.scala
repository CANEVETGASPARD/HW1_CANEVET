import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object ShortestPath {

  def main(args: Array[String]) {
    val t0 = System.nanoTime()
    val conf = new SparkConf().setAppName("shortestPath").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //problem variables
    val initial_vertice = "17038"
    var clusterFullyExplored = false
    val INFINITE = 5243 // max possible length path 5242 => 5243 == INFINITE

    val inputPath = args(0) + "/ca-GrQc.txt"
    val outputPath = args(1) + "/task_II"

    //load and split file
    val file = sc.textFile(inputPath)
    val groupedFile = file.groupBy(line => {
      val lineArray = line.split("\t")
      if (lineArray.length == 2) {
        lineArray(0)
      }
    }) //(key,value) of all connection in the maze

    var AdjencyList = groupedFile.map(line => {
      if(line._1 == initial_vertice) {
        (line._1,0,"UNDISCOVERED",line._2, initial_vertice)
      } else{
        (line._1,INFINITE,"UNDISCOVERED",line._2,"")
      }
    }) //there we grouped all values. thus we have tuples that look like (source, weight, State -> discover of not,
    // CompactBuffer made tuple of all neigbours (source,neigbours), path)

    var i =0 // variable init for test
    var previousUndiscoverNumber = AdjencyList.filter(row => row._3 == "UNDISCOVERED").count()
    while (AdjencyList.filter(row => row._3 == "UNDISCOVERED").count() != 0 && !clusterFullyExplored){
      //We stop the loop when all vertices from the cluster are discover. Due to the fact that all path weight are equal to 1
      //when a vertice is discover the algorithm took the shortest path

      //RDD made of reached variables that are reach but their state is not yet init to DISCOVER
      // these variables are the next sources in our algorithm.
      //during the first init the RDD is made of one row with the initial vertice as source
      //next step it is made of all neighbours of the initial vertice and so on
      val AdjencyListDiscovered = AdjencyList.filter(row => row._3 == "UNDISCOVERED" && row._2 < INFINITE)

      AdjencyList = AdjencyList.map(row => {
        if (row._3 == "UNDISCOVERED" && row._2 < INFINITE){
          (row._1,row._2,"DISCOVERED",row._4,row._5)
        } else {
          row
        }
      }) //we change the state of reached vertice to DISCOVER


      for (source <- AdjencyListDiscovered.collect()){ //for every source

        val neighboursListBuffer = new ListBuffer[String]()
        val neighbours = source._4 //list of neighbours
        neighbours.foreach(neighbour => {
          val splitedNeighbour = neighbour.split("\t")
          if(splitedNeighbour.length == 2){
            neighboursListBuffer += splitedNeighbour(1)}
        }) // foreach neighbours we save only the value of the tuple

        //then our ListerBuffer is made of all neighbours of the source
        // we are now going to reach neighbour vertices
        AdjencyList = AdjencyList.map(row => {
          if(neighboursListBuffer.contains(row._1) && row._3 == "UNDISCOVERED"){ //if the neighbours is not already discovered
            if(source._1 == initial_vertice){ //we reach the vertice according of the source
              (row._1,source._2 + 1,"UNDISCOVERED",row._4,source._1.toString) //if the source is the intial vertice => step 1
              //then we do not have to add the path
            } else { //otherwise the path is initialise differently
              (row._1,source._2 + 1,"UNDISCOVERED",row._4,source._5 + "->" + source._1)
            }
          } else { //then if a row is discover or not in the neighbours
            (row._1,row._2,row._3,row._4,row._5) //we do nothing
          }
        })
      }
      AdjencyList.cache().foreach(row => if(row._3=="DISCOVERED"){println(row)}) //we cache and print output to debug

      //statement that make sure there are still undiscover vertices in the cluster
      var currentUndiscoverNumber = AdjencyList.filter(row => row._3 == "UNDISCOVERED").count()
      if (currentUndiscoverNumber == previousUndiscoverNumber){
        clusterFullyExplored = true
      }
      previousUndiscoverNumber = currentUndiscoverNumber

    }
    val output = AdjencyList.filter(row => row._2 < INFINITE).map(row => row._5 + "->" + row._1 + ": " + row._2)
    output.coalesce(1).saveAsTextFile(outputPath)
    val t1 = System.nanoTime()
    print("execution time = " + (t1-t0) + "ns")
  }
}
