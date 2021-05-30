import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object ShortestPath {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("shortestPath").setMaster("local[2]").set("spark.executor.memory", "5g")
    val sc = new SparkContext(conf)
    val initial_vertice = "17038"

    val file = sc.textFile("input/ca-GrQc.txt")
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
        (line._1,5243,"UNDISCOVERED",line._2,"") // max possible length path 5242 => 5243 == INFINITE
      }
    }) //there we grouped all values. thus we have tuples that look like (source, weight, State -> discover of not,
    // CompactBuffer made tuple of all neigbours (source,neigbours), path)

    var i =0 // variable init for test
    while (AdjencyList.filter(row => row._3 == "UNDISCOVERED").count() != 0 && i < 5){ //we stop the loop earlier in order to test our algorithm
      //otherwise we stop the loop when all vertice are discover. Due to the fact that all path weight are equal to 1
      //when a vertice is discover the algorithm took the shortest path

      //Debug variables
      i += 1
      print(AdjencyList.filter(row => row._3 == "UNDISCOVERED").count())

      //RDD made of reached variables that are reach but their state is not yet init to DISCOVER
      // these variables are the next sources in our algorithm.
      //during the first init the RDD is made of one row with the initial vertice as source
      //next step it is made of all neighbours of the initial vertice and so on
      val AdjencyListDiscovered = AdjencyList.filter(row => row._3 == "UNDISCOVERED" && row._2 < 5243)

      AdjencyList = AdjencyList.map(row => {
        if (row._3 == "UNDISCOVERED" && row._2 < 5243){
          (row._1,row._2,"DISCOVERED",row._4,row._5)
        } else {
          row
        }
      }) //we change the state of reached vertice to DISCVOER


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
        AdjencyList.first() //I try this to curb the computing time oh the algorithm
      }
      AdjencyList.foreach(row => if(row._3=="DISCOVERED"){println(row)})
    }
  }
}
