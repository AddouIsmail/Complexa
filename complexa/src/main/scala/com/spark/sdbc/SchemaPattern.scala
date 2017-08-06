package com.spark.sdbc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._ 
import org.apache.hadoop.fs.shell.Tail
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLImplicits$
import org.apache.spark.sql.DataFrame
import java.io.IOException

case class tableDF(var df : DataFrame, table_name : String, parent_name : String )
case class SelectStatement(attribute : String, attribute_path : Array[String], foreign_key: String,  table_name : String, parent_name: String)

// node ( node's name, parent's name ) 
case class Node(node : String, nested_tables : Array[Node]) 
class treeFormatException(message: String) extends Exception(message){
  
}
class SchemaPattern(hdfsPath : String, masterUrl : String, nodesNumber : Int) {
       val conf = new SparkConf().setAppName("Spark Pi").setMaster(masterUrl)
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      val df  = sqlContext.read.parquet(hdfsPath).cache()
      val tabledf : tableDF = tableDF(df,"root","") 
  import sqlContext.implicits._
  def SchemaToTables(tabledf : tableDF, schema : ArrayBuffer[tableDF] ): ArrayBuffer[tableDF] = { 
    // table of simple fields to add on the new schema 
    var simpletable = tabledf
    
    // Retrieves complex fields of the first level of a schema (arrays and structs)
    val complex_fields = tabledf.df.schema.fields
        .filter(p => p.dataType.typeName.equalsIgnoreCase("array") || p.dataType.typeName.equalsIgnoreCase("struct"))
    
    // For each complex field we do:
    for( item <- complex_fields ) {
      // IF ARRAY
      if(item.dataType.typeName.equalsIgnoreCase("array")){
        //Reach the next level of complexity of arrays
         var nestedTable = tableDF(tabledf.df.select(explode(tabledf.df(item.name)).as(item.name)) , item.name, tabledf.table_name)
          try {
          // select all the items 
          nestedTable.df = nestedTable.df.select(item.name+".*")
        } catch {
          // case of array of simple field this code throws exception ( we do nothing )
          case t: Throwable => println(item.name+" : C'est un tableau d'éléments simples ") 
        }
        // Jump to the next level of complexity 
        SchemaToTables( nestedTable , schema )
      }else{
        // Get all the structure's fields
        var nestedStructure = tableDF(tabledf.df.select(item.name+".*"),item.name, tabledf.table_name )
        // Jump to the next level
        SchemaToTables( nestedStructure , schema )
      }
    }
    
    // For each level we add only simple fields to the tableDF class
    for( item <- complex_fields ) {
      simpletable.df = simpletable.df.drop(item.name)
    }
    
    schema += simpletable
    }
      
  def maxTables(x: String, y: String):String = {
    if( x.length() > y.length() ) 
      x
    else
      y
  }
  
  def transpose[String](xs : List[List[String]]) : List[List[String]] ={
    xs.filter(_.nonEmpty) match {
      case Nil => Nil
      case ys: List[List[String]] => ys.map{_.head}::transpose(ys.map{_.tail})
    }
  }


  def transposeArray(paths : Array[Array[(String,String)]]):Array[Array[(String,String)]] = {
    val array_paths = paths
    array_paths.foreach( p => p.map(print))
    println(" transpose =>")
    val transposed_array = transpose(array_paths.map(p => p.toList).toList).map(p => p.toArray).toArray
    transposed_array
  }
  
  def detectTree( path_arrays : Array[Array[(String, String)]] ) : Node = { 
    val tree = transposeArray(path_arrays).map(p => p.distinct)
    for ( item <- tree.reverse ) {
      item.map( p => p._1 ) 
    }
    new Node("",null)
  }
  
  def explodeNestedTables(purged_tables : ArrayBuffer[ArrayBuffer[String]], dataframes : ArrayBuffer[DataFrame] )  {
    dataframes ++ purged_tables(0).map( 
        p => df.select(  col(p) , explode( col(p) ) )
    )
  }

     def getRequest(statements : Array[SelectStatement]):Node = {
       statements.foreach(println)
       println("1. getRequest ______________________________")
       val paths = statements.map( p => p.attribute_path ).map(p => toNode(p))
       println("2. getRequest ______________________________")
       paths.reduceLeft((X,Y) => merge(X,Y))
     }
    
    def toNode(arraypath : Array[String]): Node = {
      //println("1. ToNode______________________________")
      if(arraypath.tail.nonEmpty){
        //println("1.1 ToNode ______________________________")
        val childnode = toNode(arraypath.tail)
        //println("1.2 ToNode ______________________________")
        new Node(arraypath.head, Array(childnode))
      }else
      //println("2. ToNode ______________________________")
      new Node(arraypath.head,null) 
    }
    
    def printTree(node : Node){
        println(node.node)
        println("================")
        node.nested_tables.map(printTree(_))
    }
     
    def mergeChilds( leftChilds : Array[Node], rightChild : Array[Node]): Array[Node]={
      println("leftChilds's number ="+leftChilds.size)
      println("1. MergeChilds______________________________")
      if (leftChilds.filter(p => p.node == rightChild.head.node).isEmpty){
        println("1.1 MergeChilds______________________________")
          leftChilds ++ rightChild
        }
      else{
        println("1.2 MergeChilds______________________________")
        leftChilds.map( p => {
            if(p.node == rightChild.head.node ){
              println("merging p and rightchild.head :"+rightChild.head)
              merge(p,rightChild.head)
            }else{
              p
            }
        })        
      }
    }
   
    def merge(node1 : Node, node2 : Node) : Node = {
      println("1. Merge______________________________")
        if(node1.node != node2.node) {
          println("1.1 exception________________________")
          throw new treeFormatException("cannot merge two trees with different root")
        }
      println("2. Merge______________________________"+node1.nested_tables)  
      
      Node(node1.node, mergeChilds(node1.nested_tables, node2.nested_tables))
    }
    
//    val table_paths = purgeTables(attributes)
//    //table_paths.map( p => df.select($"reclocAmd",explode($"passengersList")).show
//    
//        
    //val flattened = df.select($"name", explode($"schools").as("schools_flat"))
  
}