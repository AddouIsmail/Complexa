package com.spark.complexa

import org.scalatra._
import org.scalatra.CorsSupport
import org.json4s.{DefaultFormats,Formats}
import org.scalatra.json._
import org.apache.spark._
import com.spark.sdbc.tableDF
import com.spark.sdbc.SelectStatement
import com.spark.sdbc.SchemaPattern
import scala.collection.mutable.ArrayBuffer

case class TableSC(table_schema : Any, table_name : String , parent_name : String)

class complexaServlet extends ComplexaStack  with JacksonJsonSupport with CorsSupport{
  
  protected implicit lazy val jsonFormats: Formats = DefaultFormats
  val hdfsPath = "/Users/ismailaddou/Downloads/sample"  
  var schema = ArrayBuffer.empty[tableDF]
  var sch = new SchemaPattern(hdfsPath,"spark://MacBook-Pro-de-Ismail.local:7077",1)
  sch.SchemaToTables(sch.tabledf, schema) 
  schema.foreach(
    p => {
      println("parent"+ p.parent_name)
      println("table"+ p.table_name)
      p.df.printSchema()
  })
  sch.sc.stop()
  
  options("/*"){
    response.setHeader("Access-Control-Allow-Headers", request.getHeader("Access-Control-Request-Headers"));
  }
  
  before(){
       //contentType = formats("json")
  }
  get("/") {
       schema.map(p => TableSC(p.df.schema, p.table_name,p.parent_name)).toList
  }
  post("/"){
        
        val selected = parsedBody.extract[Array[SelectStatement]]
        selected.foreach(println)
        response.addHeader("ACK", "GOT IT")
  }

}
