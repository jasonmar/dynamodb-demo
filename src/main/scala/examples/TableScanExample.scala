/*
 *    Copyright 2016 Jason Mar
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package examples

import app.ItemTypes
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import dynamodb.{Dynamo, Items}

import scala.collection.JavaConverters._

/**
  * This example performs a table scan with result limit
  * Scan Example 1: simple scan with limit 5
  * Scan Example 2: recursive scan with limit 3 and depth 2 (9 items total)
  */
object TableScanExample extends App {
  val startTimeMillis = System.currentTimeMillis()

  def log(msg: String) = {
    val time = (System.currentTimeMillis() - startTimeMillis).toString
    System.out.println(time + " " + msg)
  }

  log("initializing DynamoDB client")
  val client = Dynamo.client(Some("http://localhost:8000"))

  log("initializing DynamoDB connection")
  val dynamoDB = new DynamoDB(client)
  val ID_TABLE_NAME = "ids"

  log("get table")
  val table1 = dynamoDB.getTable(ID_TABLE_NAME)
  table1.waitForActive()

  log("scan table")
  val result = Items.scanTable(client, ID_TABLE_NAME, Vector(ItemTypes.STRING_ATTRIBUTE), limit = 5)

  log("get items")
  val items = result.getItems.asScala.map(x => x.asScala.toMap)

  log("read attributes")
  val attributes = items.flatMap{item =>
    item.map{attr =>
      (attr._1, attr._2.getS)
    }
  }

  log("print attributes")
  attributes.foreach{attr =>
    System.out.println(attr)
  }

  log("check for more")
  val last = Option(result.getLastEvaluatedKey)

  log("table has more items remaining?")
  System.out.println(last.nonEmpty)
  last match {
    case Some(key) => log("last key: " + key.asScala.toMap.toString)
    case _ =>
  }

  log("scan table recursively")
  val result2 = Items.scanRec(client, ID_TABLE_NAME, Vector(ItemTypes.STRING_ATTRIBUTE), limit = 3, recLimit = 2)
  val items2 = result2.items.view.flatMap{item => item.map{x => (x._1, x._2.getS)}}

  log("print items")
  items2.foreach(System.out.println)

  log("table has more items remaining?")
  val last2 = Option(result2.results.getLastEvaluatedKey)
  last2 match {
    case Some(key) => log("last key: " + key.asScala.toMap.toString)
    case _ =>
  }
}
