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
import app.ItemTypes.Id
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import dynamodb.CreateTable._
import dynamodb.{Dynamo, Items}

/**
  * This example creates a table with primary key "id"
  */
object TableCreateExample extends App {
  val startTimeMillis = System.currentTimeMillis()

  def log(msg: String) = {
    val time = (System.currentTimeMillis() - startTimeMillis).toString
    System.out.println(time + " " + msg)
  }

  log("initialize client")
  val client = Dynamo.client(Some("http://localhost:8000"))

  log("connect to DynamoDB")
  val dynamoDB = new DynamoDB(client)

  val tableName = "ids"

  // Attributes to be used in Partition Key or Sort Key
  val attributes = Vector(AttributeString(ItemTypes.PRIMARY_KEY))

  log("create table")
  val table = createTable(dynamoDB, tableName, attributes, PartitionKey(ItemTypes.PRIMARY_KEY), throughput = DynamoThroughput(10, 10))
  val desc = table.waitForActive()

  log("generate items")
  val ids = (1000 to 1020).map{ x => Id(x.toString, x.toString, x) }.map(_.result()).toVector

  log("write items")
  Items.write(dynamoDB, tableName, ids)

  log("get items")
  val items = (1000 to 1010).map{x =>
    table.getItem(ItemTypes.PRIMARY_KEY, x.toString)
  }

  log("print items")
  items.foreach{item => System.out.println(item.toJSONPretty)}

}
