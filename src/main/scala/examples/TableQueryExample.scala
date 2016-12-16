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

import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import dynamodb.{Dynamo, Items}

/**
  * This queries a table by primary key only
  * query 1: find item with id = "1012"
  * query 2: find item with id = "1012", filter abc <> "1012"
  */
object TableQueryExample extends App {
  val startTimeMillis = System.currentTimeMillis()

  def log(msg: String) = {
    val time = (System.currentTimeMillis() - startTimeMillis).toString
    System.out.println(time + " " + msg)
  }

  log("initializing DynamoDB client")
  val client = Dynamo.client(Some("http://localhost:8000"))

  log("initializing DynamoDB connection")
  val dynamoDB = new DynamoDB(client)
  val tableName = "ids"

  log("get table")
  val table1 = dynamoDB.getTable(tableName)
  table1.waitForActive()

  log("value map")
  val valueMap = new ValueMap()
    .withString(":v_id", "1012")
    .withLong(":v_abc", 1012)

  val spec = Items.query(valueMap, "id = :v_id", "abc = :v_abc")

  log("query table")
  val items = table1.query(spec)
  Items.print(items)

  val spec2 = Items.query(valueMap, "id = :v_id", "abc <> :v_abc")
  log("query table")
  val items2 = table1.query(spec2)
  Items.print(items2)

}
