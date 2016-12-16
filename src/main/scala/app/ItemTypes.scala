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

package app

import dynamodb.Items.DynamoItemBuilder
import com.amazonaws.services.dynamodbv2.document.{Item, PrimaryKey}

object ItemTypes {

  val PRIMARY_KEY = "id"
  val SORT_KEY = "abc"
  val STRING_ATTRIBUTE = "xyz"

  case class Id(id: String, xyz: String, abc: Long) extends DynamoItemBuilder {
    override def pk(): PrimaryKey = {
      new PrimaryKey(PRIMARY_KEY, id)
    }
    override def result(): Item = {
      new Item()
        .withPrimaryKey(pk())
        .withString("xyz", xyz)
        .withLong(SORT_KEY, abc)
    }
  }

  case class Document(id: String, xyz: String, abc: Long) extends DynamoItemBuilder {
    override def pk(): PrimaryKey = {
      new PrimaryKey(PRIMARY_KEY, id)
    }
    override def result(): Item = {
      new Item()
        .withPrimaryKey(pk())
        .withString("xyz", xyz)
        .withLong(SORT_KEY, abc)
    }
  }

}
