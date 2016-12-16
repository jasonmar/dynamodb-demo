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

package dynamodb

import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.model._

import scala.collection.JavaConverters._

import dynamodb.Dynamo._

object CreateTable {

  sealed trait Attribute extends AWSSDKObjectBuilder {
    val name: String
    val code: String
    def result(): AttributeDefinition = {
      new AttributeDefinition()
        .withAttributeName(name)
        .withAttributeType(code)
    }
  }
  trait NumberType extends Attribute { override val code = "N" }
  trait BinaryType extends Attribute { override val code = "B" }
  trait StringType extends Attribute { override val code = "S" }

  case class AttributeString(name: String) extends StringType
  case class AttributeBinary(name: String) extends BinaryType
  case class AttributeNumber(name: String) extends NumberType

  sealed trait SchemaElement extends AWSSDKObjectBuilder {
    val name: String
    val keyType: KeyType
    def result(): KeySchemaElement = {
      new KeySchemaElement()
        .withAttributeName(name)
        .withKeyType(keyType)
    }
  }
  trait HashType extends SchemaElement { override val keyType = KeyType.HASH }
  trait RangeType extends SchemaElement { override val keyType = KeyType.RANGE }

  case class PartitionKey(name: String) extends HashType
  case class SortKey(name: String) extends RangeType

  case class DynamoThroughput(rcu: Long, wcu: Long) extends AWSSDKObjectBuilder {
    override def result(): ProvisionedThroughput = {
      new ProvisionedThroughput()
        .withReadCapacityUnits(rcu)
        .withWriteCapacityUnits(wcu)
    }
  }

  sealed trait DynamoProjection extends AWSSDKObjectBuilder {
    val projectionType: ProjectionType
    val nonKeyAttributes: Vector[String]
    override def result(): Projection = {
        new Projection()
          .withNonKeyAttributes(nonKeyAttributes.asJavaCollection)
          .withProjectionType(projectionType)
    }
  }
  trait AllProjection extends DynamoProjection { override val projectionType = ProjectionType.ALL }
  trait IncludeProjection extends DynamoProjection { override val projectionType = ProjectionType.INCLUDE }
  trait KeysProjection extends DynamoProjection { override val projectionType = ProjectionType.KEYS_ONLY }

  case class ProjectAll(nonKeyAttributes: Vector[String]) extends AllProjection
  case class ProjectInclude(nonKeyAttributes: Vector[String]) extends IncludeProjection
  case class ProjectKeys(nonKeyAttributes: Vector[String]) extends KeysProjection

  case class GSI(name: String, kse: Vector[SchemaElement], projection: DynamoProjection, throughput: DynamoThroughput) extends AWSSDKObjectBuilder {
    override def result(): GlobalSecondaryIndex = {
      new GlobalSecondaryIndex()
        .withIndexName(name)
        .withKeySchema(kse.map(_.result()).asJavaCollection)
        .withProjection(projection.result())
        .withProvisionedThroughput(throughput.result())
    }
  }

  case class LSI(name: String, kse: Vector[SchemaElement], projection: DynamoProjection) extends AWSSDKObjectBuilder {
    override def result(): LocalSecondaryIndex = {
      new LocalSecondaryIndex()
        .withIndexName(name)
        .withKeySchema(kse.map(_.result()).asJavaCollection)
        .withProjection(projection.result())
    }
  }

  class ImprovedCreateTableRequest(r: CreateTableRequest) {
    def withGSIOpt(gsiOpt: Option[GSI]): CreateTableRequest = {
      gsiOpt match {
        case Some(gsi) => r.withGlobalSecondaryIndexes(gsi.result())
        case _ => r
      }
    }
    def withLSIOpt(lsiOpt: Option[LSI]): CreateTableRequest = {
      lsiOpt match {
        case Some(lsi) => r.withLocalSecondaryIndexes(lsi.result())
        case _ => r
      }
    }
  }
  implicit def improveCreateTableRequest(r: CreateTableRequest): ImprovedCreateTableRequest = new ImprovedCreateTableRequest(r)

  def createTable(
    dynamoDB: DynamoDB,
    tableName: String,
    attributeDefinitions: Vector[Attribute],
    partitionKey: PartitionKey,
    sortKeyOpt: Option[SortKey] = None,
    throughput: DynamoThroughput,
    gsiOpt: Option[GSI] = None,
    lsiOpt: Option[LSI] = None
  ): Table = {

    val keySchemaElements = sortKeyOpt match {
      case Some(sortKey) => Vector(partitionKey.result(), sortKey.result())
      case _ => Vector(partitionKey.result())
    }

    val definitions = attributeDefinitions.map(_.result())

    val request = new CreateTableRequest()
      .withTableName(tableName)
      .withKeySchema(keySchemaElements.asJavaCollection)
      .withAttributeDefinitions(definitions.asJavaCollection)
      .withProvisionedThroughput(throughput.result())
      .withGSIOpt(gsiOpt)
      .withLSIOpt(lsiOpt)

    dynamoDB.createTable(request)
  }
}
