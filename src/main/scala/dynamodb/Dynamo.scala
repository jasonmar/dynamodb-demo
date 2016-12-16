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

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClient}

object Dynamo {
  trait AWSSDKObjectBuilder {
    def result(): Any
  }

  class ImprovedAmazonDynamoDBClient(c: AmazonDynamoDBClient){
    def withEndpointOpt(endpointOpt: Option[String]): AmazonDynamoDB = {
      endpointOpt match {
        case Some(endpoint) => c.withEndpoint(endpoint)
        case _ => c
      }
    }
  }
  implicit def improveAmazonDynamoDB(c: AmazonDynamoDBClient): ImprovedAmazonDynamoDBClient = new ImprovedAmazonDynamoDBClient(c)

  def client(endpoint: Option[String] = None): AmazonDynamoDB = {
    new AmazonDynamoDBClient(new ProfileCredentialsProvider()).withEndpointOpt(endpoint)
  }
}
