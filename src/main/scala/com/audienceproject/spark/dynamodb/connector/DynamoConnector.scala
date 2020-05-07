/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied.  See the License for the
  * specific language governing permissions and limitations
  * under the License.
  *
  * Copyright © 2018 AudienceProject. All rights reserved.
  */
package com.audienceproject.spark.dynamodb.connector

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicSessionCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, ItemCollection, ScanOutcome}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBAsync, AmazonDynamoDBAsyncClientBuilder, AmazonDynamoDBClientBuilder}
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest
import org.apache.spark.sql.sources.Filter
import com.amazonaws.retry.PredefinedBackoffStrategies.EqualJitterBackoffStrategy
import com.amazonaws.retry.RetryPolicy
import com.amazonaws.{ClientConfiguration, PredefinedClientConfigurations}

private[dynamodb] trait DynamoConnector {

    @transient private lazy val properties = sys.props

    def getDynamoDB(region: Option[String] = None, roleArn: Option[String] = None): DynamoDB = {
        val client: AmazonDynamoDB = getDynamoDBClient(region, roleArn)
        new DynamoDB(client)
    }

    private def getDynamoDBClient(region: Option[String] = None, roleArn: Option[String] = None): AmazonDynamoDB = {
        val chosenRegion = region.getOrElse(properties.getOrElse("aws.dynamodb.region", "us-east-1"))
        val credentials = getCredentials(chosenRegion, roleArn)

        properties.get("aws.dynamodb.endpoint").map(endpoint => {
            AmazonDynamoDBClientBuilder.standard()
                .withCredentials(credentials)
                .withEndpointConfiguration(new EndpointConfiguration(endpoint, chosenRegion))
                .withClientConfiguration(getClientConfiguration)
                .build()
        }).getOrElse(
            AmazonDynamoDBClientBuilder.standard()
                .withCredentials(credentials)
                .withRegion(chosenRegion)
                .withClientConfiguration(getClientConfiguration)
                .build()
        )
    }

    def getDynamoDBAsyncClient(region: Option[String] = None, roleArn: Option[String] = None): AmazonDynamoDBAsync = {
        val chosenRegion = region.getOrElse(properties.getOrElse("aws.dynamodb.region", "us-east-1"))
        val credentials = getCredentials(chosenRegion, roleArn)

        properties.get("aws.dynamodb.endpoint").map(endpoint => {
            AmazonDynamoDBAsyncClientBuilder.standard()
                .withCredentials(credentials)
                .withEndpointConfiguration(new EndpointConfiguration(endpoint, chosenRegion))
                .withClientConfiguration(getClientConfiguration)
                .build()
        }).getOrElse(
            AmazonDynamoDBAsyncClientBuilder.standard()
                .withCredentials(credentials)
                .withRegion(chosenRegion)
                .withClientConfiguration(getClientConfiguration)
                .build()
        )
    }

    private def getClientConfiguration: ClientConfiguration = {
        val clientConfiguration = PredefinedClientConfigurations.dynamoDefault()

        /* We go with custom retry policy of retry till 30 seconds */
        val baseDelayMs = 100 /* 100 MilliSeconds */
        val maxDelayMs  = 30 * 1000 /* 30 Seconds */
        val maxRetry    = 30
        val honorMaxErrorRetryInClientConfig = false
        val retryPolicy = new RetryPolicy(null,
            new EqualJitterBackoffStrategy(baseDelayMs, maxDelayMs), maxRetry, honorMaxErrorRetryInClientConfig)
        clientConfiguration.setRetryPolicy(retryPolicy)
        clientConfiguration
    }

    /**
      * Get credentials from a passed in arn or from profile or return the default credential provider
      **/
    private def getCredentials(chosenRegion: String, roleArn: Option[String]) = {
        roleArn.map(arn => {
            val stsClient = properties.get("aws.sts.endpoint").map(endpoint => {
                AWSSecurityTokenServiceClientBuilder
                    .standard()
                    .withCredentials(new DefaultAWSCredentialsProviderChain)
                    .withEndpointConfiguration(new EndpointConfiguration(endpoint, chosenRegion))
                    .build()
            }).getOrElse(
                // STS without an endpoint will sign from the region, but use the global endpoint
                AWSSecurityTokenServiceClientBuilder
                    .standard()
                    .withCredentials(new DefaultAWSCredentialsProviderChain)
                    .withRegion(chosenRegion)
                    .build()
            )
            val assumeRoleResult = stsClient.assumeRole(
                new AssumeRoleRequest()
                    .withRoleSessionName("DynamoDBAssumed")
                    .withRoleArn(arn)
            )
            val stsCredentials = assumeRoleResult.getCredentials
            val assumeCreds = new BasicSessionCredentials(
                stsCredentials.getAccessKeyId,
                stsCredentials.getSecretAccessKey,
                stsCredentials.getSessionToken
            )
            new AWSStaticCredentialsProvider(assumeCreds)
        }).orElse(properties.get("aws.profile").map(new ProfileCredentialsProvider(_)))
            .getOrElse(new DefaultAWSCredentialsProviderChain)
    }

    val keySchema: KeySchema

    val readLimit: Double

    val itemLimit: Int

    val totalSegments: Int

    val filterPushdownEnabled: Boolean

    def scan(segmentNum: Int, columns: Seq[String], filters: Seq[Filter]): ItemCollection[ScanOutcome]

    def isEmpty: Boolean = itemLimit == 0

    def nonEmpty: Boolean = !isEmpty

}
