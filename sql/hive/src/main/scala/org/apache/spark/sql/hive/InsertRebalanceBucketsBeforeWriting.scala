/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.internal.SQLConf

case class InsertRebalanceBucketsBeforeWriting(session: SparkSession) extends Rule[LogicalPlan] {
  private val AUTO_BUCKET = "autoBucket"

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.getConf(SQLConf.ADAPTIVE_OPTIMIZE_BUCKET_PARTITIONING_ENABLED) &&
      conf.adaptiveExecutionEnabled) {
      applyInternal(plan)
    } else {
      plan
    }
  }

  def applyInternal(plan: LogicalPlan): LogicalPlan = plan match {
    case i @ InsertIntoHadoopFsRelationCommand(_, sp, _, pc, bucket, _, _, query, _, table, _, _)
      if isRebalanceBucketsEnabled(table) && canInsertRebalanceBuckets(query) &&
        sp.size == pc.size =>
//      val bucketColumns = query.output.filter{ attr =>
//        bucket.get.bucketColumnNames.contains(attr.name)
//      }
      val resolver = session.sessionState.conf.resolver
      val bucketColumns = bucket.get.bucketColumnNames
          .flatMap(col => query.output.find(attr => resolver(attr.name, col)))
      i.copy(query = RebalanceBuckets(bucketColumns, query, Option(1024)))

    case _ => plan
  }

  def isRebalanceBucketsEnabled(table: Option[CatalogTable]): Boolean = {
    def isAutoBucketEnabled(props: Map[String, String]): Boolean =
      props.contains(AUTO_BUCKET) && "true".equalsIgnoreCase(props(AUTO_BUCKET))

    table.isDefined && table.get.bucketSpec.isDefined && isAutoBucketEnabled(table.get.properties)
  }

  def canInsertRebalanceBuckets(plan: LogicalPlan): Boolean = {
    def canInsert(p: LogicalPlan): Boolean = p match {
      case Project(_, child) => canInsert(child)
      case SubqueryAlias(_, child) => canInsert(child)
      case Limit(_, _) => false
      case _: Sort => false
      case _: RepartitionByExpression => false
      case _: Repartition => false
      case _ => true
    }

    plan.resolved && canInsert(plan)
  }
}