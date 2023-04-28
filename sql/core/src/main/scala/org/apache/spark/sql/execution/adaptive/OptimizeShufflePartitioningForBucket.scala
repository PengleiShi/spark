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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, RebalanceBuckets}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.REBALANCE_BUCKETS
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.internal.SQLConf

object OptimizeShufflePartitioningForBucket extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.ADAPTIVE_OPTIMIZE_BUCKET_PARTITIONING_ENABLED) ||
      !plan.isInstanceOf[InsertIntoHadoopFsRelationCommand] ||
      !plan.containsPattern(REBALANCE_BUCKETS)) {
      return plan
    }
    val join = plan.collectFirst {
      case j: Join => j
    }
    if (join.isEmpty || !join.get.children.forall(_.isInstanceOf[LogicalQueryStage])) {
      return plan
    }

    val leftStats = join.get.left.stats
    val rightStats = join.get.right.stats
    val sizePerRow = leftStats.sizeInBytes / leftStats.rowCount.get +
      rightStats.sizeInBytes / rightStats.rowCount.get
    val estimateTotalSize = leftStats.rowCount.get * sizePerRow
    val advisorySize = conf.getConf(SQLConf.ADVISORY_BUCKET_SIZE_IN_BYTES)
    val targetNumPartitions = math.ceil(estimateTotalSize.toDouble/advisorySize.toDouble)
    // bucket num should be power of 2
    val advisoryNumBuckets = math.pow(2, math.ceil(math.log(targetNumPartitions)/math.log(2))).toInt

    // todo bucket num should between 1024 and 4096

    plan match {
      case i @ InsertIntoHadoopFsRelationCommand(
        _, _, _, _, Some(bucketSpec), _, _, _, _, _, _, _) =>
        val newBucketSpec = bucketSpec.copy(numBuckets = advisoryNumBuckets)
        i.copy(bucketSpec = Option(newBucketSpec)) transformDown {
          case r: RebalanceBuckets =>
            r.copy(optNumPartitions = Option(advisoryNumBuckets))
        }
      case _ => plan
    }
  }
}
