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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils._
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.util.SchemaUtils

import java.io.IOException
import scala.util.Try

/**
 * A command for writing data to a [[HadoopFsRelation]].  Supports both overwriting and appending.
 * Writing to dynamic partitions is also supported.
 *
 * @param staticPartitions partial partitioning spec for write. This defines the scope of partition
 *                         overwrites: when the spec is empty, all partitions are overwritten.
 *                         When it covers a prefix of the partition keys, only partitions matching
 *                         the prefix are overwritten.
 * @param ifPartitionNotExists If true, only write if the partition does not exist.
 *                             Only valid for static partitions.
 */
case class InsertIntoHadoopFsRelationCommand(
                                              outputPath: Path,
                                              staticPartitions: TablePartitionSpec,
                                              ifPartitionNotExists: Boolean,
                                              partitionColumns: Seq[Attribute],
                                              bucketSpec: Option[BucketSpec],
                                              fileFormat: FileFormat,
                                              options: Map[String, String],
                                              query: LogicalPlan,
                                              mode: SaveMode,
                                              catalogTable: Option[CatalogTable],
                                              fileIndex: Option[FileIndex],
                                              outputColumnNames: Seq[String])
  extends DataWritingCommand {

  private lazy val parameters = CaseInsensitiveMap(options)

  private[sql] lazy val dynamicPartitionOverwrite: Boolean = {
    val partitionOverwriteMode = parameters.get("partitionOverwriteMode")
      // scalastyle:off caselocale
      .map(mode => PartitionOverwriteMode.withName(mode.toUpperCase))
      // scalastyle:on caselocale
      .getOrElse(SQLConf.get.partitionOverwriteMode)
    val enableDynamicOverwrite = partitionOverwriteMode == PartitionOverwriteMode.DYNAMIC
    // This config only makes sense when we are overwriting a partitioned dataset with dynamic
    // partition columns.
    enableDynamicOverwrite && mode == SaveMode.Overwrite &&
      staticPartitions.size < partitionColumns.length
  }

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    // Most formats don't do well with duplicate columns, so lets not allow that
    SchemaUtils.checkColumnNameDuplication(
      outputColumnNames,
      s"when inserting into $outputPath",
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    val fs = outputPath.getFileSystem(hadoopConf)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

    val partitionsTrackedByCatalog = sparkSession.sessionState.conf.manageFilesourcePartitions &&
      catalogTable.isDefined &&
      catalogTable.get.partitionColumnNames.nonEmpty &&
      catalogTable.get.tracksPartitionsInCatalog

    var initialMatchingPartitions: Seq[TablePartitionSpec] = Nil
    var customPartitionLocations: Map[TablePartitionSpec, String] = Map.empty
    var matchingPartitions: Seq[CatalogTablePartition] = Seq.empty

    // When partitions are tracked by the catalog, compute all custom partition locations that
    // may be relevant to the insertion job.
    if (partitionsTrackedByCatalog) {
      matchingPartitions = sparkSession.sessionState.catalog.listPartitions(
        catalogTable.get.identifier, Some(staticPartitions))
      initialMatchingPartitions = matchingPartitions.map(_.spec)
      customPartitionLocations = getCustomPartitionLocations(
        fs, catalogTable.get, qualifiedOutputPath, matchingPartitions)
    }

    val committer = FileCommitProtocol.instantiate(
      sparkSession.sessionState.conf.fileCommitProtocolClass,
      jobId = java.util.UUID.randomUUID().toString,
      outputPath = outputPath.toString,
      dynamicPartitionOverwrite = dynamicPartitionOverwrite)

    val doInsertion = if (mode == SaveMode.Append) {
      true
    } else {
      val pathExists = fs.exists(qualifiedOutputPath)
      (mode, pathExists) match {
        case (SaveMode.ErrorIfExists, true) =>
          throw new AnalysisException(s"path $qualifiedOutputPath already exists.")
        case (SaveMode.Overwrite, true) =>
          if (ifPartitionNotExists && matchingPartitions.nonEmpty) {
            false
          } else if (dynamicPartitionOverwrite) {
            // For dynamic partition overwrite, do not delete partition directories ahead.
            true
          } else {
            deleteMatchingPartitions(fs, qualifiedOutputPath, customPartitionLocations, committer)
            true
          }
        case (SaveMode.Overwrite, _) | (SaveMode.ErrorIfExists, false) =>
          true
        case (SaveMode.Ignore, exists) =>
          !exists
        case (s, exists) =>
          throw new IllegalStateException(s"unsupported save mode $s ($exists)")
      }
    }

    if (doInsertion) {

      def refreshUpdatedPartitions(updatedPartitionPaths: Set[String]): Unit = {
        val updatedPartitions = updatedPartitionPaths.map(PartitioningUtils.parsePathFragment)
        if (partitionsTrackedByCatalog) {
          val newPartitions = updatedPartitions -- initialMatchingPartitions
          if (newPartitions.nonEmpty) {
            AlterTableAddPartitionCommand(
              catalogTable.get.identifier, newPartitions.toSeq.map(p => (p, None)),
              ifNotExists = true).run(sparkSession)
          }
          // For dynamic partition overwrite, we never remove partitions but only update existing
          // ones.
          if (mode == SaveMode.Overwrite && !dynamicPartitionOverwrite) {
            val deletedPartitions = initialMatchingPartitions.toSet -- updatedPartitions
            if (deletedPartitions.nonEmpty) {
              AlterTableDropPartitionCommand(
                catalogTable.get.identifier, deletedPartitions.toSeq,
                ifExists = true, purge = false,
                retainData = true /* already deleted */).run(sparkSession)
            }
          }
        }
      }

      val updatedPartitionPaths =
        FileFormatWriter.write(
          sparkSession = sparkSession,
          plan = child,
          fileFormat = fileFormat,
          committer = committer,
          outputSpec = FileFormatWriter.OutputSpec(
            qualifiedOutputPath.toString, customPartitionLocations, outputColumns),
          hadoopConf = hadoopConf,
          partitionColumns = partitionColumns,
          bucketSpec = bucketSpec,
          statsTrackers = Seq(basicWriteJobStatsTracker(hadoopConf)),
          options = options)


      // update metastore partition metadata
      if (updatedPartitionPaths.isEmpty && staticPartitions.nonEmpty
        && partitionColumns.length == staticPartitions.size) {
        // Avoid empty static partition can't loaded to datasource table.
        val staticPathFragment =
          PartitioningUtils.getPathFragment(staticPartitions, partitionColumns)
        refreshUpdatedPartitions(Set(staticPathFragment))
      } else {
        refreshUpdatedPartitions(updatedPartitionPaths)
      }

      // refresh cached files in FileIndex
      fileIndex.foreach(_.refresh())
      // refresh data cache if table is cached
      sparkSession.sharedState.cacheManager.recacheByPath(sparkSession, outputPath, fs)

      if (catalogTable.nonEmpty) {
        CommandUtils.updateTableStats(sparkSession, catalogTable.get)

        // TODO: ------------ start：二次开发代码 --------------- //
        if (catalogTable.get.partitionColumnNames.nonEmpty && updatedPartitionPaths.nonEmpty) {
          updatePartitionsMetadata(sparkSession, updatedPartitionPaths)
        }
        // TODO: ------------ end：二次开发代码 --------------- //
      }

    } else {
      logInfo("Skipping insertion into a relation that already exists.")
    }

    Seq.empty[Row]
  }

  /**
   * Update the specified partition metadata information.
   */
  private def updatePartitionsMetadata(sparkSession: SparkSession,
                                       updatedPartitionPaths: Set[String]): Unit = {
    logInfo("Current partition table, will update partition information soon.")
    val catalog = sparkSession.sessionState.catalog
    val identifier = catalogTable.get.identifier

    try {
      val partitions = updatedPartitionPaths.map(partitionPath => Try {
        val partitionSpec = partitionPath.split("/").map(_.split("="))
          .filter(_.length == 2).map {case Array(a, b) => (a, b)}.toMap

        catalog.getPartition(identifier, partitionSpec)
      })

      val newPartitions = partitions.filter(_.isSuccess).map(_.get)
        .zipWithIndex.flatMap { case (p, _) =>
        // Statistical partition file size
        val newSize = CommandUtils.calculateSingleLocationSize(
          sparkSession.sessionState, identifier, Some(p.location))

        val rowCount = if (p.stats.isDefined && p.stats.get.rowCount.isDefined) {
          p.stats.get.rowCount.get
        } else BigInt(1)

        val newStats = CommandUtils.compareAndGetNewStats(p.stats, newSize, Some(rowCount))
        val numFiles = p.parameters.getOrElse("numFiles", "1")

        val newStatParameters =
          Map("numFiles" -> numFiles,
            "rawDataSize" -> newSize.toString,
            "totalSize" -> newSize.toString)
        val newParameters = p.parameters ++ newStatParameters
        newStats.map(_ => p.copy(stats = newStats, parameters = newParameters))
      }

      // update metastore partition metadata
      catalog.alterPartitions(identifier, newPartitions.toSeq)
      logInfo(s"All partition information updates have been completed")
    } catch {
      case e: Throwable => logError(
        "Partition table metadata information update failed.", e)
    }
  }

  /**
   * Deletes all partition files that match the specified static prefix. Partitions with custom
   * locations are also cleared based on the custom locations map given to this class.
   */
  private def deleteMatchingPartitions(
                                        fs: FileSystem,
                                        qualifiedOutputPath: Path,
                                        customPartitionLocations: Map[TablePartitionSpec, String],
                                        committer: FileCommitProtocol): Unit = {
    val staticPartitionPrefix = if (staticPartitions.nonEmpty) {
      "/" + partitionColumns.flatMap { p =>
        staticPartitions.get(p.name).map(getPartitionPathString(p.name, _))
      }.mkString("/")
    } else {
      ""
    }
    // first clear the path determined by the static partition keys (e.g. /table/foo=1)
    val staticPrefixPath = qualifiedOutputPath.suffix(staticPartitionPrefix)
    if (fs.exists(staticPrefixPath) && !committer.deleteWithJob(fs, staticPrefixPath, true)) {
      throw new IOException(s"Unable to clear output " +
        s"directory $staticPrefixPath prior to writing to it")
    }
    // now clear all custom partition locations (e.g. /custom/dir/where/foo=2/bar=4)
    for ((spec, customLoc) <- customPartitionLocations) {
      assert(
        (staticPartitions.toSet -- spec).isEmpty,
        "Custom partition location did not match static partitioning keys")
      val path = new Path(customLoc)
      if (fs.exists(path) && !committer.deleteWithJob(fs, path, true)) {
        throw new IOException(s"Unable to clear partition " +
          s"directory $path prior to writing to it")
      }
    }
  }

  /**
   * Given a set of input partitions, returns those that have locations that differ from the
   * Hive default (e.g. /k1=v1/k2=v2). These partitions were manually assigned locations by
   * the user.
   *
   * @return a mapping from partition specs to their custom locations
   */
  private def getCustomPartitionLocations(
                                           fs: FileSystem,
                                           table: CatalogTable,
                                           qualifiedOutputPath: Path,
                                           partitions: Seq[CatalogTablePartition]): Map[TablePartitionSpec, String] = {
    partitions.flatMap { p =>
      val defaultLocation = qualifiedOutputPath.suffix(
        "/" + PartitioningUtils.getPathFragment(p.spec, table.partitionSchema)).toString
      val catalogLocation = new Path(p.location).makeQualified(
        fs.getUri, fs.getWorkingDirectory).toString
      if (catalogLocation != defaultLocation) {
        Some(p.spec -> catalogLocation)
      } else {
        None
      }
    }.toMap
  }
}
