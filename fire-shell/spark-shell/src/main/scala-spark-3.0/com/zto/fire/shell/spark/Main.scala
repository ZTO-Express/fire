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

package com.zto.fire.shell.spark

import java.io.File
import java.net.URI
import scala.tools.nsc.GenericRunnerSettings
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.repl.Signaling
import org.apache.spark.sql.SparkSession

/**
 * java -Dscala.usejavacp=true -cp /Users/insight/project/workspace/fire/fire-repl/spark-repl/target/zto-spark-repl_2.12-2.2.0-SNAPSHOT.jar com.zto.fire.repl.spark.Main
 */
object Main extends Logging {
  System.setProperty("scala.usejavacp", "true")
  initializeLogIfNecessary(true)
  Signaling.cancelOnInterrupt()
  val utils = Class.forName("org.apache.spark.util.Utils")

  val conf = new SparkConf()
  val rootDir = conf.getOption("spark.repl.classdir").getOrElse(utils.getMethod("getLocalDir", classOf[SparkConf]).invoke(null, conf).asInstanceOf[String])
  val outputDir = utils.getMethod("createTempDir", classOf[String], classOf[String]).invoke(null, rootDir, "repl").asInstanceOf[File]

  var sparkContext: SparkContext = _
  var sparkSession: SparkSession = _
  // this is a public var because tests reset it.
  var interp: FireILoop = _

  private var hasErrors = false
  private var isShellSession = false

  private def scalaOptionError(msg: String): Unit = {
    hasErrors = true
    // scalastyle:off println
    Console.err.println(msg)
    // scalastyle:on println
  }

  def main(args: Array[String]): Unit = {
    isShellSession = true
    doMain(args, new FireILoop)
  }

  // Visible for testing
  private[shell] def doMain(args: Array[String], _interp: FireILoop): Unit = {
    interp = _interp

    val jars = utils.getMethod("getLocalUserJarsForShell", classOf[SparkConf]).invoke(null, conf).asInstanceOf[Seq[String]]
      // Remove file:///, file:// or file:/ scheme if exists for each jar
      .map { x => if (x.startsWith("file:")) new File(new URI(x)).getPath else x }
      .mkString(File.pathSeparator)
    val interpArguments = List(
      "-Yrepl-class-based",
      "-Yrepl-outdir", s"${outputDir.getAbsolutePath}",
      "-classpath", jars
    ) ++ args.toList

    val settings = new GenericRunnerSettings(scalaOptionError)
    settings.processArguments(interpArguments, true)

    if (!hasErrors) {
      interp.process(settings) // Repl starts and goes in loop of R.E.P.L
      Option(sparkContext).foreach(_.stop)
    }
  }
}
