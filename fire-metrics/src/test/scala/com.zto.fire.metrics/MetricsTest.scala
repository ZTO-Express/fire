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

package com.zto.fire.metrics

import java.util.Random
import java.util.concurrent.TimeUnit
import com.codahale.metrics.jvm.{FileDescriptorRatioGauge, GarbageCollectorMetricSet, MemoryUsageGaugeSet, ThreadStatesGaugeSet}
import com.codahale.metrics.{ConsoleReporter, JmxReporter, MetricRegistry, Slf4jReporter}
import org.antlr.v4.runtime.tree.ParseTreeWalker
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.junit.Test

/**
 * Metrics模块测试
 * <a href='https://www.jianshu.com/p/e5bba03fd64f'>文档</a>
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-17 10:11
 */
class MetricsTest {
  val metrics = new MetricRegistry()

  @Test
  def testMeter: Unit = {
    val reporter = ConsoleReporter.forRegistry(metrics).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build
    reporter.start(1, TimeUnit.SECONDS)

    val requests = metrics.meter("requests")
    /*(1 to 100).foreach(i => {
      requests.mark()
      Thread.sleep(10)
    })
    Thread.sleep(1000)*/
  }

  @Test
  def testHistogram: Unit = {
    val jmxReporter = JmxReporter.forRegistry(metrics)
      .convertDurationsTo(TimeUnit.SECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      //.withLoggingLevel(Slf4jReporter.LoggingLevel.INFO)
      .build
    jmxReporter.start(/*3, TimeUnit.SECONDS*/)

    val consoleReporter = ConsoleReporter.forRegistry(metrics)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      //.withLoggingLevel(Slf4jReporter.LoggingLevel.INFO)
      .build
    consoleReporter.start(3, TimeUnit.SECONDS)

    val resultCounts = metrics.timer("result-counts")
    val resultCounts2 = metrics.meter("cost")
    val random = new Random()
    /*(1 to 10000).foreach(i => {
      // resultCounts.update(random.nextInt(100))
      val start = System.currentTimeMillis()
      Thread.sleep(random.nextInt(50))
      val end = System.currentTimeMillis() - start
      resultCounts.update(end, TimeUnit.MILLISECONDS)
      resultCounts2.mark()
    })
    Thread.sleep(10000)*/
  }

  @Test
  def testJvm: Unit = {
    val reporter2 = ConsoleReporter.forRegistry(metrics)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build
    reporter2.start(3, TimeUnit.SECONDS)
    val reporter = Slf4jReporter.forRegistry(metrics).convertDurationsTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).withLoggingLevel(Slf4jReporter.LoggingLevel.ERROR).build
    reporter.start(5, TimeUnit.SECONDS)

    metrics.register("jvm.gc", new GarbageCollectorMetricSet())
    metrics.register("jvm.memroy", new MemoryUsageGaugeSet())
    metrics.register("jvm.thread-states", new ThreadStatesGaugeSet())
    metrics.register("jvm.fd.usage", new FileDescriptorRatioGauge())

    // Thread.sleep(100000)
  }

  /*@Test
  def testAntlr: Unit = {
    val input = CharStreams.fromString(
      """
        |a=(1+2+3)*10/5
        |a
        |""".stripMargin)
    val lexer = new HelloLexer(input)
    val tokens = new CommonTokenStream(lexer)
    val parser = new HelloParser(tokens)
    val tree = parser.prog()
    val visitor = new HelloMyVisitor()
    visitor.visit(tree)
  }

  @Test
  def testArrayInit: Unit = {
    val input = CharStreams.fromString("{1,2,{3}}")
    val lexer = new ArrayInitLexer(input)
    val tokens = new CommonTokenStream(lexer)
    val parser = new ArrayInitParser(tokens)
    val tree = parser.init()
    val walker = new ParseTreeWalker
    walker.walk(new MyArrayInitListener(), tree)
    println()
  }*/

}
