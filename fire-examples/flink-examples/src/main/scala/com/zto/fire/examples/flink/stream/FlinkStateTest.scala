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

package com.zto.fire.examples.flink.stream

import com.zto.fire._
import com.zto.fire.flink.FlinkStreaming
import org.apache.flink.api.common.functions.{RichAggregateFunction, RichMapFunction}
import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._

/**
 * 用于演示基于FireMapFunction的状态使用
 * 本例演示KeyedStream相关的状态使用，也就是说stream是通过keyBy分组过的
 * 1. 由于经过keyBy算子进行了分组，因此相同key的算子都会跑到同一个subtask中执行，并行度的改变也就不会影响状态数据的一致性
 * 2. 状态在不同的task之间是隔离的，也就是说对同一个keyedStream进行多次map操作，每个map中的状态是不一样的，是隔离开来的
 *
 * @author ChengLong 2021年1月5日09:13:50
 * @since 2.0.0
 */
object FlinkStateTest extends FlinkStreaming {
  // 将dstream声明为成员变量时，一定要加lazy关键字，避免env还没初始化导致空指针异常
  lazy val dstream = this.fire.createCollectionStream(Seq((1, 1), (1, 2), (1, 3), (1, 6), (1, 9), (2, 1), (2, 2), (3, 1))).keyBy(0)

  /**
   * 一、基于FireMapFunction演示ValueState、ListState、MapState的使用
   */
  private def testSimpleState: Unit = {
    this.dstream.map(new RichMapFunction[(Int, Int), Int]() {
      // 定义状态的ttl时间，如果不在open方法中定义
      lazy val ttlConfig = StateTtlConfig.newBuilder(Time.days(1)).build()
      // 如果状态放到成员变量中声明，则需加lazy关键字
      lazy val listState = this.getListState[Int]("list_state")

      override def map(value: (Int, Int)): Int = {
        // FireMapFunction中提供的API，通过名称获取对应的状态实例，该API具有缓存的特性
        // 因此不需要放到open或声明为成员变量，每次直接通过this.getXxxState即可获取同一实例
        // 第一个参数是状态实例名称，不可重复，ttlConfig参数如果不指定，则默认不启用ttl，生产环境强烈建议开启
        // 1. ValueState与KeyedStream中的每个key是一一对应的
        val valueState = this.getState[Int]("value_state", ttlConfig)
        valueState.update(value._2 + valueState.value())
        logger.warn(s"key=${value._1} 状态结果：" + valueState.value())
        Thread.sleep(10000)

        // 2. 获取ListState，该状态的特点是KeyedStream中的每个key都单独对应一个List集合
        listState.add(value._2)
        listState.add(value._2 + 1)

        // 3. 获取mapState，该状态的特点是KeyedStream中的每个key都单独对应一个Map集合
        val mapState = this.getMapState[Int, Int]("map_state", ttlConfig)
        mapState.put(value._1, value._2)
        mapState.put(value._1 + 1, value._2)

        value._2
      }
    }).uname(uid = "simpleState", name = "状态累加")  // 通过uname进行uid与name的指定
  }

  /**
   * 二、基于FireMapFunction演示AggregatingState、getReducingState的使用
   */
  private def testFunctionState: Unit = {
    this.dstream.map(new RichMapFunction[(Int, Int), Int]() {
      // 1. ReducingState状态演示，将Int类型数据保存到状态中
      //    该ReduceFunction中定义的逻辑是将当前状态中的值与传入的新值进行累加，然后重新update到状态中
      //    方法的第二个参数是reduce的具体逻辑，本示例演示的是累加
      lazy val reduceState = this.getReducingState[Int]("reduce_state", (a: Int, b: Int) => a + b)

      // 2. AggregatingState状态使用，将Int类型数据保存到状态中
      //    需要创建AggregateFunction，泛型意义依次为：输入数据类型、累加器类型，聚合结果类型
      lazy val aggrState = this.getAggregatingState[(Int, Int), Int, Int]("aggr_state", this.newRichAggregateFunction)

      override def map(value: (Int, Int)): Int = {
        // 1. reduceState状态使用
        this.reduceState.add(value._2)
        logger.warn(s"reduceState当前结果：key=${value._1} state=${this.reduceState.get()}")

        // 2. AggregatingState状态使用
        this.aggrState.add(value)
        this.aggrState.get()
        logger.warn(s"aggrState当前结果：key=${value._1} state=${this.aggrState.get()}")

        value._2
      }

      /**
       * 创建一个RichAggregateFunction的子类
       * 在该子类中构建AggregateFunction对象，并定义好聚合的逻辑
       * 定义将输入数据与状态中的数据进行累加
       */
      def newRichAggregateFunction: RichAggregateFunction[(Int, Int), Int, Int] = {
        new RichAggregateFunction[(Int, Int), Int, Int]() {
          /** 迭代状态的初始值 */
          override def createAccumulator(): Int = 0

          /** 每一条输入数据，和迭代数据如何迭代 */
          override def add(value: (Int, Int), accumulator: Int): Int = value._2 + accumulator

          /** 返回数据，对最终的迭代数据如何处理，并返回结果 */
          override def getResult(accumulator: Int): Int = accumulator

          /** 多个分区的迭代数据如何合并 */
          override def merge(a: Int, b: Int): Int = a + b
        }
      }
    }).uname("testFunctionState")
  }

  /**
   * 三、演示mapWithState的使用
   */
  def testWithState: Unit = {
    // [String, Int]分表表示map后类型与状态的类型
    // 每个case中返回值中Some(xxx)中的xxx就是下一次同样key的数据进来以后状态获取到的数据
    // 也就是自动将Some(xxx)中的xxx数据update到ValueState中
    // 本例是将上一次的状态与当前进入的值进行累加，更新到状态中
    this.dstream.mapWithState[String, Int]({
      // 当第一次进入，状态中没有值时，给当前value
      case (value: (Int, Int), None) => {
        logger.warn(s"状态为空：当前key=${value._1} value=${value._2}")
        (value._1.toString, Some(value._2))
      }
      // 后续进入，状态中有值时，则累加当前进入的数据到状态中
      case (value: (Int, Int), state: Some[Int]) => {
        // 从state中get到的数据是上一次同一个key的sum值，因此通过state.get的值总是滞后于sum的
        val sum = value._2 + state.get
        logger.warn(s"当前key=${value._1} value=${value._2} state=${state.get} sum=$sum")
        (value._1.toString, Some(sum))
      }
    }).uid("flatMapWithState").name("计算状态")
  }

  /**
   * 业务逻辑处理，该方法会被fire自动调用，可避免main方法中代码过于臃肿
   */
  override def process: Unit = {
    this.fire.setParallelism(3)
    // 演示ValueState、ListState、MapState的使用
    this.testSimpleState
    // 演示AggregatingState、getReducingState的使用
    // this.testFunctionState
    // 演示mapWithState的使用
    // this.testWithState
  }
}
