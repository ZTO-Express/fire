package com.zto.fire.flink.ext.function

import com.zto.fire._
import com.zto.fire.flink.conf.FireFlinkConf
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction, RichFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor, ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, State, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time

import java.io.File
import scala.reflect.ClassTag

/**
 * RichFunction api扩展，支持方便的获取状态数据
 *
 * @author ChengLong 2021-9-14 09:59:17
 * @since 2.2.0
 */
class RichFunctionExt(richFunction: RichFunction) {
  lazy val runtimeContext = richFunction.getRuntimeContext
  private[this] lazy val stateMap = new JConcurrentHashMap[String, State]()
  // 默认的状态TTL配置（flink.state.ttl.days）
  private[this] lazy val defaultTTLConfig = StateTtlConfig
    .newBuilder(Time.days(FireFlinkConf.flinkStateTTL))
    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build()

  /**
   * 根据name获取广播变量
   *
   * @param name 广播变量名称
   * @tparam T
   * 广播变量的类型
   * @return
   * 广播变量引用
   */
  def getBroadcastVariable[T](name: String): Seq[T] = {
    requireNonEmpty(name)("广播变量名称不能为空")
    this.runtimeContext.getBroadcastVariable[T](name)
  }

  /**
   * 将值添加到指定的累加器中
   *
   * @param name
   * 累加器名称
   * @param value
   * 待累加的值
   * @tparam T
   * 累加值的类型（Int/Long/Double）
   */
  def addCounter[T: ClassTag](name: String, value: T): Unit = {
    requireNonEmpty(name, value)
    getParamType[T] match {
      case valueType if valueType eq classOf[Int] => this.runtimeContext.getIntCounter(name).add(value.asInstanceOf[Int])
      case valueType if valueType eq classOf[Long] => this.runtimeContext.getLongCounter(name).add(value.asInstanceOf[Long])
      case valueType if valueType eq classOf[Double] => this.runtimeContext.getDoubleCounter(name).add(value.asInstanceOf[Double])
    }
  }


  /**
   * 根据文件名获取分布式缓存文件
   *
   * @param fileName 缓存文件名称
   * @return 被缓存的文件
   */
  def DistributedCache(fileName: String): File = {
    requireNonEmpty(fileName)("分布式缓存文件名称不能为空！")
    this.runtimeContext.getDistributedCache.getFile(fileName)
  }

  /**
   * 根据name获取ValueState
   */
  def getState[T: ClassTag](name: String, ttlConfig: StateTtlConfig = this.defaultTTLConfig): ValueState[T] = {
    this.stateMap.mergeGet(name) {
      val desc = new ValueStateDescriptor[T](name, getParamType[T])
      if (ttlConfig != null) desc.enableTimeToLive(ttlConfig)
      this.runtimeContext.getState[T](desc)
    }.asInstanceOf[ValueState[T]]
  }

  /**
   * 根据name获取ListState
   */
  def getListState[T: ClassTag](name: String, ttlConfig: StateTtlConfig = this.defaultTTLConfig): ListState[T] = {
    this.stateMap.mergeGet(name) {
      val desc = new ListStateDescriptor[T](name, getParamType[T])
      if (ttlConfig != null) desc.enableTimeToLive(ttlConfig)
      this.runtimeContext.getListState[T](desc)
    }.asInstanceOf[ListState[T]]
  }

  /**
   * 根据name获取MapState
   */
  def getMapState[K: ClassTag, V: ClassTag](name: String, ttlConfig: StateTtlConfig = this.defaultTTLConfig): MapState[K, V] = {
    this.stateMap.mergeGet(name) {
      val desc = new MapStateDescriptor[K, V](name, getParamType[K], getParamType[V])
      if (ttlConfig != null) desc.enableTimeToLive(ttlConfig)
      this.runtimeContext.getMapState[K, V](desc)
    }.asInstanceOf[MapState[K, V]]
  }

  /**
   * 根据name获取ReducingState
   */
  def getReducingState[T: ClassTag](name: String, reduceFun: (T, T) => T, ttlConfig: StateTtlConfig = this.defaultTTLConfig): ReducingState[T] = {
    this.stateMap.mergeGet(name) {
      val desc = new ReducingStateDescriptor[T](name, new ReduceFunction[T] {
        override def reduce(value1: T, value2: T): T = reduceFun(value1, value2)
      }, getParamType[T])
      if (ttlConfig != null) desc.enableTimeToLive(ttlConfig)
      this.runtimeContext.getReducingState[T](desc)
    }.asInstanceOf[ReducingState[T]]
  }

  /**
   * 根据name获取AggregatingState
   */
  def getAggregatingState[I, T: ClassTag, O](name: String, aggFunction: AggregateFunction[I, T, O], ttlConfig: StateTtlConfig = this.defaultTTLConfig): AggregatingState[I, O] = {
    this.stateMap.mergeGet(name) {
      val desc = new AggregatingStateDescriptor[I, T, O](name, aggFunction, getParamType[T])
      if (ttlConfig != null) desc.enableTimeToLive(ttlConfig)
      this.runtimeContext.getAggregatingState(desc)
    }.asInstanceOf[AggregatingState[I, O]]
  }
}
