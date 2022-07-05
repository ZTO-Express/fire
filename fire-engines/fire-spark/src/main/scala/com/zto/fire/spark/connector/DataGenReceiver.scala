package com.zto.fire.spark.connector

import com.zto.fire.common.util.{Logging, ThreadUtils}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.mutable

/**
 * 数据动态生成Receiver，数据的生成逻辑使用generateFun函数来定义
 *
 * @param delay 数据生成的间隔时间（ms）
 * @param generateFun 数据生成的函数
 * @author ChengLong 2022-03-07 14:10:55
 * @since 2.2.1
 */
class DataGenReceiver[T](delay: Long = 1000, generateFun: => mutable.Buffer[T]) extends Receiver[T](StorageLevel.MEMORY_AND_DISK_SER) with Logging {

  /**
   * 生命周期方法，开始receive
   */
  override def onStart(): Unit = {
    this.logInfo("开始启动DataGenReceiver.")
    ThreadUtils.runAsSingle(receive())
  }

  /**
   * 接受T类型自动生成的JavaBean对象实例
   */
  def receive(): Unit = {
    while (true) {
      this.store(this.generateFun.toIterator)
      Thread.sleep(delay)
    }
  }

  override def onStop(): Unit = {
    this.logWarning("停止DataGenReceiver.")
  }
}