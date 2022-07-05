package com.zto.fire.spark.connector

import com.zto.fire._
import com.zto.fire.common.util.{Logging, ReflectionUtils, ThreadUtils}
import com.zto.fire.spark.bean.GenerateBean
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.reflect.{ClassTag, classTag}

/**
 * JavaBean动态生成Receiver，用于随机生成JavaBean实例
 * JavaBean必须是GenerateBean的子类，并且必须实现generate方法
 *
 * @param delay 数据生成的间隔时间（ms）
 * @author ChengLong 2022-03-07 14:30:55
 * @since 2.2.1
 */
class BeanGenReceiver[T <: GenerateBean[T] : ClassTag](delay: Long = 1000) extends Receiver[T](StorageLevel.MEMORY_AND_DISK_SER) with Logging {
  private lazy val generate = "generate"

  /**
   * 生命周期方法，开始receive
   */
  override def onStart(): Unit = {
    this.logInfo("开始启动BeanGenReceiver")
    ThreadUtils.runAsSingle(receive())
  }

  /**
   * 接受T类型自动生成的JavaBean对象实例
   */
  def receive(): Unit = {
    val clazz = classTag[T].runtimeClass
    ReflectionUtils.getMethodByName(clazz, generate)
    val method = clazz.getDeclaredMethod(generate)
    val emptyInstance = clazz.newInstance()

    while (true) {
      this.store(method.invoke(emptyInstance).asInstanceOf[JList[T]].toIterator)
      Thread.sleep(delay)
    }
  }

  override def onStop(): Unit = {
    this.logWarning("停止BeanGenReceiver.")
  }
}