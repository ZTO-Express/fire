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

package com.zto.fire.common.util

import com.zto.fire.common.conf._
import com.zto.fire.predef._
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import java.io.{FileInputStream, InputStream}
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.Map
import scala.collection.{immutable, mutable}
import scala.reflect.ClassTag

/**
 * 读取配置文件工具类
 * Created by ChengLong on 2016-11-22.
 */
object PropUtils {
  private val props = new Properties()
  private val configurationFiles = Array[String]("fire", "cluster", "spark", "flink")
  // 用于判断是否merge过
  private[fire] val isMerge = new AtomicBoolean(false)
  // 引擎类型判断，当前阶段仅支持spark与flink，未来若支持新的引擎，则需在此处做支持
  private[fire] val engine = if (this.isExists("spark")) "spark" else "flink"
  // 加载默认配置文件
  this.load(this.configurationFiles: _*)
  // 避免已被加载的配置文件被重复加载
  private[this] lazy val alreadyLoadMap = new mutable.HashMap[String, String]()
  // 用于存放所有的配置信息
  private[fire] lazy val settingsMap = new mutable.HashMap[String, String]()
  // 用于存放固定前缀，而后缀不同的配置信息
  private[this] lazy val cachedConfMap = new mutable.HashMap[String, collection.immutable.Map[String, String]]()
  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * 判断指定的配置文件是否存在
   *
   * @param fileName
   * 配置文件名称
   */
  def isExists(fileName: String): Boolean = {
    var resource: InputStream = null
    try {
      resource = this.getInputStream(fileName)
      if (resource == null) false else true
    } finally {
      if (resource != null) {
        IOUtils.close(resource)
      }
    }
  }

  /**
   * 获取完整的配置文件名称
   */
  private[this] def getFullName(fileName: String): String = if (fileName.endsWith(".properties")) fileName else s"$fileName.properties"

  /**
   * 获取指定配置文件的输入流
   * 注：此api调用者需主动关闭输入流
   *
   * @param fileName
   * 配置文件名称
   */
  private[this] def getInputStream(fileName: String): InputStream = {
    val fullName = this.getFullName(fileName)
    var resource: InputStream = null
    try {
      resource = FileUtils.resourceFileExists(fullName)
      if (resource == null) {
        val findFileName = FindClassUtils.findFileInJar(fullName)
        if (StringUtils.isNotBlank(findFileName)) {
          if (FindClassUtils.isJar) {
            resource = FileUtils.resourceFileExists(findFileName)
          } else {
            resource = new FileInputStream(findFileName)
          }
        }
      }
      resource
    }
  }

  /**
   * 加载指定配置文件，resources根目录下优先级最高，其次是按字典顺序的目录
   *
   * @param fileName
   * 配置文件名称
   */
  def loadFile(fileName: String): this.type = this.synchronized {
    val fullName = this.getFullName(fileName)
    if (StringUtils.isNotBlank(fullName) && !this.alreadyLoadMap.contains(fullName)) {
      var resource: InputStream = null
      try {
        resource = this.getInputStream(fullName)
        if (resource == null && !this.configurationFiles.contains(fileName)) this.logger.warn(s"未找到配置文件[ $fullName ]，请核实！")
        if (resource != null) {
          this.logger.warn(s"${FirePS1Conf.YELLOW} -------------> loaded ${fullName} <------------- ${FirePS1Conf.DEFAULT}")
          props.load(resource)
          // 将所有的配置信息存放到settings中，并统一添加key的引擎前缀，如：
          // 如果是spark引擎，则key前缀统一添加spark. 如果是flink引擎，则统一添加flink.
          props.foreach(prop => this.settingsMap.put(this.adaptiveKey(prop._1), prop._2))
          props.clear()
          this.alreadyLoadMap.put(fullName, fullName)
        }
      } finally {
        if (resource != null) {
          IOUtils.close(resource)
        }
      }
    }
    this
  }

  /**
   * 加载多个指定配置文件，resources根目录下优先级最高，其次是按字典顺序的目录
   *
   * @param fileNames
   * 配置文件名称
   */
  def load(fileNames: String*): this.type = {
    if (noEmpty(fileNames)) fileNames.foreach(this.loadFile)
    this
  }

  /**
   * 自适应key的前缀
   */
  private[this] def adaptiveKey(key: String): String = {
    if (!key.startsWith(s"${this.engine}.")) s"${this.engine}.$key" else key
  }

  /**
   * 根据key获取配置信息
   * 注：其他均需要通过该API进行配置的获取,禁止直接调用：props.getProperty
   *
   * @param key
   * 配置的key
   * @return
   * 配置的value
   */
  def getProperty(key: String): String = {
    if (this.isMerge.compareAndSet(false, true)) this.mergeEngineConf
    this.getOriginalProperty(this.adaptiveKey(key))
  }

  /**
   * 获取原生的配置信息
   */
  private[fire] def getOriginalProperty(key: String): String = this.settingsMap.getOrElse(key, "")

  /**
   * 将给定的配置中的值与计量单位拆分开
   *
   * @param value
   * 配置的值，形如：10.3min
   * @return
   * 拆分单位后的tuple，形如：(10.3, min)
   */
  def splitUnit(value: String): (String, String) = {
    val numericPrefix = RegularUtils.numericPrefix.findFirstIn(value)
    val unitSuffix = RegularUtils.unitSuffix.findFirstIn(value)
    if (numericPrefix.isEmpty || unitSuffix.isEmpty) throw new IllegalArgumentException("配置中不包含数值或计量单位，请检查配置")

    (numericPrefix.get.trim, unitSuffix.get.trim)
  }

  /**
   * 获取字符串
   */
  def getString(key: String): String = this.getProperty(key)

  /**
   * 获取字符串，为空则取默认值
   */
  def getString(key: String, default: String): String = {
    val value = this.getProperty(key)
    if (StringUtils.isNotBlank(value)) value else default
  }

  /**
   * 获取拼接后数值的配置字符串
   *
   * @param key    配置的前缀
   * @param keyNum 拼接到key后的数值后缀
   * @return
   * 对应的配置信息
   */
  def getString(key: String, default: String, keyNum: Int = 1): String = {
    if (keyNum <= 1) {
      var value = this.getProperty(key)
      if (StringUtils.isBlank(value)) {
        value = this.getString(key + "1", default)
      }
      value
    } else {
      this.getString(key + keyNum, default)
    }
  }

  /**
   * 获取拼接后数值的配置整数
   *
   * @param key    配置的前缀
   * @param keyNum 拼接到key后的数值后缀
   * @return
   * 对应的配置信息
   */
  def getInt(key: String, default: Int, keyNum: Int = 1): Int = {
    val value = this.getString(key, default + "", keyNum)
    if (StringUtils.isNotBlank(value)) value.toInt else default
  }


  /**
   * 获取拼接后数值的配置长整数
   *
   * @param key    配置的前缀
   * @param keyNum 拼接到key后的数值后缀
   * @return
   * 对应的配置信息
   */
  def getLong(key: String, default: Long, keyNum: Int = 1): Long = {
    this.get[Long](key, Some(default), keyNum)
  }

  /**
   * 获取float型数据
   */
  def getFloat(key: String, default: Float, keyNum: Int = 1): Float = {
    this.get[Float](key, Some(default), keyNum)
  }

  /**
   * 获取Double型数据
   */
  def getDouble(key: String, default: Double, keyNum: Int = 1): Double = {
    this.get[Double](key, Some(default), keyNum)
  }


  /**
   * 获取拼接后数值的配置布尔值
   *
   * @param key    配置的前缀
   * @param keyNum 拼接到key后的数值后缀
   * @return
   * 对应的配置信息
   */
  def getBoolean(key: String, default: Boolean, keyNum: Int = 1): Boolean = {
    this.get[Boolean](key, Some(default), keyNum)
  }

  /**
   * 根据指定的key与key的num，获取对应的配置信息
   * 1. 如果配置存在，则进行类型转换，返回T类型数据
   * 2. 如果配置不存在，则取default参数作为默认值返回
   *
   * @param key
   * 配置的key
   * @param default
   * 如果配置不存在，则取default只
   * @param keyNum
   * 配置key的后缀编号
   * @tparam T
   * 返回配置的类型
   * @return
   */
  def get[T: ClassTag](key: String, default: Option[T] = Option.empty, keyNum: Int = 1): T = {
    val value = this.getString(key, if (default.isDefined) default.get.toString else "", keyNum = keyNum)
    val paramType = getParamType[T]
    val property = tryWithReturn {
      paramType match {
        case _ if paramType eq classOf[Int] => value.toInt
        case _ if paramType eq classOf[Long] => value.toLong
        case _ if paramType eq classOf[Float] => value.toFloat
        case _ if paramType eq classOf[Double] => value.toDouble
        case _ if paramType eq classOf[Boolean] => value.toBoolean
        case _ => value
      }
    } (this.logger, catchLog = s"为找到配置信息：${key}，请检查！")
    property.asInstanceOf[T]
  }

  /**
   * 使用map设置多个值
   *
   * @param map
   * java map，存放多个配置信息
   */
  def setProperties(map: mutable.Map[String, String]): Unit = this.synchronized {
    if (map != null) map.foreach(kv => this.setProperty(kv._1, kv._2))
  }

  /**
   * 使用map设置多个值
   *
   * @param map
   * java map，存放多个配置信息
   */
  def setProperties(map: JMap[String, Object]): Unit = this.synchronized {
    if (map != null) {
      map.foreach(kv => {
        if (StringUtils.isNotBlank(kv._1) && kv._2 != null) {
          this.setProperty(kv._1, kv._2.toString)
        }
      })
    }
  }

  /**
   * 设置指定的配置
   * 注：其他均需要通过该API进行配置的设定,禁止直接调用：props.setProperty
   *
   * @param key
   * 配置的key
   * @param value
   * 配置的value
   */
  def setProperty(key: String, value: String): Unit = this.synchronized {
    if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
      this.setOriginalProperty(this.adaptiveKey(key), value)
    }
  }

  /**
   * 添加原生的配置信息
   */
  private[fire] def setOriginalProperty(key: String, value: String): Unit = this.synchronized(this.settingsMap.put(key, value))

  /**
   * 隐蔽密码信息后返回
   */
  def cover: Map[String, String] = this.settingsMap.filter(t => !t._1.contains("pass"))

  /**
   * 打印配置文件中的kv
   */
  def show(): Unit = {
    if (!FireFrameworkConf.fireConfShow) return
    LogUtils.logStyle(this.logger, "Fire configuration.")(logger => {
      this.settingsMap.foreach(key => {
        // 如果包含配置黑名单，则不打印
        if (key != null && !FireFrameworkConf.fireConfBlackList.exists(conf => key.toString.contains(conf))) {
          logger.info(s">>${FirePS1Conf.PINK} ${key._1} --> ${key._2} ${FirePS1Conf.DEFAULT}")
        }
      })
    })
  }

  /**
   * 将配置信息转为Map，并设置到SparkConf中
   *
   * @return
   * confMap
   */
  def settings: Map[String, String] = {
    val map = Map[String, String]()
    map.putAll(this.settingsMap)
    map
  }

  /**
   * 指定key的前缀获取所有该前缀的key与value
   */
  def sliceKeys(keyStart: String): immutable.Map[String, String] = {
    if (!this.cachedConfMap.contains(keyStart)) {
      val confMap = new mutable.HashMap[String, String]()
      this.settingsMap.foreach(key => {
        val adaptiveKeyStar = this.adaptiveKey(keyStart)
        if (key._1.contains(adaptiveKeyStar)) {
          val keySuffix = key._1.substring(adaptiveKeyStar.length)
          confMap.put(keySuffix, key._2)
        }
      })
      this.cachedConfMap.put(keyStart, confMap.toMap)
    }
    this.cachedConfMap(keyStart)
  }

  /**
   * 根据keyNum选择对应的kafka配置
   */
  def sliceKeysByNum(keyStart: String, keyNum: Int = 1): collection.immutable.Map[String, String] = {
    // 用于匹配以指定keyNum结尾的key
    val reg = "\\D" + keyNum + "$"
    val map = new mutable.HashMap[String, String]()
    this.sliceKeys(keyStart).foreach(kv => {
      val keyLength = kv._1.length
      val keyNumStr = keyNum.toString
      // 末尾匹配keyNum并且keyNum的前一位非整数
      val isMatch = reg.r.findFirstMatchIn(kv._1).isDefined
      // 提前key，如key=session.timeout.ms33，则提前后的key=session.timeout.ms
      val trimKey = if (isMatch) kv._1.substring(0, keyLength - keyNumStr.length) else kv._1

      // 配置的key的末尾与keyNum匹配
      if (isMatch) {
        map += (trimKey -> kv._2)
      } else if (keyNum <= 1) {
        // 匹配没有数字后缀的key，session.timeout.ms与session.timeout.ms1认为是同一个配置
        val lastChar = kv._1.substring(keyLength - 1, keyLength)
        // 如果配置的结尾是字母
        if (!StringsUtils.isInt(lastChar)) {
          map += (kv._1 -> kv._2)
        }
      }
    })
    map.toMap
  }

  /**
   * 合并Conf中的配置信息
   */
  private[this] def mergeEngineConf: Unit = {
    val clazz = Class.forName(FireFrameworkConf.FIRE_ENGINE_CONF_HELPER)
    val method = clazz.getDeclaredMethod("getEngineConf")
    val map = method.invoke(null).asInstanceOf[immutable.Map[String, String]]
    if (map.nonEmpty) {
      this.setProperties(map)
      logger.info(s"完成计算引擎配置信息的同步，总计：${map.size}条")
      map.foreach(k => logger.debug("合并：k=" + k._1 + " v=" + k._2))
    }
  }

  /**
   * 调用外部配置中心接口获取配合信息
   */
  def invokeConfigCenter(className: String): Unit = ConfigurationCenterManager.invokeConfigCenter(className)
}
