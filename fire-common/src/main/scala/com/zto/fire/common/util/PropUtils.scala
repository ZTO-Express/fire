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

import com.zto.fire.common.anno.{Config, Internal}
import com.zto.fire.common.conf._
import com.zto.fire.common.enu.ConfigureLevel
import com.zto.fire.predef._
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import java.io.{FileInputStream, InputStream, StringReader}
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.Map
import scala.collection.{immutable, mutable}
import scala.reflect.{ClassTag, classTag}

/**
 * 读取配置文件工具类
 * Created by ChengLong on 2016-11-22.
 */
object PropUtils extends Logging {
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
  // 用于存放自适应引擎前缀的配置信息
  private[fire] lazy val adaptiveSettingsMap = new mutable.HashMap[String, String]()
  // 用于存放原始的配置信息
  private[fire] lazy val originalSettingsMap = new mutable.HashMap[String, String]()
  // 用于存放固定前缀，而后缀不同的配置信息
  private[this] lazy val cachedConfMap = new mutable.HashMap[String, collection.immutable.Map[String, String]]()

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
          props.foreach(prop => {
            this.adaptiveSettingsMap.put(this.adaptiveKey(prop._1), prop._2)
            this.originalSettingsMap.put(prop._1, prop._2)
          })
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
   * 加载扩展的注解配置信息：
   * @Kafka、@RocketMQ、@Hive、@HBase等
   *
   * @param clazz
   * 任务入口类
   */
  def loadAnnoConf(clazz: Class[_]): this.type = {
    if (!FireFrameworkConf.annoConfEnable) return this
    if (clazz == null) return this

    // 加载通过@Config注解配置的信息
    val option = this.getAnnoConfig(clazz)
    if (option.nonEmpty) {
      val (files, props, value) = option.get
      if (noEmpty(value)) {
        // 移除所有的注释信息
        val normalValue = RegularUtils.propAnnotation.replaceAllIn(value, "").replaceAll("\\|", "").trim
        val valueProps = new Properties()
        val stringReader = new StringReader(normalValue)
        valueProps.load(stringReader)
        stringReader.close()
        valueProps.map(kv => (StringUtils.trim(kv._1), StringUtils.trim(kv._2))).filter(kv => noEmpty(kv, kv._1, kv._2)).foreach(kv => this.setProperty(kv._1, kv._2))
      }
      props.foreach(kv => this.setProperty(kv._1, kv._2))
      if (noEmpty(files)) this.load(files: _*)
    }

    // 加载其他注解指定的配置信息
    val annoManagerClass = FireFrameworkConf.annoManagerClass
    if (isEmpty(annoManagerClass)) throw new IllegalArgumentException(s"未找到注解管理器，请通过：${FireFrameworkConf.FIRE_CONF_ANNO_MANAGER_CLASS}进行配置！")

    tryWithLog {
      val annoClazz = Class.forName(annoManagerClass)
      val method = ReflectionUtils.getMethodByName(annoClazz, "getAnnoProps")
      if (isEmpty(method)) throw new RuntimeException(s"未找到getAnnoProps()方法，通过${FireFrameworkConf.FIRE_CONF_ANNO_MANAGER_CLASS}指定的类必须是com.zto.fire.core.conf.AnnoManager的子类")
      val annoProps = method.invoke(annoClazz.newInstance(), clazz)
      this.setProperties(annoProps.asInstanceOf[mutable.HashMap[String, String]])
    } (this.logger, "成功加载注解中的配置信息！", "注解配置信息加载失败！")

    this
  }

  /**
   * 加载注解配置信息
   *
   * @param clazz
   * 任务入口类
   */
  def loadJobConf(clazz: Class[_]): this.type = {
    if (clazz == null) return this
    this.load(clazz.getSimpleName.replace("$", ""))
    this
  }

  /**
   * 获取配置中心配置信息并加载用户配置以及注解配置
   * 配置的优先级：fire公共配置 < 配置中心公共配置 < 用户任务配置 < 配置中心任务级别配置 < 配置中心紧急配置
   *
   * @param className
   * 入口类的包名+类名
   */
  def loadJobConf(className: String): this.type = {
    // 通过接口调用获取配置中心配置各等级的参数信息
    val centerConfig = this.invokeConfigCenter(className)
    // 配置中心的默认配置优先级高于框架（fire.properties）以及引擎（spark.properties/flink.properties）等配置
    this.setProperties(centerConfig.getOrDefault(ConfigureLevel.FRAMEWORK, Map.empty[String, String]))
    // 加载扩展类注解配置（@Kafka、@RocketMQ、@Hive、@HBase等）
    this.loadAnnoConf(Class.forName(className))
    // 加载用户配置文件以及@Config注解配置
    this.loadJobConf(Class.forName(className))
    // 配置中心任务级别配置优先级高于用户本地配置文件中的配置，做到重启任务即可生效
    this.setProperties(centerConfig.getOrDefault(ConfigureLevel.TASK, Map.empty[String, String]))
    // 配置中心紧急配置优先级最高，用于对所有任务生效的紧急参数调优
    this.setProperties(centerConfig.getOrDefault(ConfigureLevel.URGENT, Map.empty[String, String]))

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
  private[fire] def getOriginalProperty(key: String): String = this.adaptiveSettingsMap.getOrElse(key, "")

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
    }(this.logger, catchLog = s"为找到配置信息：${key}，请检查！")
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
      this.originalSettingsMap.put(key, value)
    }
  }

  /**
   * 添加原生的配置信息
   */
  private[fire] def setOriginalProperty(key: String, value: String): Unit = this.synchronized(this.adaptiveSettingsMap.put(key, value))

  /**
   * 隐蔽密码信息后返回
   */
  def cover: Map[String, String] = this.adaptiveSettingsMap.filter(t => !t._1.contains("pass"))

  /**
   * 打印配置文件中的kv
   */
  def show(): Unit = {
    if (!FireFrameworkConf.fireConfShow) return
    LogUtils.logStyle(this.logger, "Fire configuration.")(logger => {
      this.adaptiveSettingsMap.foreach(key => {
        // 如果包含配置黑名单，则不打印
        if (key != null && !FireFrameworkConf.fireConfBlackList.exists(conf => key.toString.contains(conf))) {
          logger.info(s">>${FirePS1Conf.PINK} ${key._1} --> ${key._2} ${FirePS1Conf.DEFAULT}")
        }
      })
    })
  }

  /**
   * 获所有的配置信息（包含经过自适应处理的配置）
   *
   * @return
   * confMap
   */
  def settings: Map[String, String] = {
    val map = Map[String, String]()
    map ++= this.originalSettingsMap
    map ++= this.adaptiveSettingsMap
    map
  }

  /**
   * 获取经过适配前缀的配置信息
   *
   * @return
   * confMap
   */
  def adaptiveSettings: Map[String, String] = {
    val map = Map[String, String]()
    map ++= this.adaptiveSettingsMap
    map
  }

  /**
   * 获取原始的配置信息
   *
   * @return
   * confMap
   */
  def originalSettings: Map[String, String] = {
    val map = Map[String, String]()
    map ++= this.originalSettingsMap
    map
  }

  /**
   * 指定key的前缀获取所有该前缀的key与value
   */
  def sliceKeys(keyStart: String): immutable.Map[String, String] = {
    if (!this.cachedConfMap.contains(keyStart)) {
      val confMap = new mutable.HashMap[String, String]()
      this.adaptiveSettingsMap.foreach(key => {
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
  @Internal
  private[this] def mergeEngineConf: Unit = {
    val clazz = Class.forName(FireFrameworkConf.FIRE_ENGINE_CONF_HELPER)
    val method = clazz.getDeclaredMethod("syncEngineConf")
    val map = method.invoke(null).asInstanceOf[immutable.Map[String, String]]
    if (map.nonEmpty) {
      this.setProperties(map.filter(kv => !kv._1.contains(FireFrameworkConf.FIRE_REST_SERVER_SECRET)))
      logger.info(s"完成计算引擎配置信息的同步，总计：${map.size}条")
      map.foreach(k => logger.debug("合并：k=" + k._1 + " v=" + k._2))
    }
  }

  /**
   * 获取指定类的配置注解信息
   *
   * @param clazz
   * flink或spark任务的具体入口类
   * @return
   * 配置文件名称 & 配置列表
   */
  @Internal
  private[this] def getAnnoConfig(clazz: Class[_]): Option[(Array[String], Array[(String, String)], String)] = {
    val anno = ReflectionUtils.getClassAnnotation(clazz, classOf[Config])
    if (anno == null) return None
    val confAnno = anno.asInstanceOf[Config]
    val files = confAnno.files().filter(StringUtils.isNotBlank).map(_.trim)
    val props = confAnno.props().filter(StringUtils.isNotBlank)
      .map(_.split("=", 2))
      .filter(prop => noEmpty(prop) && prop.length == 2 && noEmpty(prop(0), prop(1)))
      .map(prop => {
        (prop(0).trim, prop(1).trim)
      })
    Some(files, props, confAnno.value())
  }

  /**
   * 调用外部配置中心接口获取配合信息
   */
  @Internal
  private[this] def invokeConfigCenter(className: String): JMap[ConfigureLevel, JMap[String, String]] = {
    // 调用配置中心接口获取优先级最高的配置信息
    ConfigurationCenterManager.invokeConfigCenter(className)
  }
}
