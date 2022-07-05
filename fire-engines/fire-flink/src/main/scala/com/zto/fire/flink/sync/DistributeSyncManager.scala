package com.zto.fire.flink.sync

import com.zto.fire.common.util.{JSONUtils, PropUtils, ThreadUtils}
import com.zto.fire.core.bean.ArthasParam
import com.zto.fire.core.plugin.ArthasDynamicLauncher
import com.zto.fire.core.rest.SystemRestful
import com.zto.fire.core.sync.SyncManager
import com.zto.fire.flink.bean.DistributeBean
import com.zto.fire.flink.conf.FireFlinkConf
import com.zto.fire.flink.enu.DistributeModule
import com.zto.fire.predef._

import java.util.concurrent.TimeUnit

/**
 * Flink分布式数据同步管理器，用于将数据从JobManager端同步至每一个TaskManager端
 *
 * @author ChengLong 2021-11-9 13:21:39
 * @since 2.2.0
 */
private[fire] object DistributeSyncManager extends SyncManager {
  private var lastJsonConf = ""
  private lazy val distributeSyncUrl = "/system/distributeSync"

  /**
   * 准实时同步最新配置信息
   */
  def sync: Unit = {
    ThreadUtils.scheduleWithFixedDelay({
      if (!FireFlinkConf.distributeSyncEnabled) return
      val jsonConf = SystemRestful.restInvoke(this.distributeSyncUrl)
      if (!this.lastJsonConf.equals(jsonConf)) {
        if (JSONUtils.isJson(jsonConf)) {
          val distribute = JSONUtils.parseObject[DistributeBean](jsonConf)
          distribute.getModule match {
            case DistributeModule.CONF => this.syncConf(distribute.getJson)
            case DistributeModule.ARTHAS => ArthasDynamicLauncher.command(JSONUtils.parseObject[ArthasParam](distribute.getJson))
          }
        }
        this.lastJsonConf = jsonConf
      }
    }, 60, 30, TimeUnit.SECONDS)
  }

  /**
   * 更新配置信息
   */
  def syncConf(json: String): Unit = {
    if (noEmpty(json)) {
      val confMap = JSONUtils.parseObject[JMap[String, String]](json)
      PropUtils.setProperties(confMap)
      this.logger.info(s"本次分布式更新配置数：${confMap.size()}个")
    }
  }
}

