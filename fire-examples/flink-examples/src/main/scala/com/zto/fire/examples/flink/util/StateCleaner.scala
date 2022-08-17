package com.zto.fire.examples.flink.util

import com.zto.fire.flink.conf.FireFlinkConf
import com.zto.fire.flink.util.StateCleanerUtils
import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.fs.LocatedFileStatus
import com.zto.fire._

import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * flink历史失效状态清理工具
 * 清理策略：
 * conservativeModel：筛选出不再使用的checkpoint文件，将这些文件归档至指定的目录中，并定期删除指定时间的数据
 * 直接删除模式：直接删除不再需要的checkpoint文件
 *
 * @author ChengLong 2021-9-6 15:06:21
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
object StateCleaner extends StateCleanerUtils {
  // ------------------------------ hdfs 选项 ----------------------------------- //
  override protected val hdfs = FireFlinkConf.stateHdfsUrl
  override protected val hdfsUser = "hadoop"

  // ------------------------------ checkpoint 选项 ------------------------------ //
  override protected val checkpointDir = "/user/flink/checkpoint"
  override protected val localCheckpointBaseDir = "./home/checkpoint"
  override protected val archiveDir = "/user/flink/archive"
  // 用于存放当前线上flink任务需要使用到的状态绝对路径
  override protected val inuserSet = new JHashSet[String]()
  // download到本地的metadata文件是否采用覆盖的方式避免本地磁盘存放过多的文件
  override protected val overwrite = true
  // 是否将失效的状态文件移到到回收站，等待后续清理
  override protected val conservativeModel = true
  // 用于存放遍历的checkpoint文件，避免二次遍历导致漏分析的文件被标记为删除
  override protected val files = ListBuffer[LocatedFileStatus]()
  // checkpoint元数据的过期时间，AccessTime超过该时间的将会被清理
  override protected val checkpointTTL = 60
  // 计算出checkpointTtl对应的unix时间戳
  override protected val checkpointTTLStamp = DateUtils.addDays(new Date, -this.checkpointTTL).getTime
  // 是否删除空文件夹
  override protected val deleteEmptyDirEnabled = true
  // ture表示使用访问时间，false表示使用修改时间
  override protected val useAccessTime = false

  // ------------------------------ checkpoint归档选项 ---------------------------- //
  // 默认清理多少天之前的归档checkpoint文件
  override protected val archiveTTL = 30
  override protected val archiveTTLStamp = DateUtils.addDays(new Date, -this.archiveTTL).getTime
  // 用于指定是否删除过期的checkpoint归档文件
  override protected val deleteArchiveEnabled = true

  // ------------------------------ savepoint 选项 ------------------------------- //
  override protected val savepointDir = "/user/flink/savepoint"
  // savepoint的ttl时间
  override protected val savepointTTL = 10
  override protected val savepointTTLStamp = DateUtils.addDays(new Date, -this.savepointTTL).getTime
  // 用于指定是否清理过期savepoint
  override protected val deleteSavepointEnabled = true

  // ------------------------------ savepoint 选项 ------------------------------- //
  override protected val completedDir = "/user/flink/completed-jobs"
  override protected val completedTTL = 31
  override protected val completedTTLStamp = DateUtils.addDays(new Date, -this.completedTTL).getTime
  override protected val deleteCompleteJobEnable = true

  def main(args: Array[String]): Unit = {
    this.run()
  }
}
