package com.zto.fire.flink.util

import java.io.{BufferedInputStream, DataInputStream, File, FileInputStream}
import java.net.URI
import java.util.Date
import java.util.regex.{Matcher, Pattern}
import com.zto.fire._
import com.zto.fire.common.anno.Internal
import com.zto.fire.common.util.UnitFormatUtils.DateUnitEnum
import com.zto.fire.common.util.{DateFormatUtils, Logging, UnitFormatUtils}
import com.zto.fire.flink.conf.FireFlinkConf
import org.apache.commons.lang3.time.DateUtils
import org.apache.flink.runtime.checkpoint.{Checkpoints, OperatorSubtaskState}
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle
import org.apache.flink.runtime.state.filesystem.FileStateHandle
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.ListBuffer

/**
 * flink历史失效状态清理工具
 * 清理策略：
 * conservativeModel：筛选出不再使用的checkpoint文件，将这些文件归档至指定的目录中，并定期删除指定时间的数据
 * 直接删除模式：直接删除不再需要的checkpoint文件
 *
 * @author ChengLong 2021-9-6 15:06:21
 */
protected[fire] class StateCleanerUtils extends Logging {
	Logger.getLogger(this.getClass).setLevel(Level.toLevel("info"))

	// ------------------------------ hdfs 选项 ----------------------------------- //
	protected val hdfs = FireFlinkConf.stateHdfsUrl
	protected val hdfsUser = "hadoop"

	// ------------------------------ checkpoint 选项 ------------------------------ //
	protected val checkpointDir = "/user/flink/checkpoint"
	protected val localCheckpointBaseDir = "./home/checkpoint"
	protected val archiveDir = "/user/flink/archive"
	// 用于存放当前线上flink任务需要使用到的状态绝对路径
	protected val inuserSet = new JHashSet[String]()
	// download到本地的metadata文件是否采用覆盖的方式避免本地磁盘存放过多的文件
	protected val overwrite = true
	// 是否将失效的状态文件移到到回收站，等待后续清理
	protected val conservativeModel = true
	// 用于存放遍历的checkpoint文件，避免二次遍历导致漏分析的文件被标记为删除
	protected val files = ListBuffer[LocatedFileStatus]()
	// checkpoint元数据的过期时间，AccessTime超过该时间的将会被清理
	protected val checkpointTTL = 62
	// 计算出checkpointTtl对应的unix时间戳
	protected val checkpointTTLStamp = DateUtils.addDays(new Date, -this.checkpointTTL).getTime
	// 是否删除空文件夹
	protected val deleteEmptyDirEnabled = true
	// ture表示使用访问时间，false表示使用修改时间
	protected val useAccessTime = false

	// ------------------------------ checkpoint归档选项 ---------------------------- //
	// 默认清理多少天之前的归档checkpoint文件
	protected val archiveTTL = 7
	protected val archiveTTLStamp = DateUtils.addDays(new Date, -this.archiveTTL).getTime
	// 用于指定是否删除过期的checkpoint归档文件
	protected val deleteArchiveEnabled = true

	// ------------------------------ savepoint 选项 ------------------------------- //
	protected val savepointDir = "/user/flink/savepoint"
	// savepoint的ttl时间
	protected val savepointTTL = 10
	protected val savepointTTLStamp = DateUtils.addDays(new Date, -this.savepointTTL).getTime
	// 用于指定是否清理过期savepoint
	protected val deleteSavepointEnabled = true

	// ------------------------------ savepoint 选项 ------------------------------- //
	protected val completedDir = "/user/flink/completed-jobs"
	protected val completedTTL = 31
	protected val completedTTLStamp = DateUtils.addDays(new Date, -this.completedTTL).getTime
	protected val deleteCompleteJobEnable = true

	// 指定checkpoint与savepoint的路径
	protected val checkpoint_pattern = Pattern.compile("/user/flink/checkpoint/") //指定任务id
	protected val savepoint_pattern = Pattern.compile("/user/flink/savepoint/") //指定任务id

	/**
	 * 获取HDFS的FileSystem对象
	 */
	@Internal
	protected def getFileSystem: FileSystem = {
		val fs = FileSystem.get(new URI(this.hdfs), new Configuration(), this.hdfsUser)
		fs.setWorkingDirectory(new Path("/"))
		fs
	}

	/**
	 * 解析 operatorSubtaskState 的 ManagedKeyedState
	 *
	 * @param operatorSubtaskState operatorSubtaskState
	 */
	@Internal
	protected def parseManagedKeyedState(operatorSubtaskState: OperatorSubtaskState): Unit = {
		if (noEmpty(operatorSubtaskState)) {
			// 本案例针对 Flink RocksDB 的增量 Checkpoint 引发的问题，
			// 因此仅处理 IncrementalRemoteKeyedStateHandle
			operatorSubtaskState.getManagedKeyedState.filter(_.isInstanceOf[IncrementalRemoteKeyedStateHandle])
				.map(_.asInstanceOf[IncrementalRemoteKeyedStateHandle]).foreach(keyedStateHandle => {
				// 获取 RocksDB 的 sharedState
				val sharedState = keyedStateHandle.getSharedState
				if (noEmpty(sharedState)) {
					sharedState.map(t => t._2).filter(_.isInstanceOf[FileStateHandle]).map(_.asInstanceOf[FileStateHandle])
						.foreach(t => {
							val filePath = t.getFilePath
							this.logger.info("parseManagedKeyedState:" + filePath)
							this.inuserSet.add(filePath.getPath)
						})
				}
			})
		}
	}

	/**
	 * 解析 operatorSubtaskState 的 ManagedOperatorState
	 *
	 * @param operatorSubtaskState operatorSubtaskState
	 */
	@Internal
	protected def parseManagedOperatorState(operatorSubtaskState: OperatorSubtaskState): Unit = {
		if (isEmpty(operatorSubtaskState)) {
			operatorSubtaskState.getManagedOperatorState.map(_.getDelegateStateHandle).filter(_.isInstanceOf[FileStateHandle]).map(_.asInstanceOf[FileStateHandle]).foreach(fileStateHandle => {
				val filePath = fileStateHandle.getFilePath
				this.logger.info("parseManagedKeyedState:" + filePath)
				this.inuserSet.add(filePath.getPath)
			})
		}
	}

	/**
	 * 递归遍历checkpoint目录下所有的_metadata文件
	 */
	@Internal
	protected def recursionCheckpointDir(): Unit = {
		var count = 0
		var fs: FileSystem = null
		tryFinally {
			fs = this.getFileSystem
			val path = new Path(this.checkpointDir)
			//增加checkpoint路径的正则匹配
			val it = fs.listFiles(path, true)
			while (it.hasNext) {
				val status = it.next()
				val matcher: Matcher = checkpoint_pattern.matcher(status.getPath().toUri.getPath + "/")
				if (matcher.find) {
					this.files += status
					this.logger.info(status.getPath().toUri.getPath)
					val timeFlag = if (this.useAccessTime) status.getAccessTime else status.getModificationTime
					// 只分析最近访问时间在配置的metadataTtl之后的metadata文件，也就是说默认62天之前仍未被访问或修改的metadata文件将会被删除
					if (status.getPath.getName.endsWith("_metadata") && (timeFlag > this.checkpointTTLStamp)) {
						// 获取metadata在hdfs上的相对路径
						val metadataPath = status.getPath.toString.replace(this.hdfs, "")
						this.inuserSet.add(metadataPath)
						this.logger.info(s"开始分析metadata文件：${metadataPath}")

						// 是否复用同一个本地元数据的路径，如果复用，则分析完成后就会被下一个元数据文件覆盖，否则会保留所有的metadata文件
						val localPath = if (this.overwrite) this.localCheckpointBaseDir + "/_metadata" else this.localCheckpointBaseDir + metadataPath
						// 将metadata文件拷贝到本地进行分析
						fs.copyToLocalFile(status.getPath, new Path(localPath))
						this.analyzeMetadata(localPath, status.getPath.getParent.toString)
						count += 1
					}
				}
			}
			this.logger.info(s"此次分析metadata文件数共计：${count}")
			this.logger.info(s"此次inuserSet文件数共计：${inuserSet.size()}")
		}(if (fs != null) fs.close())(this.logger, catchLog = "分析metadata文件发生异常", finallyCatchLog = "FileSystem.close()失败")
	}

	/**
	 * 清理不再被使用的状态数据
	 */
	protected def cleanCheckpoint(): Unit = {
		var count = 0
		var blockSize = 0L
		var fs: FileSystem = null

		tryFinally {
			fs = this.getFileSystem
			val newFilePath = new Path(s"${this.archiveDir}/${DateFormatUtils.formatCurrentDate()}")
			fs.mkdirs(newFilePath)
			this.files.foreach(status => {
				val currentFile = status.getPath.toString.replace(this.hdfs, "")
				if (!this.inuserSet.contains(currentFile)) {
					if (this.conservativeModel) {
						// 保守模式下仅将过期的状态文件移动至指定的文件夹中，等待后续的单独处理
						val subPath = status.getPath.getParent.toString.replace(this.hdfs, "").replace(this.checkpointDir + "/", "")
						val destPath = new Path(s"${this.archiveDir}/${DateFormatUtils.formatCurrentDate()}/$subPath")
						fs.mkdirs(destPath)
						fs.rename(status.getPath, destPath)
						this.logger.info(s"移动状态文件：${status.getPath.toString} to ${destPath.toString}")
					} else {
						// 非保守模式下，直接删除失效的状态文件
						fs.delete(status.getPath, true)
						this.logger.info(s"删除状态文件：${status.getPath}")
					}
					count += 1
					blockSize += status.getBlockSize
				}
			})

			this.logger.info(s"清理过期文件数：${count}，释放磁盘空间：${UnitFormatUtils.readable(blockSize, DateUnitEnum.BYTE)}")
		}(if (fs != null) fs.close())(this.logger, catchLog = "删除/归档checkpoint文件过程中发生异常", finallyCatchLog = "FileSystem.close()失败")
	}

	/**
	 * 通过解析指定的_metadata分析还在被使用的checkpoint文件
	 *
	 * externalPointer 设置为 当前解析_metadata 的父目录即可
	 * 解决 状态反序列化中 type为 RELATIVE_STREAM_STATE_HANDLE 导致报错
	 * Cannot deserialize a RelativeFileStateHandle without a context to make it relative to
	 *
	 * @param path
	 * metadata的绝对路径
	 */
	@Internal
	protected def analyzeMetadata(path: String, externalPointer: String): Unit = {
		//  读取元数据文件
		val metadataFile = new File(path)
		var fis: FileInputStream = null
		var bis: BufferedInputStream = null
		var dis: DataInputStream = null

		tryFinally {
			// 通过IO流获取本地的metadata文件
			fis = new FileInputStream(metadataFile)
			bis = new BufferedInputStream(fis)
			dis = new DataInputStream(bis)

			val checkpointMetadata = Checkpoints.loadCheckpointMetadata(dis, this.getClass.getClassLoader, externalPointer)

			// 遍历 OperatorState，这里的每个 OperatorState 对应一个 Flink 任务的 Operator 算子
			// 不要与 OperatorState  和 KeyedState 混淆，不是一个层级的概念
			checkpointMetadata.getOperatorStates.filter(_.getStateSize > 0).foreach(operatorState => {
				this.logger.debug(s"算子状态：${operatorState}")
				// 遍历当前算子的所有 subtask
				operatorState.getStates.foreach(operatorSubtaskState => {
					// 解析 operatorSubtaskState 的 ManagedKeyedState
					this.parseManagedKeyedState(operatorSubtaskState)
					// 解析 operatorSubtaskState 的 ManagedOperatorState
					this.parseManagedOperatorState(operatorSubtaskState)
				})
			})
		}(if (dis != null) dis.close())(this.logger, catchLog = "解析metadata文件过程中出现异常", finallyCatchLog = "关闭IO流过程中出现异常")
	}

	/**
	 * 删除过期的归档文件
	 */
	protected def deleteArchive(): Unit = {
		if (!this.deleteArchiveEnabled || !this.conservativeModel) return

		var fs: FileSystem = null
		var count = 0
		tryFinally {
			fs = this.getFileSystem
			val path = new Path(this.archiveDir)
			val files = fs.listStatus(path)
			files.filter(file => file.isDirectory).foreach(file => {
				val timeFlag = if (this.useAccessTime) file.getAccessTime else file.getModificationTime
				// 清理checkpoint归档目录
				if (timeFlag < this.archiveTTLStamp) {
					fs.delete(file.getPath, true)
					count += 1
					this.logger.info(s"清理checkpoint归档目录成功：${file.getPath}，归档时间：${DateFormatUtils.formatDateTime(new Date(timeFlag))}")
				}
			})
			this.logger.info(s"本次清理checkpoint归档目录共计：${count}个")
		}(if (fs != null) fs.close())(this.logger, catchLog = "清理checkpoint归档目录出现异常", finallyCatchLog = "FileSystem.close()失败")
	}

	/**
	 * 删除空文件夹，文件夹总大小为0的目录会被清空
	 */
	protected def deleteEmptyDir(): Unit = {
		if (!this.deleteEmptyDirEnabled) return

		var fs: FileSystem = null
		var count = 0
		tryFinally {
			fs = this.getFileSystem
			val path = new Path(this.checkpointDir)
			//获取指定目录
			val files = fs.listStatus(path, new PathFilter {
				override def accept(p: Path): Boolean = {
					val matcher: Matcher = checkpoint_pattern.matcher(p.toUri.getPath + "/")
					matcher.find()
				}
			})
			files.filter(file => file.isDirectory).foreach(file => {
				val checkpointList: Array[FileStatus] = fs.listStatus(file.getPath)
				checkpointList.foreach(file => {
					val size = fs.getContentSummary(file.getPath).getLength
					if (size == 0) {
						fs.delete(file.getPath, true)
						count += 1
						this.logger.info(s"清理空文件夹：${file.getPath}，空文件时间：${DateFormatUtils.formatDateTime(new Date(file.getAccessTime))}")
					}
				})
			})
			this.logger.info(s"本次清理空文件夹共计：${count}个")
		}(if (fs != null) fs.close())(this.logger, catchLog = "清理空文件过程中出现异常", finallyCatchLog = "FileSystem.close()失败")
	}

	/**
	 * 定期清理过期的savepoint文件
	 */
	protected def deleteSavepoint(): Unit

	= {
		if (!this.deleteSavepointEnabled) return
		var fs: FileSystem = null
		var count = 0
		tryFinally {
			fs = this.getFileSystem
			val path = new Path(this.savepointDir)
			//获取满足指定目录的 flink savepoint目录
			val files = fs.listStatus(path, new PathFilter {
				override def accept(p: Path): Boolean = {
					val matcher: Matcher = savepoint_pattern.matcher(p.toUri.getPath + "/")
					matcher.find()
				}
			})
			files.filter(file => file.isDirectory).foreach(file => {
				val savepointList: Array[FileStatus] = fs.listStatus(file.getPath)
				savepointList.foreach(file => {
					val timeFlag = if (this.useAccessTime) file.getAccessTime else file.getModificationTime
					if (timeFlag < this.savepointTTLStamp) {

						//TODO 考虑是否可以改为删除至回收站
						//						val t = new Trash(fs.getConf)
						//						t.moveToTrash(file.getPath)

						fs.delete(file.getPath, true)
						count += 1
						this.logger.info(s"清理savepoint目录成功：${file.getPath}，savepoint时间：${DateFormatUtils.formatDateTime(new Date(timeFlag))}")
					}
				})
			})
			this.logger.info(s"本次清理savepoint共计：${count}个")
		}(if (fs != null) fs.close())(this.logger, catchLog = "清理savepoint文件过程中出现异常", finallyCatchLog = "FileSystem.close()失败")
	}

	/**
	 * 定期清理过期的complete job文件
	 */
	protected def deleteCompleteJobs(): Unit

	= {
		if (!this.deleteCompleteJobEnable) return
		var fs: FileSystem = null
		var count = 0
		tryFinally {
			fs = this.getFileSystem
			val path = new Path(this.completedDir)
			val files = fs.listStatus(path)
			files.foreach(file => {
				val timeFlag = if (this.useAccessTime) file.getAccessTime else file.getModificationTime
				if (timeFlag < this.completedTTLStamp) {
					fs.delete(file.getPath, true)
					count += 1
					this.logger.info(s"清理completed job目录成功：${file.getPath}，completed job时间：${DateFormatUtils.formatDateTime(new Date(timeFlag))}")
				}
			})
			this.logger.info(s"本次清理completed job共计：${count}个")
		}(if (fs != null) fs.close())(this.logger, catchLog = "清理清理completed job文件过程中出现异常", finallyCatchLog = "FileSystem.close()失败")
	}

	/**
	 * 执行清理任务
	 */
	protected def run(): Unit = {
		elapsed[Unit]("step 5. 清理完毕，执行结束", this.logger) {
			this.logger.info("开始执行新checkpoint与savepoint清理程序...")
			this.logger.warn(s"step 1. 开始解析${checkpointTTL}天内增量checkpoint metadata文件并分析直接的血缘关系.")
			this.recursionCheckpointDir()
			this.logger.warn("step 2. 开始归档历史的checkpoint文件.")
			this.cleanCheckpoint()
			this.logger.warn(s"step 3. 开始清理${archiveTTL}天前过期的checkpoint归档文件.")
			this.deleteArchive()
			this.logger.warn("step 3. 开始清理checkpoint空文件夹.")
			this.deleteEmptyDir()
			this.logger.warn(s"step 4. 开始清理${savepointTTL}天前过期的savepoint文件.")
			this.deleteSavepoint()
			this.logger.warn(s"step 5. 开始清理${completedTTL}天前过期的completed job文件.")
			this.deleteCompleteJobs()
		}
	}
}
