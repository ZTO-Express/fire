package com.zto.fire.flink.ext.stream

import org.apache.flink.table.api.TableResult

/**
 * 用于对Flink TableResult的API库扩展
 *
 * @author ChengLong 2020年1月7日 09:18:21
 * @since 2.1.0
 */
class TableResultImplExt(tableResult: TableResult) {

  /**
   * 打印执行结果
   */
  def show(): Unit = this.tableResult.print()
}
