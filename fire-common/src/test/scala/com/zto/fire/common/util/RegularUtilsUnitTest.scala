package com.zto.fire.common.util

import com.zto.fire.common.anno.TestStep
import org.junit.Test

import java.io.StringReader
import java.util.Properties

/**
 * 常用的正则表达式
 *
 * @author ChengLong 2022-05-12 17:20:55
 * @since fire 2.2.2
 */
class RegularUtilsUnitTest {

  @Test
  @TestStep(step = 1, desc = "@Config注解中的注释解析单元测试")
  def testPropAnnotation: Unit = {
    val conf =
      """
        |# hello world
        |   # hello world
        | # 注释
        |   # 注释
        |#fire framework
        |#  fire   framework
        |      #fire framework
        | hive.cluster=batch
        |kafka.brokers1 = test#$kafka
        |kafka.brokers2=test # 注释kafka
        |#kafka.brokers3=test # kafka
        |""".stripMargin
    val normalValue = RegularUtils.propAnnotation.replaceAllIn(conf, "").replaceAll("\\|", "").trim
    println(normalValue)
    val valueProps = new Properties()
    val stringReader = new StringReader(normalValue)
    valueProps.load(stringReader)
    stringReader.close()
    assert(valueProps.size() == 3)
    assert(valueProps.getProperty("kafka.brokers1").equals("test#$kafka"))
    assert(valueProps.getProperty("kafka.brokers2").equals("test"))
  }

  @Test
  @TestStep(step = 2, desc = "用于测试insert的sql语句")
  def testInsetReg: Unit = {
    val sql1 = "insert into"
    val sql2 = "INSERT into"
    val sql3 = " insert asf "
    val sql4 = """insert into"""
    val sql5 =
      """
        |insert into
        |""".stripMargin
    val sql6 =
      """
        | insert into
        |""".stripMargin
    val sql7 =
      """
        |
        | insert into
        |""".stripMargin

    val sqls = Seq(sql1, sql2, sql3, sql4, sql5, sql6, sql7)
    // 用于匹配使用#号作为注释的所有结尾
    sqls.foreach(sql => {
      assert(RegularUtils.insertReg.findFirstIn(sql.toUpperCase).isDefined)
    })

    val sql8 =
      """
        |create xxx -- insert
        |""".stripMargin

    assert(RegularUtils.insertReg.findFirstIn(sql8.toUpperCase).isEmpty)

    val sql9 =
      """
        |c insert
        |""".stripMargin
    assert(RegularUtils.insertReg.findFirstIn(sql9.toUpperCase).isEmpty)
  }
}
