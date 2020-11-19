package com.haizhi.volans.sink.config

import java.util

import com.haizhi.volans.sink.constant.Keys
import org.apache.commons.collections.MapUtils
import org.apache.commons.lang3.StringUtils

/**
  * Create by zhoumingbing on 2020-08-06
  */
case class StoreJanusConfig(database: String = "default",
                            label: String = "default",
                            graphElement: String = null,
                            backingIndex: String = "search",
                            graph: java.util.Map[String, AnyRef] = new util.HashMap[String, AnyRef](),
                            importBatchSize: Int = 2000) extends StoreConfig {

  def this() {
    this(null)
  }

  def isVertex(): Boolean = {
    StringUtils.equalsAnyIgnoreCase(graphElement, Keys.VERTEX)
  }

  def isEdge(): Boolean = {
    !isVertex()
  }

  override def getGraph: String = {
    database
  }

  override def getSchema: String = {
    label
  }

  def getScalaGraphConfig: Map[String, AnyRef] = {
    import scala.collection.JavaConversions._
    this.graph.toMap
  }

  def isConfigurationManagementGraph(): Boolean = {
    val graphName: String = MapUtils.getString(this.graph, "graph.graphname", "")
    StringUtils.equals(graphName, "ConfigurationManagementGraph")
  }

  def getBackingIndex(): String = {
    if (isConfigurationManagementGraph()) {
      return this.backingIndex
    } else {
      val config: Map[String, AnyRef] = this.getScalaGraphConfig
      for (elem <- config) {
        val key: String = elem._1
        if (StringUtils.contains(key, "index.")
          && StringUtils.contains(key, ".backend")) {
          val strings: Array[String] = StringUtils.split(String.valueOf(elem._1),".")
          return strings(1)
        }
      }
      return ""
    }
  }

  def canBuildMixedIndex(): Boolean = {
    val config: Map[String, AnyRef] = this.getScalaGraphConfig
    for (elem <- config) {
      val key: String = elem._1
      if (StringUtils.contains(key, "index.")
        && StringUtils.contains(key, ".backend")) {
        true
      }
    }
    false
  }

}
