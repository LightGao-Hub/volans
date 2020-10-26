package com.haizhi.volans.common.flink.base.scala.exception

/**
  * Create by Hanson on 2019/08/19
  */
case class ErrorMessage(id: String,
                        object_key: String,
                        var task_instance_id: String,
                        error_type: String,
                        error_log: String,
                        affected_store: String,
                        data_row: String,
                        updated_dt: String)
