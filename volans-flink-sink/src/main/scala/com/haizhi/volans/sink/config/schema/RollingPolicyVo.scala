package com.haizhi.volans.sink.config.schema

/**
 * Author pengxb
 * Date 2020/11/4
 */
case class RollingPolicyVo(workDir: String,
                           rolloverEnable: Boolean = false,
                           maxPartSize: Long = 128 * 1024 * 1024,
                           rolloverInterval: Long = 60,
                           inactivityInterval: Long = 60,
                           rolloverCheckInterval: Long = 60
                          )
/**
 * "workDir":"hdfs://cmb01:8020/user/work/bigdata/hive", Hive临时文件路径，必须是HDFS目录
 * "rolloverEnable": false, 是否开启合并策略，默认不合并
 * "maxPartSize":128, 单位：MB, 默认值：128MB，当临时文件达到该大小时，滚动成目标文件。如果设置成0，则表示不根据临时文件大小来滚动文件
 * "rolloverInterval":60, 单位：秒，默认值：60，当临时文件创建时间间隔时长达到该值时，将临时文件滚动成目标文件，如果设置成0，则表示不根据创建时间间隔时长来滚动文件,
 * "inactivityInterval":60 单位：秒，默认值：60，当临时文件最后修改时间间隔时长到达该大小时，将临时文件滚动成目标文件，如果设置成0，则表示不根据最后修改时间来滚动文件(建议与rolloverInterval保持一致)
 * "rolloverCheckInterval": 60, 单位：秒，默认值：60，文件滚动定时检查的间隔时间，不能超过rolloverInterval和inactivityInterval
 */