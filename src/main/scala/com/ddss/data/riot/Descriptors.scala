package com.ddss.data.riot

import iot.trigger.Trigger
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}

/**
 * State descriptors defined in the Job
 */
object Descriptors {

  val ruleTriggerDescriptor = new MapStateDescriptor[String, Trigger](
    "RulesBroadcastState",
    BasicTypeInfo.STRING_TYPE_INFO,
    TypeInformation.of(new TypeHint[Trigger]() {}));

}
