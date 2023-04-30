package org.esgi.project.streaming.models.visits

import play.api.libs.json.{Json, OFormat}

case class VisitWithLatency(
                             _id: String,
                             timestamp: String,
                             sourceIp: String,
                             url: String,
                             latency: Long
                           )



object VisitWithLatency {
  implicit val format: OFormat[VisitWithLatency] = Json.format[VisitWithLatency]
}
