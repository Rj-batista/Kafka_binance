package org.esgi.project.streaming.models.visits

import play.api.libs.json.{Json, OFormat}

case class Metric(
                   _id: String,
                   timestamp: String,
                   latency: Long
                 )


object Metric {
  implicit val format: OFormat[Metric] = Json.format[Metric]
}
