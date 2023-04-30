package org.esgi.project.streaming.models.visits

import play.api.libs.json.{Json, OFormat}

case class Visit(
                  _id: String,
                  timestamp: String,
                  sourceIp: String,
                  url: String
                )

object Visit {
  implicit val format: OFormat[Visit] = Json.format[Visit]
}
