package org.esgi.project.streaming.models.visits

import play.api.libs.json.{Json, OFormat}

case class MeanLatencyForURL(
                              sum: Long,
                              count: Long,
                              meanLatency: Long
                            ) {
  def increment(latency: Long) = this.copy(sum = this.sum + latency, count = this.count + 1)

  def computeMeanLatency = this.copy(
    meanLatency = this.sum / this.count
  )
}


object MeanLatencyForURL {
  implicit val format: OFormat[MeanLatencyForURL] = Json.format[MeanLatencyForURL]

  def empty: MeanLatencyForURL = MeanLatencyForURL(0, 0, 0)
}
