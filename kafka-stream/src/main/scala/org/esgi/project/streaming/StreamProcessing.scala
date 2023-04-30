package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import io.github.azhur.kafka.serde.PlayJsonSupport.toSerde
import jdk.jfr.internal.handlers.EventHandler.timestamp
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{Printed, TimeWindows, Windowed}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable, Materialized}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes.{Double, JavaDouble, String}
import org.apache.kafka.streams.scala.serialization.Serdes.longSerde
import org.esgi.project.streaming.models.binance.{Trade, Trading}
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KTable}
import org.esgi.project.Main.system

import scala.Double._
import java.time.Duration
import java.util.Properties
import scala.Double.MinValue

object StreamProcessing extends PlayJsonSupport {

  val applicationName = s"some-application-name"

  private val props: Properties = buildProperties

  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  val topicName: String = "trades"

  val tradesStreamTest: KStream[String, String] = builder.stream[String, String](topicName)

  /** Just a simple printing of the stream
    *
    * @return
    */
  //val tradesTable: KTable[String, Long] = tradesStreamTest.mapValues(trade => 1L).groupByKey.reduce(_ + _)
  //tradesTable.toStream.foreach((k, v) => println(s"key: $k, value: $v"))

  //Nom de tous les stores

  val tradeByPairByMinuteStoreName: String = "TradeByPairByMinute"
  val tradeVolByPairByMinuteStoreName: String = "TradeVolByPairByMinute"
  val tradeVolByPairByHourStoreName: String = "TradeVolByPairByHour"
  val meanPriceByPairByMinuteStoreName: String = "MeanPriceByPairByMinute"
  val openClosePriceByPairByMinuteStoreName: String = "OpenClosePriceByPairByMinute"

  implicit val tradingSerde: Serde[Trading] = toSerde[Trading]
  implicit val tradeSerde: Serde[Trade] = toSerde[Trade]

  val trading: KStream[String, Trading] = builder.stream[String, Trading](topicName)

  val tradesStream: KStream[String, Trade] = trading.mapValues(trading => {
    val price = trading.p.toDouble
    val quantity = trading.q.toDouble
    Trade(
      eventType = trading.e,
      eventTime = trading.E,
      symbol = trading.s,
      tradeId = trading.t,
      price = price,
      quantity = quantity,
      buyerOrderId = trading.b,
      sellerOrderId = trading.a,
      tradeTime = trading.T,
      isMarketMaker = trading.m,
      ignore = trading.M
    )
  })

  val tradesByPair: KGroupedStream[String, Trade] = tradesStream.groupBy((_, trade) => trade.symbol)

  val tradeByPairByMinute: KTable[Windowed[String], Long] = tradesByPair
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1)))
    .count()(Materialized.as(tradeByPairByMinuteStoreName))

  val tradeVolByPairByMinute: KTable[Windowed[String], Double] = tradesByPair
    .windowedBy(
      TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1))
    )
    .aggregate[Double](0.0)((_, trade, agg) => agg + trade.quantity)(Materialized.as(tradeVolByPairByMinuteStoreName))

  val tradeVolByPairByHour: KTable[Windowed[String], Double] = tradesByPair
    .windowedBy(
      TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)).advanceBy(Duration.ofHours(1))
    )
    .aggregate[Double](0.0)((_, trade, agg) => agg + trade.quantity)(Materialized.as(tradeVolByPairByHourStoreName))

  // Prix moyen par pair par minute

  val meanPriceByPairByMinute: KTable[Windowed[String], Double] = tradesByPair
    .windowedBy(
      TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1))
    )
    .aggregate[Double](0.0)((_, trade, agg) => agg + trade.price)(Materialized.as(meanPriceByPairByMinuteStoreName))
    .mapValues((priceSum, count) => priceSum.toString.toDouble / count)

  // Prix d'ouverture et de fermeture par pair par minute
  val openClosePriceByPairByMinute: KTable[Windowed[String], (Double, Double)] = tradesByPair
    .windowedBy(
      TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1))
    )(Materialized.as(openClosePriceByPairByMinuteStoreName))

  def run(): KafkaStreams = {
    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run(): Unit = {
        streams.close()
      }
    }))
    streams
  }

  def topology: Topology = builder.build()

  // auto loader from properties file in project
  def buildProperties: Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(StreamsConfig.CLIENT_ID_CONFIG, applicationName)
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    properties
  }
}
