package com.landoop.data.generator

import com.landoop.data.generator.stocks.{StockGenerator, Tick}
import com.sksamuel.pulsar4s.{ProducerConfig, PulsarClient, Topic}
import com.typesafe.scalalogging.StrictLogging
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.impl.conf.ClientConfigurationData
import org.apache.pulsar.common.policies.data.TenantInfo
import scopt.OptionParser

object Pulsar extends App with StrictLogging {

  import scala.collection.JavaConverters._

  case class PulsarConfig(tenant: String,
                          namespace: String,
                          topic: String,
                          brokerUrl: String,
                          httpUrl: String)

  logger.info(
    """
      |
      |  _
      | | |    ___ _ __  ___  ___  ___
      | | |   / _ \ '_ \/ __|/ _ \/ __|
      | | |__|  __/ | | \__ \  __/\__ \
      | |_____\___|_|_|_|___/\___||___/                         _
      | |  _ \  __ _| |_ __ _   / ___| ___ _ __   ___ _ __ __ _| |_ ___  _ __
      | | | | |/ _` | __/ _` | | |  _ / _ \ '_ \ / _ \ '__/ _` | __/ _ \| '__|
      | | |_| | (_| | || (_| | | |_| |  __/ | | |  __/ | | (_| | || (_) | |
      | |____/ \__,_|\__\__,_|  \____|\___|_| |_|\___|_|  \__,_|\__\___/|_|
      |
    """.stripMargin)

  val parser: OptionParser[PulsarConfig] = new scopt.OptionParser[PulsarConfig]("generator") {
    head("generator")

    opt[String]("tenant").required().action { case (tenant, a) =>
      a.copy(tenant = tenant)
    }.validate(f => if (f.trim.isEmpty) Left("Invalid tenant") else Right(()))
      .text("Pulsar tenant")

    opt[String]("namespace").required().action { case (namespace, a) =>
      a.copy(namespace = namespace)
    }.validate(f => if (f.trim.isEmpty) Left("Invalid namespace") else Right(()))
      .text("Pulsar namespace")

    opt[String]("topic").required().action { case (topic, a) =>
      a.copy(topic = topic)
    }.validate(f => if (f.trim.isEmpty) Left("Invalid topic") else Right(()))
      .text("Pulsar topic")

    opt[String]("brokerUrl").required().action { case (brokerUrl, a) =>
      a.copy(brokerUrl = brokerUrl)
    }.validate(f => if (f.trim.isEmpty) Left("Invalid brokerUrl") else Right(()))
      .text("Pulsar binaryUrl")

    opt[String]("httpUrl").required().action { case (httpUrl, a) =>
      a.copy(httpUrl = httpUrl)
    }.validate(f => if (f.trim.isEmpty) Left("Invalid httpUrl") else Right(()))
      .text("Pulsar httpUrl")
  }

  parser.parse(args, PulsarConfig("", "", "", "", "")).foreach { config =>

    val adminConfig = new ClientConfigurationData()
    val admin = new PulsarAdmin(config.httpUrl, adminConfig)

    if (!admin.tenants().getTenants.contains(config.tenant)) {
      admin.tenants().createTenant(config.tenant, new TenantInfo(Set.empty[String].asJava, Set("standalone").asJava))
    }

    if (!admin.namespaces().getNamespaces(config.tenant).contains(config.tenant + "/" + config.namespace)) {
      admin.namespaces().createNamespace(config.tenant + "/" + config.namespace, Set("standalone").asJava)
    }

    val client = PulsarClient(config.brokerUrl)
    val topic = Topic(s"persistent://${config.tenant}/${config.namespace}/${config.topic}")
    val producer = client.producer[Tick](ProducerConfig(topic, enableBatching = Some(false)))(TickSchema)

    for (_ <- 1 to 10000) {
      producer.send(StockGenerator.generateTick)
      println("LastSequenceId=" + producer.lastSequenceId)
    }

    producer.close()
    client.close()
  }
}
