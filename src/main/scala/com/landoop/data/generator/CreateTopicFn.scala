package com.landoop.data.generator

import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties}

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}

import scala.util.Try

object CreateTopicFn {
  def apply(topic: String,
            partitions: Int,
            replication: Int)(implicit arguments: Arguments): Try[Unit] = {
    Try {
      createAdminClient()
    }.map { implicit adminClient =>
      try {
        val topics = adminClient.listTopics().names().get()
        if (!topics.contains(topic)) {
          adminClient.createTopics(Collections.singleton(new NewTopic(topic, partitions, replication.toShort)))
            .all().get()
        }
      } finally {
        adminClient.close()
      }
    }
  }

  def createAdminClient()(implicit arguments: Arguments): AdminClient = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, arguments.brokers)
    AdminClient.create(props)
  }
}