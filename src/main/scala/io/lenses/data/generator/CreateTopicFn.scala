package io.lenses.data.generator

import java.util.{Collections, Properties}

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}

import scala.util.Try

object CreateTopicFn {
  def apply(arguments: Arguments): Try[Unit] = {
    Try {
      createAdminClient(arguments.brokers)
    }.map { implicit adminClient =>
      try {
        val topics = adminClient.listTopics().names().get()
        if (!topics.contains(arguments.topic)) {
          adminClient
            .createTopics(
              Collections.singleton(
                new NewTopic(
                  arguments.topic,
                  arguments.partitions,
                  arguments.replication.toShort
                )
              )
            )
            .all()
            .get()
        }
      } finally {
        adminClient.close()
      }
    }
  }

  def createAdminClient(brokers: String): AdminClient = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers)
    AdminClient.create(props)
  }
}
