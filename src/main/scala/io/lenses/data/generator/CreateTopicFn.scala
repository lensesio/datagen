package io.lenses.data.generator

import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}

import scala.util.Try
import io.lenses.data.generator.cli.GenRecords
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.admin.NewPartitions
import org.apache.kafka.clients.admin.ListTopicsOptions

object CreateTopicFn extends StrictLogging {
  def apply(arguments: GenRecords): Try[Unit] = {
    Try {
      createAdminClient(arguments.brokers)
    }.map { implicit adminClient =>
      try {

        val maybeTopicDescription = {
          if (adminClient.listTopics().names().get().contains(arguments.topic))
            adminClient
              .describeTopics(Collections.singleton(arguments.topic))
              .all()
              .get()
              .asScala
              .get(arguments.topic)
          else None
        }

        maybeTopicDescription match {
          case None =>
            println(s"Creating topic [${arguments.topic}]")
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
          case Some(description) =>
            if (description.partitions.size >= arguments.partitions) {
              logger.info(
                s"topic [${arguments.topic}] has [${description.partitions().size()}] partitions. Required value is [${arguments.partitions}]"
              )

            } else {
              logger.info(
                s"trying to increase the partitions of topic [${arguments.topic}] to [${arguments.partitions}]"
              )

              val newParitions = Map(
                arguments.topic -> NewPartitions.increaseTo(
                  arguments.partitions
                )
              )

              adminClient.createPartitions(newParitions.asJava).all()
            }

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
