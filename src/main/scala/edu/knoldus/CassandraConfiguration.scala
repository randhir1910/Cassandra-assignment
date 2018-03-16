package edu.knoldus

import com.datastax.driver.core.Cluster
import com.typesafe.config.ConfigFactory

trait CassandraConfiguration {

  val config = ConfigFactory.load()
  val cassandraKeyspace = config.getString("cassandra.keyspace")

  val cluster = new Cluster.Builder().withClusterName("Knoldus").
      addContactPoints("localhost").build
  val session = cluster.connect
  session.execute(s"CREATE KEYSPACE IF NOT EXISTS  ${cassandraKeyspace} WITH REPLICATION = " +
      s"{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")

  session.execute(s"USE  ${cassandraKeyspace}")
  session

}

