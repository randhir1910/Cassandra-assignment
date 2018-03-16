package edu.knoldus

import com.datastax.driver.core.{ConsistencyLevel, Session}

import scala.collection.JavaConverters._

object CassandraOperation extends App with CassandraConfiguration {

  session.getCluster.getConfiguration.getQueryOptions.setConsistencyLevel(ConsistencyLevel.QUORUM)
  createTable(session)
  insertRecord(session)
  getRecordById(session, 2)
  updateRecord(session, 1, 50000)
  getRecordBySalary(session, 4000, 1)
  getRecordByCity(session, "chandigarh")
  deleteRecordByCity(session, "chandigarh")
  session.close()

  private def createTable(session: Session): Unit = {
    session.execute("drop table emp")
    session.execute("create table if not exists emp (id int, name text, city text,salary varint, phone varint, primary key(id,salary))")
    session.execute("create table if not exists emp1(id int, name text, city text,salary varint, phone varint, primary key(city))")
    session.execute("create index cityIndex on emp(city)")
  }

  private def insertRecord(session: Session): Unit = {

    session.execute("insert into emp(id,name,city,salary,phone) values(2,'randhir','chennai',12000,9953188803)")
    session.execute("insert into emp(id,name,city,salary,phone) values(1,'rahul','Kolkata',50000,1234567890)")
    session.execute("insert into emp(id,name,city,salary,phone) values(3,'shubham','chandigarh',12000,888899999)")
    session.execute("insert into emp(id,name,city,salary,phone) values(4,'vinisha','delhi',12000,7773188803)")

    session.execute("insert into emp1(id,name,city,salary,phone) values(2,'randhir','chennai',12000,9953188803)")
    session.execute("insert into emp1(id,name,city,salary,phone) values(1,'rahul','Kolkata',50000,1234567890)")
    session.execute("insert into emp1(id,name,city,salary,phone) values(3,'shubham','chandigarh',12000,888899999)")
    session.execute("insert into emp1(id,name,city,salary,phone) values(4,'vinisha','delhi',12000,7773188803)")
  }

  private def getAllRecord(session: Session): Unit = {
    val record = session.execute("select * from emp").asScala.toList
    record.foreach(println(_))
  }

  private def getRecordById(session: Session, emp_id: Int): Unit = {

    val record = session.execute(s"select * from emp where id=${emp_id}").asScala.toList
    record.foreach(println(_))
    println()
  }

  private def updateRecord(session: Session, id: Int, salary: Int): Unit = {
    session.execute(s"update emp set city='chandigarh' where id=${id} AND salary=${salary}")
    getAllRecord(session)
    println()
  }

  private def getRecordBySalary(session: Session, salary: Int, id: Int): Unit = {
    val record = session.execute(s"select * from emp where id =${id} AND salary > ${salary} ")
    println("fetch record salary where greater than 30000")
    record.forEach(println(_))
    println()
  }

  private def getRecordByCity(session: Session, city: String): Unit = {


    val record = session.execute(s"select * from emp where city ='${city}'").asScala.toList
    record.foreach(println(_))
    println()
  }

  private def deleteRecordByCity(session: Session, city: String): Unit = {

    session.execute(s"delete from emp1 where city = '${city}'")
    val record = session.execute("select * from emp1").asScala.toList
    record.foreach(println(_))
  }
}