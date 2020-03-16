package clients

import java.lang

import org.apache.commons.dbcp2.BasicDataSource
import java.sql.{Connection, PreparedStatement, SQLException, Statement}

import javax.sql.DataSource
import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit
import java.util.{Date, TimeZone}

import model.JsonRecord
import org.postgresql.util.{PGobject, PSQLException}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import org.json4s.jackson.Serialization

class PostgresClient {}

case class DataSourceDefn(url: String, classDriveName: String, maxConnections: Integer = -1)

// TODO: add connection pool
trait UsingTSDB {
  implicit val host: String
  implicit val port: Int
  implicit val database: String
  implicit val username: String
  implicit val password: String
  implicit val connectTimeout: Int = 10
  implicit val tcpKeepAlive: Boolean = true
  implicit val applicationName: String = com.snowplowanalytics.stream.loader.generated.Settings.name
  implicit val reWriteBatchedInserts: Boolean = true
  implicit val maxConnections: Integer = -1
  implicit val parentTable: String
  implicit val schemas: Map[String, String]
  implicit val partitionDateField: Option[String]

  val sqlDateFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  sqlDateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"))

  val log = LoggerFactory.getLogger(getClass)
  val mainSchema = "atomic"
  val partitionSchema = "partition"
  val coreTableName = "events"
  val schemaFiles: mutable.Map[String, String] = collection.mutable.Map(schemas.toSeq: _*)
  val existingTables = mutable.Set[String]()
  implicit val formats = org.json4s.DefaultFormats

  implicit val defn: DataSourceDefn =
    DataSourceDefn(
      url = s"jdbc:postgresql://$host:$port/$database?" +
        s"user=$username" +
        s"&password=$password" +
        s"&connectTimeout=$connectTimeout" +
        s"&tcpKeepAlive=$tcpKeepAlive" +
        s"&ApplicationName=$applicationName" +
        s"&reWriteBatchedInserts=$reWriteBatchedInserts" +
        s"&stringtype=unspecified",
      classDriveName = "org.postgresql.Driver"
    )

  implicit val dataSource = createDataSource

  protected def write(partitionName: String, jsonRecords: List[JsonRecord])(
    implicit dataSource: DataSource
  ) = {

    val tableNames = Set(coreTableName)
    val tableNamesWithPartition =
      tableNames
        .map(tableName => (tableName, tableNameForPartition(tableName, partitionName)))
        .toMap
    createParentTablesIfNotExists(tableNames)
    if (partitionName != null) { // partitioning is active
      log.error("no need for partitioning in TSDB")
    }
    withConnection { implicit connection =>
      connection.setAutoCommit(false)
      jsonRecords
        .map(jsonRecord => createParentTableQueriesTSDB(jsonRecord.json.values))
        .map { statement =>
          try {
            statement.execute()

          } catch {
            case ex: SQLException =>
              connection.rollback()
              if (!connection.getAutoCommit)
                connection.setAutoCommit(true)
              //cleaning table cache for retry
              existingTables.clear()
              //log what happened
              log.error(ex.getMessage, ex)
              log.error(statement.toString)
              ex.getSQLState
          }
        }
      try {
        connection.commit() //commit once for all
        connection.setAutoCommit(true)
        Array.fill[String](jsonRecords.size)("00000").toList
      } catch {
        case ex: SQLException =>
          connection.rollback()
          if (!connection.getAutoCommit)
            connection.setAutoCommit(true)
          //cleaning table cache for retry
          existingTables.clear()
          //log what happened
          log.error(ex.getMessage, ex)
          Array.fill[String](jsonRecords.size)(ex.getSQLState).toList
      }

    }
  }

  private def createDataSource(implicit dsDefn: DataSourceDefn) = {
    val ds = new BasicDataSource()
    ds.setDriverClassName(dsDefn.classDriveName)
    ds.setUrl(dsDefn.url)
    ds
  }

  private def withConnection[X](fn: Connection => X)(implicit dataSource: DataSource) = {
    val c = dataSource.getConnection
    try fn(c)
    finally c.close
  }

  private def withStatement[X](fn: Statement => X)(implicit dataSource: DataSource) =
    withConnection { connection =>
      val statement = connection.createStatement()
      try fn(statement)
      finally statement.close
    }

  /**
   * Generates insert query for parent table
   * @param jsonFields
   * @param connection
   * @return the prepared statement
   */
  private def createParentTableQueriesTSDB(jsonFields: Map[String, Any])(
    implicit connection: Connection
  ): PreparedStatement = {

    val columnNames = jsonFields.keys.map(c => utils.JsonUtils.camelToSnake(c)).mkString(",")
    val stringSigns = List.fill(jsonFields.values.size)("?").mkString(",")
    val insertStatement =
      s"INSERT INTO $mainSchema.$coreTableName ($columnNames) VALUES ($stringSigns)"
    convertJsonFieldsToPreparedStatement(insertStatement, jsonFields)
  }

  /**
   * Puts json fields inside the prepared statement
   * @param statement
   * @param jsonFields
   * @param connection
   * @return a prepared statement ready to be executed
   */
  private def convertJsonFieldsToPreparedStatement(statement: String, jsonFields: Map[String, Any])(
    implicit connection: Connection
  ): PreparedStatement = {
    val ps = connection.prepareStatement(statement)
    jsonFields.zipWithIndex.map { case (v, index) => (v, index + 1) }.foreach {
      case ((key, value: String), index: Int)  => ps.setString(index, value)
      case ((_, value: Boolean), index: Int)   => ps.setBoolean(index, value)
      case ((_, value: Double), index: Int)    => ps.setDouble(index, value)
      case ((_, value: Int), index: Int)       => ps.setInt(index, value)
      case ((_, value: Long), index: Int)      => ps.setLong(index, value)
      case ((_, value: BigInt), index: Int)    => ps.setLong(index, value.toLong)
      case ((_, value: List[Any]), index: Int) =>
        // the alternative is to use : connection.createArrayOf(typeName, data); or https://jdbc.postgresql.org/documentation/head/arrays.html
        ps.setObject(index, value)
      case ((_, null | None), index: Int) => ps.setString(index, null)
      case ((_, value: Map[Any, Any]), index: Int) =>
        val pgObject = new PGobject
        pgObject.setType("json")
        pgObject.setValue(
          Serialization
            .write(value)
            .replaceAll("\u0000", "")
            .replaceAll("\\u0000", "") // some values from arc feed have null characters
        )
        ps.setObject(index, pgObject)
      case ((key, value: Any), index: Int) =>
        throw new RuntimeException(
          s"error preparing $statement: the value of $key at index $index is of an unexpected type: $value"
        )
    }
    ps
  }

  /**
   * generating similar data structure for unstrcut and contexts child tables
   * @param jsonFields
   * @return
   */
  private def convertChildJsonToColumnValue(jsonFields: Map[String, Any]) =
    try {
      jsonFields
        .map {
          case (contextEventName: String, contextEventData: List[Map[String, Any]]) =>
            (
              contextEventName,
              List(contextEventData.flatMap(internalMap2 => internalMap2.toList).toMap)
            )
          // I had to do that wired flatMap to convert Map2/MapN to Seq and then to regular Map. This is either a Scala issue, Snowplow Bug or the library that converted that Json Object to Map.
          // We get MatchError exception, because filter doesn't work for all of the Map kids. The Library creates Map2, Map3, etc instead of Map
          // All of them go through this match/case but not filter
          // Below is an example of the data that turns out to be Map2
          //(contexts_org_ietf_http_cookie_1,List(Map(name -> sp, value -> 1f0644fc-79e8-4b40-aee7-76dafe620e81), Map(name -> sp, value -> 1f0644fc-79e8-4b40-aee7-76dafe620e81))) (of class scala.Tuple2)
          case (unstructEventName: String, unstructEventData: Map[String, Any]) =>
            (unstructEventName, List(unstructEventData))
          case _ =>
            throw new RuntimeException(s"Not sure what do you want with ${jsonFields.toString}")
        }
        .toList
        .filter {
          case (_, List(keyValues)) => {
            val valuesSet = keyValues.values.toSet
            !(
              (valuesSet.isEmpty ||
                valuesSet.size == 1 && (valuesSet == Set(null) || valuesSet == Set(None)))
                || (valuesSet.size == 2 && valuesSet == Set(null, None))
            ) // if all empty, what is the point?
          }
        }

    } catch {
      case e: scala.MatchError =>
        log.error("matching issue, skipping data for now to solve it: ", e)
        throw e
    }

  private def tableExists(tableName: String)(implicit dataSource: DataSource): Boolean =
    existingTables.contains(tableName) || withStatement { implicit statement =>
      val sql = s"SELECT * FROM $tableName LIMIT 1"
      Try(statement.executeQuery(sql)) match {
        case Success(_) =>
          existingTables += tableName
          true
        case Failure(e) => false
      }
    }

  /**
   * Creating the parent table with double check pattern
   * @param tableNames
   * @param dataSource
   */
  private def createParentTablesIfNotExists(
    tableNames: Set[String]
  )(implicit dataSource: DataSource) =
    withStatement { implicit statement =>
      tableNames.foreach { tableName =>
        schemaFiles.get(tableName) match {
          case Some(schema) => createTableWithDoubleCheck(tableName, schema)
          case None         => throw new RuntimeException(s"schema file doesn't exist for $tableName")
        }
      }
    }

  /**
   * Creates the table with double check pattern
   * @param tableName
   * @param schema
   * @param dataSource
   * @return
   */
  private def createTableWithDoubleCheck(tableName: String, schema: String)(
    implicit dataSource: DataSource
  ) =
    withStatement { implicit statement =>
      if (!tableExists(s"$mainSchema.$tableName")) {
        try {
          statement.execute(schema)
        } catch {
          case e: PSQLException =>
            if (!tableExists(s"$mainSchema.$tableName")) {
              log.error(s"error in creating the table $mainSchema.$tableName", e)
            }
          case e: Exception => throw e
        }
      }
    }

  /**
   * What is the table name for this partition date value
   * @param tableName
   * @param partitionName
   * @return
   */
  private def tableNameForPartition(tableName: String, partitionName: String): String =
    partitionName match {
      case null => tableName
      case p    => tableName + "_" + p
    }

}
