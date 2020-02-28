/*
 * © Copyright 2020 The Globe and Mail
 */
package com.snowplowanalytics.snowplow.storage.kinesis.s3.clients

import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode}
import com.snowplowanalytics.stream.loader.model.JsonRecord
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.kitesdk.data.spi.{JsonUtil, SchemaUtil}
import org.slf4j.{Logger, LoggerFactory}
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * This class converts Json4s JValue to Jackson JsonNode and then writes to S3 as Parquet
 */
trait UsingS3Parquet {
  val log: Logger = LoggerFactory.getLogger(getClass)

  implicit val bucket: String
  implicit val accessKey: String
  implicit val secretKey: String
  implicit val partitionDateField: Option[String]

  private val One = 1
  private val Two = 2

  val conf = new Configuration()
  //Based on  https://docs.oracle.com/database/nosql-12.1.3.0/GettingStartedGuide/avroschemas.html
  val avroPrimitiveTypes: Seq[Type] = Seq(Type.STRING,Type.INT,Type.DOUBLE,Type.LONG,Type.FLOAT,Type.BOOLEAN)
  if (accessKey != "iam" && secretKey != "iam") {
    conf.set("fs.s3a.access.key", accessKey)
    conf.set("fs.s3a.secret.key", secretKey)
  } else {
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
  }


  object SchemaHandler {
    var schema: Schema = null
    val SAMPLE_SIZE    = 500

    def takeSample(records: List[JsonNode]): Iterable[JsonNode] =
      records
        .groupBy(
          jsonNode =>
            jsonNode.get("event_vendor").asText + "." +
              jsonNode.get("event_name").asText + "." +
              jsonNode.get("event_version").asText
        )
        .flatMap {
          case (_, values) =>
            val sampleLength = if (values.length < SAMPLE_SIZE) values.length else SAMPLE_SIZE
            values.slice(0, sampleLength)
        }

    /**
     * Infers schema using Kite SDK
     *
     * @param records array of Jackson JsonNodes
     * @return the Avro schema
     */
    def inferAvroSchema(records: List[JsonNode]): Schema = {
      val result = records
        .map(r => JsonUtil.inferSchema(r, "event"))
        .reduceLeft((right, left) => SchemaUtil.merge(right, left))

      if (schema == null) {
        schema = result
      }
      else {
        schema = SchemaUtil.mergeOrUnion(List(schema, result).asJava)
      }
      schema
    }
  }

  /**
   * Based on https://avro.apache.org/docs/1.8.1/spec.html#schema_primitive
   *
   * @param jsonNode coming from Jackson
   * @param schema   the Avro Schema infered
   * @return Avro Records to be written later
   */
  def convertJsonNodeToAvroRecord(
    jsonNode: JsonNode,
    schema: Schema
  ): Either[(Throwable, JsonNode), GenericData.Record] = {
    val output = new GenericData.Record(schema)
    try {
      jsonNode.fields().asScala.toList.foreach { v =>
        if (v.getValue.isArray) {
          //This is used to create an array of primitive Avro Types below
          var primtiveInternalObjects: Boolean = false
          val internalObjects = v.getValue
            .iterator()
            .asScala
            .toList
            .map { r =>
              {
                val schemaForTheElement = if (schema.getField(v.getKey).schema().getType == Type.UNION) {
                  schema
                    .getField(v.getKey)
                    .schema()
                    .getTypes
                    .asScala
                    .filter(_.getType == Type.ARRAY)
                    .head
                    .getElementType
                } else {
                  schema.getField(v.getKey).schema().getElementType
                }
                //If array internal type is complex only then we create Generic Record Type
                if(!avroPrimitiveTypes.contains(schemaForTheElement.getType)){
                  convertJsonNodeToAvroRecord(r, schemaForTheElement) match {
                    case Right(record)                     => record
                    case Left((throwable, jsonNodeRecord)) => throw throwable
                  }
                }
                  //If type is primitive we will extract it as Text
                else{
                  primtiveInternalObjects = true
                  r.asText()
                }

              }
            }
            .asJava
          val objectSchema = if (schema.getField(v.getKey).schema().getType == Type.UNION) {
            schema.getField(v.getKey).schema().getTypes.asScala.filter(_.getType == Type.ARRAY).head
          } else {
            schema.getField(v.getKey).schema()
          }
          //if internal array type is primitive then create array of string else GenRecord
          val avroArray = primtiveInternalObjects match {
            case true => new GenericData.Array[String](objectSchema, internalObjects.asScala.map(_.asInstanceOf[String]).asJava)
            case false => new GenericData.Array[GenericData.Record](objectSchema, internalObjects.asScala.map(_.asInstanceOf[GenericData.Record]).asJava)
          }
          output.put(v.getKey, avroArray)
        } else if (v.getValue.isObject) {
          val objectSchema = if (schema.getField(v.getKey).schema().getType == Type.UNION) {
            // When the field is optional, it can have ["null", "our_type"]
            schema.getField(v.getKey).schema().getTypes.asScala.filter(_.getType != Type.NULL).head
          } else {
            schema.getField(v.getKey).schema()
          }
          output.put(v.getKey, convertJsonNodeToAvroRecord(v.getValue, objectSchema) match {
            case Right(record)                     => record
            case Left((throwable, jsonNodeRecord)) => throw throwable
          })
        } else if (v.getValue.isLong) {
          output.put(v.getKey, v.getValue.asLong)
        } else if (v.getValue.isDouble) {
          output.put(v.getKey, v.getValue.asDouble)
        } else if (v.getValue.isInt) {
          output.put(v.getKey, v.getValue.asInt)
        } else if (v.getValue.isBoolean) {
          output.put(v.getKey, v.getValue.asBoolean)
        } else {
          output.put(v.getKey, v.getValue.asText)
        }
      }
      Right(output)
    } catch {
      case e: Throwable => Left((e, jsonNode))
    }
  }

  private def initializeWriter(path: String, schema: Schema) = {
    AvroParquetWriter
      .builder[GenericData.Record](new Path(path))
      .withConf(conf)
      .withCompressionCodec(CompressionCodecName.GZIP)
      .withSchema(schema)
      .build()
  }


  /**
   * Avro gets mad when writing null values or empty list/objects. Its documentation says just ignore da data!
   * @param records
   * @return
   */
  protected def cleanUpRecords(records: List[JsonRecord]) = {
    // Kite SDK doesn't like BigInteger (default)
    mapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true)
    records.map(
      r =>
        asJsonNode(
          utils.JsonUtils.removeNullAndEmpty(r.json)
        )
    )
  }

  /**
   * To write the data to S3
   *
   * @param partition partitions based on a timestamp field
   * @param records   to be written
   * @return failures
   */
  def write(partition: Option[String], records: List[JsonNode]): Array[Int] = {
    val path = (partitionDateField, partition) match {
      case (Some(partitionDateFieldValue), partitionValue) => s"/$partitionDateFieldValue=$partitionValue/"
      case (None, null)                                    => "/"
    }
    val fileName = "events-" + System.currentTimeMillis() + ".parquet.gz"
    // sampling idea didn't work. The parquet writer has schema and also every avro record. all of them should match
    //val sampledRecords = SchemaHandler.takeSample(records)
    val schema = SchemaHandler.inferAvroSchema(records)

    val convertedRecords = records
      .map { jsonNodeRecord =>
        convertJsonNodeToAvroRecord(jsonNodeRecord, schema)
      }
      .map {
        case Right(avroRecord) => avroRecord
        case Left((throwable, jsonNodeRecord)) =>
          log.warn(s"The initial schema did not cover the record, inferring the schema: ${jsonNodeRecord.toString}")
          // let's ignore such records for now because we cannot change the schema as we go. we have to process for all records
          null
        /* convertJsonNodeToAvroRecord(jsonNodeRecord, SchemaHandler.inferAvroSchema(List(jsonNodeRecord))) match {
          case Right(avroRecord) => avroRecord
          case Left((throwable2, _)) =>
            log.error("Could not infer the schema for record: ", throwable2)
            null
        } */
      }

    val s3Writer = initializeWriter(bucket + path + fileName, schema)

    val results = convertedRecords.map { avroRecord =>
      if (avroRecord != null) {
        Try(s3Writer.write(avroRecord)) match {
          case Success(_) => 0
          case Failure(exception) =>

            log.error("error when writing Avro record in Parquet file", exception)

            -Two // parquet writer issue
        }
      } else {
        -One // schema issue
      }

    }

    s3Writer.close()
    results.toArray
  }

}
