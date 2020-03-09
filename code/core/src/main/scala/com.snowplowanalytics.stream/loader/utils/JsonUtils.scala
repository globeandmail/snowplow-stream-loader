/*
 * Copyright (c) 2013-2017 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package utils

import java.util.regex.Pattern

import org.json4s.{DefaultFormats, Formats, JObject}
import org.json4s.JsonAST.{JArray, JField, JNothing, JNull, JString, JValue}

// Scalaz
import scalaz.Scalaz._
import scalaz._

import scala.util.{Failure, Success, Try}

object JsonUtils {
  implicit val formats = DefaultFormats
  val xpathPattern     = Pattern.compile("([A-Za-z0-9_]+)[\\[]*([0-9]*)[\\]]*")

  // to rm once 2.12 as well as the right projections
  def fold[A, B](t: Try[A])(ft: Throwable => B, fa: A => B): B = t match {
    case Success(a) => fa(a)
    case Failure(t) => ft(t)
  }

  /**
   * Extract the event_id field from an event JSON for use as a document ID
   *
   * @param json object produced by the Snowplow Analytics SDK
   * @return Option boxing event_id
   */
  def extractEventId(json: JValue): Option[String] =
    extractField(json, "event_id")

  /**
   * Extract any field
   *
   * @param json object produced by the Snowplow Analytics SDK
   * @return Option boxing event_id
   */
  def extractField(json: JValue, fieldName: String): Option[String] =
    json \ fieldName match {
      case JString(eid) => eid.some
      case _            => None
    }

  /**
   * rename a field
   * @param json object produced by the Snowplow Analytics SDK
   * @return Option boxing event_id
   */
  def renameField(json: JValue, mappingTable: Option[Map[String, String]]): JValue = //, fromName: String, toName: String): JValue = {
    mappingTable match {
      case None => json
      case Some(mapping) =>
        json.transformField {
          case (from, s) =>
            mapping.get(from) match {
              case Some(mappedValue) => (mappedValue, s)
              case None              => (from, s)
            }
        }
    }

  def removeNullAndEmpty(json: JValue): JValue =
    json removeField {
      case (_, JNull | JNothing) => true
      case (_, JArray(arr)) => arr.isEmpty || arr == List(JObject(List())) || arr == List(JObject())
      case (_, JObject(kv)) => kv.isEmpty
      case (k, _)           => false
    }

  def camelToSnake(name: String) =
    "[A-Z\\d]".r.replaceAllIn(name, { m =>
      "_" + m.group(0).toLowerCase()
    })

  def snakeToCamel(name: String) =
    "_([a-z\\d])".r.replaceAllIn(name, { m =>
      m.group(1).toUpperCase()
    })

  def denormalizeEvent(json: JValue): JValue = {
    val deprecatedFields = List(
      "br_family",
      "br_name",
      "br_renderengine",
      "br_type",
      "br_version",
      "dvce_ismobile",
      "dvce_type",
      "os_family",
      "os_manufacturer",
      "os_name",
      "txn_id",
      "unstruct_event",
      "event",
      "contexts",
      "derived_contexts" //empty and deprecated
    )
    val snowplowContext = List(
      "doc_charset",
      "doc_height",
      "doc_width",
      "useragent",
      "dvce_screenheight",
      "dvce_screenwidth",
      "dvce_type",
      "dvce_created_tstamp",
      "dvce_sent_tstamp",
      "os_timezone",
      "br_colordepth",
      "br_cookies",
      "br_family",
      "br_features_director",
      "br_features_flash",
      "br_features_gears",
      "br_features_java",
      "br_features_pdf",
      "br_features_quicktime",
      "br_features_realplayer",
      "br_features_silverlight",
      "br_features_windowsmedia",
      "br_lang",
      "br_name",
      "br_renderengine",
      "br_type",
      "br_version",
      "br_viewheight",
      "br_viewwidth",
      "domain_sessionid",
      "domain_sessionidx",
      "domain_userid",
      "network_userid",
      "user_fingerprint",
      "user_id",
      "user_ipaddress",
      "geo_city",
      "geo_country",
      "geo_latitude",
      "geo_location",
      "geo_longitude",
      "geo_region",
      "geo_region_name",
      "geo_timezone",
      "geo_zipcode",
      "ip_domain",
      "ip_isp",
      "ip_netspeed",
      "ip_organization",
      "mkt_campaign",
      "mkt_clickid",
      "mkt_content",
      "mkt_medium",
      "mkt_network",
      "mkt_source",
      "mkt_term",
      "refr_device_tstamp",
      "refr_domain_userid",
      "refr_medium",
      "refr_source",
      "refr_term",
      "refr_urlfragment",
      "refr_urlhost",
      "refr_urlpath",
      "refr_urlport",
      "refr_urlquery",
      "refr_urlscheme",
      "page_referrer",
      "page_title",
      "page_url",
      "page_urlfragment",
      "page_urlhost",
      "page_urlpath",
      "page_urlport",
      "page_urlquery",
      "page_urlscheme",
      "name_tracker",
      "etl_tags",
      "etl_tstamp",
      "collector_tstamp",
      "v_collector",
      "v_etl",
      "v_tracker",
      "true_tstamp",
      "event_format"
    )
    val pagePingUnstructFields = List(
      "pp_xoffset_max",
      "pp_xoffset_min",
      "pp_yoffset_max",
      "pp_yoffset_min"
    )

    val structuredUnstructFields = List("se_action", "se_category", "se_label", "se_property", "se_value")

    val transactionItemsContextsFields =
      List("ti_category", "ti_currency", "ti_name", "ti_orderid", "ti_price", "ti_price_base", "ti_quantity", "ti_sku")

    val trasactionUnstuct = List(
      "base_currency",
      "tr_affiliation",
      "tr_city",
      "tr_country",
      "tr_currency",
      "tr_orderid",
      "tr_shipping",
      "tr_shipping_base",
      "tr_state",
      "tr_tax",
      "tr_tax_base",
      "tr_total",
      "tr_total_base"
    )

    val importantFieldsXPath = List(
      "user_id",
      "domain_userid",
      "domain_sessionid",
      "contexts_com_globeandmail_content_1[0].contentId",               //camelCase: coming from contexts schema
      "contexts_com_globeandmail_device_detector_1[0].visitorPlatform", //camelCase: coming from contexts schema
      "refr_medium",
      "page_urlpath",
      "page_urlhost",
      "contexts_com_globeandmail_environment_1[0].environment"
    )

    val extraContexts = JObject(
      List(
        JField(
          "com_globeandmail_txn_item_1",
          JArray(
            List(
              JObject(
                transactionItemsContextsFields
                  .map(field => JField(snakeToCamel(field), json \ field))
                  .filter(_._2 != JNull)
              )
            )
          )
        ),
        JField(
          "com_snowplow_core_1",
          JArray(
            List(
              JObject(
                snowplowContext.map(field => JField(snakeToCamel(field), json \ field)).filter(_._2 != JNull)
              )
            )
          )
        )
      )
    )
    // building the core of events table
    val filtered = json transform {
      case JObject(l) =>
        JObject(
          l.filter(
            kv =>
              !(
                deprecatedFields ++
                  snowplowContext ++
                  pagePingUnstructFields ++
                  structuredUnstructFields ++
                  transactionItemsContextsFields ++
                  trasactionUnstuct
              ).contains(kv._1)
                && !kv._1.startsWith("unstruct_")
                && !kv._1.startsWith("contexts_")
          )
        )
    }
    // building contexts cell
    val embeddedContexts = JObject(json filterField {
      case JField(name, _) =>
        name.startsWith("contexts_")
      case _ => false
    })

    val contexts = extraContexts merge embeddedContexts transformField {
      case JField(name, value) => (name.stripPrefix("contexts_"), value)
    } transform { // remove empty ones
      case JObject(l) => JObject(l.filter(kv => kv._2 != JArray(List(JObject(List())))))
    }

    // building unstruct cell
    val eventUnstructData = json filterField {
      case (name, _) =>
        name.startsWith("unstruct_") && name != "unstruct_event"
    }
    val unstructCell = if (eventUnstructData.nonEmpty) {
      JObject(List(JField("unstruct", eventUnstructData.head._2)))
    } else {
      JObject(
        List(
          JField(
            "unstruct",
            JObject(
              (pagePingUnstructFields ++ trasactionUnstuct ++ structuredUnstructFields)
                .map(field => JField(snakeToCamel(field), json \ field))
                .filter(_._2 != JNull)
            )
          )
        )
      )
    }

    // mixing all
    val result = filtered merge JObject(JField("contexts", contexts)) merge
      JObject(
        importantFieldsXPath
          .map(
            xpath => JField(camelToSnake(fieldFromXPath(xpath)), JString(extractValueFromXPath[String](json, xpath)))
          )
          .filter(_._2 != JObject(List()))
      )
    // adding unstruct data if exists
    if (unstructCell != JObject(List(JField("unstruct", JObject(List()))))) {
      result merge unstructCell
    } else {
      result
    }
  }
  protected[utils] def fieldFromXPath(xpath: String) = {
    val matcher = xpathPattern.matcher(xpath.split('.').last)
    if (matcher.find()) {
      matcher.group(1)
    } else {
      throw new RuntimeException(s"cannot find fieldName from xpath $xpath")
    }
  }
  protected[utils] def extractValueFromXPath[T](
    json: JValue,
    xpath: String
  )(implicit formats: Formats, mf: Manifest[T]) =
    xpath
      .split('.')
      .foldLeft(json)(
        {
          case (JNull, _) => JNull
          case (acc: JValue, node: String) =>
            val matcher = xpathPattern.matcher(node)
            if (matcher.find()) {
              val fieldName  = matcher.group(1)
              val fieldIndex = matcher.group(2)
              if (fieldIndex == "") {
                acc \ fieldName
              } else {
                (acc \ fieldName)(fieldIndex.toInt)
              }
            } else {
              JNull
            }
        }
      ) match {
      case JNothing    => JNull.extract[T]
      case acc: JValue => acc.extract[T]
    }
}
