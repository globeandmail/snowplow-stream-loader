/**
  * Copyright (c) 2014-2017 Snowplow Analytics Ltd. All rights reserved.
  *
  * This program is licensed to you under the Apache License Version 2.0,
  * and you may not use this file except in compliance with the Apache License Version 2.0.
  * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the Apache License Version 2.0 is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
  */

package utils

import org.json4s._
import org.json4s.jackson.JsonMethods._

// specs2
import org.specs2.mutable.Specification

class JsonUtilsSpec extends Specification {
  implicit val formats = DefaultFormats
  val json = parse("""
         {
           "name": "joe",
           "birthday":{
            "year": 1991,
            "month": "january",
            "day": 22
           },
           "children": [
           {"name":"Max"},
           {"name":"John"}
           ],
           "addresses": {
             "address1": {
               "street": "Bulevard",
               "city": "Helsinki"
             },
             "address2": {
               "street": "Soho",
               "city": "London"
             }
           }
         }""")
  "JsonUtils" should {
    "have fieldFromXPath working " in {
      JsonUtils.fieldFromXPath("birthday") must_== "birthday"
      JsonUtils.fieldFromXPath("birthday.day") must_== "day"
      JsonUtils.fieldFromXPath("children[0].name") must_== "name"
      JsonUtils.fieldFromXPath("children[0].name[1]") must_== "name"
      JsonUtils.fieldFromXPath("children[2]") must_== "children"
      JsonUtils.fieldFromXPath("addresses.address1.street") must_== "street"
    }
    "have extractValueFromXPath working with no array" in {
      JsonUtils.extractValueFromXPath[String](json, "name") must_== "joe"
      JsonUtils.extractValueFromXPath[String](json, "birthday.year") must_== "1991"
      JsonUtils.extractValueFromXPath[Int](json, "birthday.year") must_== 1991
      JsonUtils.extractValueFromXPath[String](json, "addresses.address2.street") must_== "Soho"
    }
    "have extractValueFromXPath working with array elements" in {
      JsonUtils.extractValueFromXPath[String](json, "children[0].name") must_== "Max"
      JsonUtils.extractValueFromXPath[String](json, "children[1].name") must_== "John"
    }
    "have extractValueFromXPath should return null when non-existing field" in {
      JsonUtils.extractValueFromXPath[String](json, "nonexistingarray[0].name") must_== null
      JsonUtils.extractValueFromXPath[String](json, "children[1].nonexistinglevel2") must_== null
      JsonUtils.extractValueFromXPath[String](json, "addresses.nonexistinglevel2") must_== null
      JsonUtils.extractValueFromXPath[String](json, "nonexistinglevel1") must_== null
    }
  }
}
