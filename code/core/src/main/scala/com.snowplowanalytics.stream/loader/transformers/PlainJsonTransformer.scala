/*
 * © Copyright 2020 The Globe and Mail
 */
/**
 * Copyright (c) 2014-2017 Snowplow Analytics Ltd.
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
package transformers

// Java

import com.snowplowanalytics.stream.loader.transformers.JsonTransformer

/**
 * Class to convert plain JSON to EmitterInputs
 *
 * @param documentIndexOrPrefix the elasticsearch index name
 */
class PlainJsonTransformer(
  documentIndexOrPrefix: String,
  documentIndexSuffixField: Option[String],
  documentIndexSuffixFormat: Option[String],
  mappingTable: Option[Map[String, String]]
) extends JsonTransformer(
      documentIndexOrPrefix: String,
      documentIndexSuffixField: Option[String],
      documentIndexSuffixFormat: Option[String],
      mappingTable: Option[Map[String, String]]
    )
