-- Copyright (c) 2013-2015 Snowplow Analytics Ltd. All rights reserved.
--
-- This program is licensed to you under the Apache License Version 2.0,
-- and you may not use this file except in compliance with the Apache License Version 2.0.
-- You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the Apache License Version 2.0 is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
--
-- Version:     0.7.0
-- URL:         -
--
-- Authors:     Yali Sassoon, Alex Dean, Fred Blundun
-- Copyright:   Copyright (c) 2013-2015 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

CREATE SCHEMA IF NOT EXISTS "atomic";

CREATE TABLE IF NOT EXISTS "atomic"."events" (
  -- App
  "app_id" text,
  "platform" text,
  -- Date/time
  "event_id" uuid NOT NULL,
  -- Derived timestamp
  "derived_tstamp" timestamptz,
  -- Event schema
  "event_vendor" text,
  "event_name" text,
  "event_version" text,
  -- Event fingerprint
  "event_fingerprint" text,
  -- Important fields for data modelling
  "user_id" text,
  "domain_userid" text,
  "domain_sessionid" text,
  "content_id" text,
  "visitor_platform" text,
  "environment" text,
  "refr_medium" text,
  "page_urlpath" text,
  "page_urlhost" text,
  -- Other information about the event
  "contexts" jsonb,
  "unstruct" jsonb
) PARTITION BY RANGE (derived_tstamp)
WITH (OIDS=FALSE);

COMMENT ON TABLE "atomic"."events" IS '0.7.0';

CREATE INDEX IF NOT EXISTS events_derived_tstamp on atomic.events (derived_tstamp);
