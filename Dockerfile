FROM harbor.sophi.io/sophi/scala-sbt:2.12.8_1.2.7 as build

# ARGS
ARG PROTOCOL
ARG LOADER_VERSION=0.10.1
ARG SNOWPLOW_COMPONENT="stream-loader"
ARG QUEUE="kafka"
ARG WORK_DIR="/snowplow"


LABEL Description="Snowplow ${SNOWPLOW_COMPONENT}" \
      Version=${LOADER_VERSION} \
      Maintainer="Saeed Zareian <szareian@globeandmail.com>"


# compile snowplow components
# https://github.com/snowplow/snowplow-elasticsearch-loader/archive/0.10.1.tar.gz
#RUN mkdir -p ${WORK_DIR} && \
#    wget https://github.com/snowplow/snowplow-elasticsearch-loader/archive/${LOADER_VERSION}.tar.gz  -P ${WORK_DIR} && \
#    tar -C ${WORK_DIR} -xvf /${WORK_DIR}/${LOADER_VERSION}.tar.gz

COPY code ${WORK_DIR}/snowplow-stream-loader-${LOADER_VERSION}

RUN mkdir -p ${WORK_DIR}/out/stream-loader && \
    cd ${WORK_DIR}/snowplow-stream-loader-${LOADER_VERSION}/ && \
    apk add --no-cache bash && \
    sbt "project ${PROTOCOL}" assembly

RUN mv ${WORK_DIR}/snowplow-stream-loader-${LOADER_VERSION}/${PROTOCOL}/target/scala-*/snowplow-stream-loader-${PROTOCOL}-*.jar ${WORK_DIR}/out/stream-loader/ && \
    mv ${WORK_DIR}/snowplow-stream-loader-${LOADER_VERSION}/examples/config.hocon.sample ${WORK_DIR}/out/stream-loader/stream-loader.conf


# ________________________________________________________ #
FROM openjdk:8-jre-alpine3.9
COPY --from=build ${WORK_DIR}/out /

ENV LOG_LEVEL="info"\
    GITHUB_TOKEN="XXXXX" \
    COLLECTOR_PORT="7070" \
    COLLECTOR_URI="snowplow-kafka-collector.pipeline:7070" \
    AWS_REGION="us-east-1" \
    AWS_SECRET_KEY="iam" \
    AWS_ACCESS_KEY="iam" \
    AWS_ARN_ROLE="" \
    AWS_ARN_STS_REGION="" \
    INPUT_QUEUE="enrichGood" \
    INPUT_QUEUE_TYPE="good" \
    OUTPUT_QUEUE="" \
    GOOD_SINK="postgres" \
    BAD_SINK="kafka" \
    QUEUE_PARTITION_KEY_NAME="network_userid" \
    QUEUE_INITIAL_POSITION="LATEST" \
    QUEUE_INITIAL_TIMESTAMP="2000-01-01T00:00:00Z" \
    LOADER_APP_NAME="loader" \
    LEASE_TABLE_INITIAL_RCU="1" \
    LEASE_TABLE_INITIAL_WCU="1" \
    ELASTICSEARCH_ENDPOINT="elasticsearch" \
    ELASTICSEARCH_PORT="9200" \
    ELASTICSEARCH_USERNAME="elasticsearch" \
    ELASTICSEARCH_PASSWORD="changeme" \
    ELASTICSEARCH_MAX_TIMEOUT="6000" \
    ELASTICSEARCH_MAX_RETRIES="10" \
    ELASTICSEARCH_SHARDS="10" \
    ELASTICSEARCH_REPLICAS="1" \
    ELASTICSEARCH_INDEX="events" \
    ELASTICSEARCH_DOCUMENT_TYPE="enriched" \
    ELASTICSEARCH_SCHEMA_FILE_PATH="mapping.json" \
    ELASTICSEARCH_SSL="false" \
    ELASTICSEARCH_SIGNING="false" \
    ELASTICSEARCH_CLUSTER_NAME="cluster" \
    POSTGRES_SERVER="" \
    POSTGRES_PORT="5432" \
    POSTGRES_DATABASE_NAME="postgres" \
    POSTGRES_USERNAME="postgres" \
    POSTGRES_PASSWORD="postgres" \
    POSTGRES_TABLE="atomic.events" \
    POSTGRES_SCHEMAS="schemas" \
    POSTGRES_SINK_APP_ID_FILTER="theglobeandmail-website" \
    SHARD_DATE_FIELD="derived_tstamp" \
    SHARD_DATE_FORMAT="yyyy_MM_dd" \
    BUCKET="tgam-sophi-data" \
    S3_ACCESS_KEY="" \
    S3_SECRET_KEY="" \
    KINESIS_ACCESS_KEY="" \
    KINESIS_SECRET_KEY="" \
    KINESIS_SINK_APP_ID_FILTER="" \
    KINESIS_ARN_ROLE="arn:aws:iam::690288799991:role\/sophi-kinesis-stream-loader" \
    KINESIS_ARN_STS_REGION="us-east-1" \
    LOADER_BUFFER_BYTE_THRESHOLD=10000 \
    LOADER_BUFFER_RECORD_THRESHOLD=100 \
    LOADER_BUFFER_TIME_THRESHOLD=1000 \
    QUEUE_MAX_RECORDS=1000 \
    DEDUPLICATION_ENABLED="false" \
    DEDUPLICATION_FIELD="event_fingerprint" \
    DEDUPLICATION_SIZE_LIMIT="1000" \
    DEDUPLICATION_TIME_LIMIT="3600" \
    CENTRAL_BRANCH=master \
    # to enable localstack set this to 1
    AWS_CBOR_DISABLE=0 \
    KAFKA_BOOTSTRAP_SERVER="kafka-cp-kafka-headless.kafka:9092" \
    KAFKA_GROUP_NAME="kafka-postgres-loader" \
    TOPIC_BAD_PRODUCER="badLoaderTopic" \

CMD cd /out/stream-loader && \
    if [ ! -d schemas ]; then \
        echo "grabbing $CENTRAL_BRANCH version of sophi/central" && \
        apk add --no-cache git && \
        rm -r -f repo && \
        git clone -c http.sslVerify=false  --single-branch -b ${CENTRAL_BRANCH} https://${GITHUB_TOKEN}@github.com/globeandmail/sophi3.git repo && \
        mv repo/central/pgsql/ schemas && \
        cp repo/central/es/mapping.json . && \
        rm -r -f repo && \
        apk del --purge git ; \
    fi && \
    sed -i "s/{{elasticsearchInputType}}/${QUEUE}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{elasticsearchGoodOutputDestination}}/${GOOD_SINK}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{elasticsearchBadOutputDestination}}/${BAD_SINK}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{kinesisInEnabledStreamType}}/${INPUT_QUEUE_TYPE}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{initialPosition}}/${QUEUE_INITIAL_POSITION}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{initialTimestamp}}/${QUEUE_INITIAL_TIMESTAMP}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{kinesisMaxRecords}}/${QUEUE_MAX_RECORDS}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{kinesisRegion}}/${AWS_REGION}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{kinesisAppName}}/${LOADER_APP_NAME}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{leastTableInitialRCU}}/${LEASE_TABLE_INITIAL_RCU}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{leastTableInitialWCU}}/${LEASE_TABLE_INITIAL_WCU}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{inStreamName}}/${INPUT_QUEUE}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{outStreamName}}/${OUTPUT_QUEUE}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{bufferByteThreshold}}/${LOADER_BUFFER_BYTE_THRESHOLD}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{bufferRecordThreshold}}/${LOADER_BUFFER_RECORD_THRESHOLD}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{bufferTimeThreshold}}/${LOADER_BUFFER_TIME_THRESHOLD}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{elasticsearchEndpoint}}/${ELASTICSEARCH_ENDPOINT}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{elasticsearchTransportPort}}/${ELASTICSEARCH_PORT}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{elasticsearchUsername}}/${ELASTICSEARCH_USERNAME}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{elasticsearchPassword}}/${ELASTICSEARCH_PASSWORD}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/username = \"\"//g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/password = \"\"//g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{sslBool}}/${ELASTICSEARCH_SSL}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{signingBool}}/${ELASTICSEARCH_SIGNING}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{elasticsearchClusterName}}/${ELASTICSEARCH_CLUSTER_NAME}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{elasticsearchIndex}}/${ELASTICSEARCH_INDEX}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{elasticsearchRegion}}/${AWS_REGION}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{elasticsearchMaxTimeout}}/${ELASTICSEARCH_MAX_TIMEOUT}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{elasticsearchMaxRetries}}/${ELASTICSEARCH_MAX_RETRIES}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{elasticsearchShards}}/${ELASTICSEARCH_SHARDS}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{elasticsearchReplicas}}/${ELASTICSEARCH_REPLICAS}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{elasticsearchDocumentType}}/${ELASTICSEARCH_DOCUMENT_TYPE}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{elasticsearchIndexDateField}}/${SHARD_DATE_FIELD}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{elasticsearchIndexDateFormat}}/${SHARD_DATE_FORMAT}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/indexDateField = \"\"//g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/indexDateFormat = \"\"//g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{elasticsearchSchemaFilePath}}/${ELASTICSEARCH_SCHEMA_FILE_PATH}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{postgresServer}}/${POSTGRES_SERVER}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{postgresPort}}/${POSTGRES_PORT}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{postgresDatabaseName}}/${POSTGRES_DATABASE_NAME}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{postgresUsername}}/${POSTGRES_USERNAME}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{postgresPassword}}/${POSTGRES_PASSWORD}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{postgresTable}}/${POSTGRES_TABLE}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{postgresSchemas}}/${POSTGRES_SCHEMAS}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{postgresFilterAppId}}/${POSTGRES_SINK_APP_ID_FILTER}/g" /out/stream-loader/stream-loader.conf && \
    sed -i "s/{{shardDateField}}/${SHARD_DATE_FIELD}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{shardDateFormat}}/${SHARD_DATE_FORMAT}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/shardTableDateField = \"\"//g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/shardTableDateFormat = \"\"//g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{bucketName}}/${BUCKET}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{S3AccessKey}}/${S3_ACCESS_KEY}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{S3SecretKey}}/${S3_SECRET_KEY}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{KinesisAccessKey}}/${KINESIS_ACCESS_KEY}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{KinesisSecretKey}}/${KINESIS_SECRET_KEY}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{FilterAppId}}/${KINESIS_SINK_APP_ID_FILTER}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{KinesisArnRole}}/${KINESIS_ARN_ROLE}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{KinesisStsRegion}}/${KINESIS_ARN_STS_REGION}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/# kinesisArnRole = \"${KINESIS_ARN_ROLE}\"/kinesisArnRole = \"${KINESIS_ARN_ROLE}\"/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/# kinesisStsRegion = \"${KINESIS_ARN_STS_REGION}\"/kinesisStsRegion = \"${KINESIS_ARN_STS_REGION}\"/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/kinesisArnRole = \"\"//g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/kinesisStsRegion = \"\"//g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{appName}}/${LOADER_APP_NAME}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{collectorUri}}/${COLLECTOR_URI}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{collectorPort}}/${COLLECTOR_PORT}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{appId}}/${LOADER_APP_NAME}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{method}}/GET/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/accessKey = iam/accessKey = ${AWS_ACCESS_KEY}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/secretKey = iam/secretKey =  ${AWS_SECRET_KEY}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/# arnRole = \"{{arnRole}}\"/arnRole = \"${AWS_ARN_ROLE}\"/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/# stsRegion = \"{{stsRegion}}\"/stsRegion = \"${AWS_ARN_STS_REGION}\"/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/arnRole = \"\"//g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/stsRegion = \"\"//g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{deduplicationEnabled}}/${DEDUPLICATION_ENABLED}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{deduplicationField}}/${DEDUPLICATION_FIELD}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{deduplicationSizeLimit}}/${DEDUPLICATION_SIZE_LIMIT}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s/{{deduplicationTimeLimit}}/${DEDUPLICATION_TIME_LIMIT}/g" /out/stream-loader/stream-loader.conf  && \
    sed -i "s;# customEndpoint = {{kinesisEndpoint}};customEndpoint = ${KINESIS_ENDPOINT};g" /out/stream-loader/stream-loader.conf && \
    sed -i "s;# dynamodbCustomEndpoint = \"http://localhost:4569\";dynamodbCustomEndpoint = ${DYNAMO_ENDPOINT};g" /out/stream-loader/stream-loader.conf && \
    sed -i "s/{{KAFKA_BOOTSTRAP_SERVER}}/${KAFKA_BOOTSTRAP_SERVER}/g" /out/stream-loader/stream-loader.conf && \
    sed -i "s/{{KAFKA_GROUP_NAME}}/${KAFKA_GROUP_NAME}/g" /out/stream-loader/stream-loader.conf && \
    sed -i "s/{{TOPIC_CONSUME}}/${INPUT_QUEUE}/g" /out/stream-loader/stream-loader.conf && \
    sed -i "s/{{TOPIC_BAD_PRODUCER}}/${TOPIC_BAD_PRODUCER}/g" /out/stream-loader/stream-loader.conf && \
    cat /out/stream-loader/stream-loader.conf && \
    cd /out/stream-loader && \
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=${LOG_LEVEL} \
    -jar snowplow-stream-loader-*.jar \
    --config stream-loader.conf
