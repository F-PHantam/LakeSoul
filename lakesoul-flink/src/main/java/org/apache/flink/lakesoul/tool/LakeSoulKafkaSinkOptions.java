/*
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.flink.lakesoul.tool;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class LakeSoulKafkaSinkOptions extends LakeSoulSinkOptions{

    public static final ConfigOption<String> BOOTSTRAP_SERVERS = ConfigOptions
            .key("bootstrap_servers")
            .stringType()
            .noDefaultValue()
            .withDescription("source kafka bootstrap servers");

    public static final ConfigOption<String> GROUP_ID = ConfigOptions
            .key("group_id")
            .stringType()
            .noDefaultValue()
            .withDescription("source kafka group ID");

    public static final ConfigOption<String> TOPIC = ConfigOptions
            .key("topic")
            .stringType()
            .noDefaultValue()
            .withDescription("source from topic");

    public static final ConfigOption<String> START_OFFSET = ConfigOptions
            .key("auto_offset_reset")
            .stringType()
            .defaultValue("latest")
            .withDescription("source consume from the offset");

    public static final ConfigOption<String> START_TIMESTAMP = ConfigOptions
            .key("start_timestamp")
            .stringType()
            .noDefaultValue()
            .withDescription("source consume from the timestamp");

    public static final ConfigOption<Integer> MAX_POLL_RECORDS = ConfigOptions
            .key("max_poll_records")
            .intType()
            .defaultValue(500)
            .withDescription("source poll max records");

    public static final ConfigOption<String> SECURITY_PROTOCOL = ConfigOptions
            .key("security_protocol")
            .stringType()
            .noDefaultValue()
            .withDescription("security protocol when connect kafka,includes SASL_PLAINTEXT, SSL and SASL_SSL");

    public static final ConfigOption<String> SASL_MECHANISM = ConfigOptions
            .key("sasl_mechanism")
            .stringType()
            .noDefaultValue()
            .withDescription("Used to specify the security protocol used when connecting to the Kafka cluster,which includes SCRAM-SHA-256 and PLAIN");

    public static final ConfigOption<String> SASL_JAAS_CONFIG = ConfigOptions
            .key("sasl_jaas_config")
            .stringType()
            .noDefaultValue()
            .withDescription("Set up the JAAS configuration, including information such as the user's username and password");

    public static final ConfigOption<String> SSL_TRUSTSTORE_LOCATION = ConfigOptions
            .key("ssl_truststore_location")
            .stringType()
            .noDefaultValue()
            .withDescription("Configure the path of truststore (CA) provided by the server");

    public static final ConfigOption<String> SSL_TRUSTSTORE_PASSWORD = ConfigOptions
            .key("ssl_truststore_password")
            .stringType()
            .noDefaultValue()
            .withDescription("Configure the passwd of truststore (CA) provided by the server");

    public static final ConfigOption<String> SSL_KEYSTORE_LOCATION = ConfigOptions
            .key("ssl_keystore_location")
            .stringType()
            .noDefaultValue()
            .withDeprecatedKeys("Configure the path of keystore (private key) if client authentication is required");

    public static final ConfigOption<String> SSL_KEYSTORE_PASSWORD = ConfigOptions
            .key("ssl_keystore_password")
            .stringType()
            .noDefaultValue()
            .withDescription("Configure the passwd of keystore (private key) if client authentication is required");

}