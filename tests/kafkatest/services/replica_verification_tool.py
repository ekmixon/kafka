# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.services.background_thread import BackgroundThreadService

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.security.security_config import SecurityConfig

import re


class ReplicaVerificationTool(KafkaPathResolverMixin, BackgroundThreadService):

    logs = {
        "producer_log": {
            "path": "/mnt/replica_verification_tool.log",
            "collect_default": False}
    }

    def __init__(self, context, num_nodes, kafka, topic, report_interval_ms, security_protocol="PLAINTEXT",
                 stop_timeout_sec=30, tls_version=None):
        super(ReplicaVerificationTool, self).__init__(context, num_nodes)

        self.kafka = kafka
        self.topic = topic
        self.report_interval_ms = report_interval_ms
        self.security_protocol = security_protocol
        self.tls_version = tls_version
        self.security_config = SecurityConfig(self.context, security_protocol, tls_version=tls_version)
        self.partition_lag = {}
        self.stop_timeout_sec = stop_timeout_sec

    def _worker(self, idx, node):
        cmd = self.start_cmd(node)
        self.logger.debug("ReplicaVerificationTool %d command: %s" % (idx, cmd))
        self.security_config.setup_node(node)
        for line in node.account.ssh_capture(cmd):
            self.logger.debug(f"Parsing line:{line}")

            if parsed := re.search(
                '.*max lag is (.+?) for partition ([a-zA-Z0-9._-]+-[0-9]+) at',
                line,
            ):
                lag = int(parsed[1])
                topic_partition = parsed[2]
                self.logger.debug(f"Setting max lag for {topic_partition} as {lag}")
                self.partition_lag[topic_partition] = lag

    def get_lag_for_partition(self, topic, partition):
        """
        Get latest lag for given topic-partition

        Args:
            topic:          a topic
            partition:      a partition of the topic
        """
        topic_partition = f'{topic}-{str(partition)}'
        lag = self.partition_lag.get(topic_partition, -1)
        self.logger.debug(f"Returning lag for {topic_partition} as {lag}")

        return lag

    def start_cmd(self, node):
        cmd = self.path.script("kafka-run-class.sh", node)
        cmd += f" {self.java_class_name()}"
        cmd += f" --broker-list {self.kafka.bootstrap_servers(self.security_protocol)} --topic-white-list {self.topic} --time -2 --report-interval-ms {self.report_interval_ms}"


        cmd += " 2>> /mnt/replica_verification_tool.log | tee -a /mnt/replica_verification_tool.log &"
        return cmd

    def stop_node(self, node):
        node.account.kill_java_processes(self.java_class_name(), clean_shutdown=True,
                                         allow_fail=True)

        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert (
            stopped
        ), f"Node {str(node.account)}: did not stop within the specified timeout of {str(self.stop_timeout_sec)} seconds"

    def clean_node(self, node):
        node.account.kill_java_processes(self.java_class_name(), clean_shutdown=False,
                                         allow_fail=True)
        node.account.ssh("rm -rf /mnt/replica_verification_tool.log", allow_fail=False)

    def java_class_name(self):
        return "kafka.tools.ReplicaVerificationTool"
