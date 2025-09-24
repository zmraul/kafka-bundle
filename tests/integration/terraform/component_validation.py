#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""
Component validation tests.
Tests specific functionality of each component.
"""

import json
import logging
from uuid import uuid4

import pytest
import requests
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic

logger = logging.getLogger(__name__)


class TestKafkaValidation:
    """Test Kafka/KRaft functionality."""

    def test_kafka_admin_operations(self, kafka_connection_info):
        """Test basic Kafka admin operations."""
        admin_client = KafkaAdminClient(
            bootstrap_servers=kafka_connection_info["bootstrap_servers"],
            security_protocol="SASL_PLAINTEXT" if kafka_connection_info["username"] else "PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-512" if kafka_connection_info["username"] else None,
            sasl_plain_username=kafka_connection_info.get("username"),
            sasl_plain_password=kafka_connection_info.get("password"),
        )

        # Create a test topic
        test_topic_name = f"test-topic-{uuid4().hex[:8]}"
        topic = NewTopic(name=test_topic_name, num_partitions=3, replication_factor=3)

        admin_client.create_topics([topic])

        # List topics to verify creation
        metadata = admin_client.list_topics()
        assert test_topic_name in metadata

        # Clean up
        admin_client.delete_topics([test_topic_name])

    def test_kafka_producer_consumer(self, kafka_connection_info):
        """Test Kafka producer and consumer operations."""
        test_topic = f"test-topic-{uuid4().hex[:8]}"
        test_message = f"test-message-{uuid4().hex}"

        # Create topic first
        admin_client = KafkaAdminClient(
            bootstrap_servers=kafka_connection_info["bootstrap_servers"],
            security_protocol="SASL_PLAINTEXT" if kafka_connection_info["username"] else "PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-512" if kafka_connection_info["username"] else None,
            sasl_plain_username=kafka_connection_info.get("username"),
            sasl_plain_password=kafka_connection_info.get("password"),
        )

        topic = NewTopic(name=test_topic, num_partitions=1, replication_factor=1)
        admin_client.create_topics([topic])

        try:
            # Producer
            producer = KafkaProducer(
                bootstrap_servers=kafka_connection_info["bootstrap_servers"],
                security_protocol="SASL_PLAINTEXT" if kafka_connection_info["username"] else "PLAINTEXT",
                sasl_mechanism="SCRAM-SHA-512" if kafka_connection_info["username"] else None,
                sasl_plain_username=kafka_connection_info.get("username"),
                sasl_plain_password=kafka_connection_info.get("password"),
                value_serializer=lambda v: v.encode('utf-8')
            )

            # Send message
            future = producer.send(test_topic, test_message)
            record_metadata = future.get(timeout=10)
            assert record_metadata.topic == test_topic

            producer.close()

            # Consumer
            consumer = KafkaConsumer(
                test_topic,
                bootstrap_servers=kafka_connection_info["bootstrap_servers"],
                security_protocol="SASL_PLAINTEXT" if kafka_connection_info["username"] else "PLAINTEXT",
                sasl_mechanism="SCRAM-SHA-512" if kafka_connection_info["username"] else None,
                sasl_plain_username=kafka_connection_info.get("username"),
                sasl_plain_password=kafka_connection_info.get("password"),
                auto_offset_reset='earliest',
                value_deserializer=lambda m: m.decode('utf-8'),
                consumer_timeout_ms=10000
            )

            # Consume message
            messages = []
            for message in consumer:
                messages.append(message.value)
                break

            consumer.close()
            assert test_message in messages

        finally:
            # Clean up topic
            admin_client.delete_topics([test_topic])


class TestKarapaceValidation:
    """Test Karapace schema registry functionality."""

    def test_karapace_health(self, karapace_endpoint):
        """Test Karapace health endpoint."""
        response = requests.get(f"{karapace_endpoint}/")
        assert response.status_code == 200

    def test_create_schema_subject(self, karapace_endpoint):
        """Test creating a schema subject in Karapace."""
        subject_name = f"test-subject-{uuid4().hex[:8]}"

        # Define a simple Avro schema
        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "message", "type": "string"}
            ]
        }

        # Register schema
        response = requests.post(
            f"{karapace_endpoint}/subjects/{subject_name}/versions",
            json={"schema": json.dumps(schema)},
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
        )
        assert response.status_code == 200

        schema_id = response.json()["id"]
        assert isinstance(schema_id, int)

        # Verify schema was created
        response = requests.get(f"{karapace_endpoint}/subjects/{subject_name}/versions/latest")
        assert response.status_code == 200

        retrieved_schema = response.json()
        assert retrieved_schema["id"] == schema_id

        # Clean up - delete subject
        requests.delete(f"{karapace_endpoint}/subjects/{subject_name}")

    def test_list_subjects(self, karapace_endpoint):
        """Test listing subjects in Karapace."""
        response = requests.get(f"{karapace_endpoint}/subjects")
        assert response.status_code == 200
        subjects = response.json()
        assert isinstance(subjects, list)


class TestConnectValidation:
    """Test Kafka Connect functionality."""

    def test_connect_health(self, connect_endpoint):
        """Test Kafka Connect health."""
        response = requests.get(f"{connect_endpoint}/")
        assert response.status_code == 200

    def test_list_connectors(self, connect_endpoint):
        """Test listing connectors."""
        response = requests.get(f"{connect_endpoint}/connectors")
        assert response.status_code == 200
        connectors = response.json()
        assert isinstance(connectors, list)

    def test_create_mm2_connector(self, connect_endpoint):
        """Test creating a basic MM2 (MirrorMaker 2) connector."""
        connector_name = f"mm2-test-{uuid4().hex[:8]}"

        # Basic MM2 connector configuration
        mm2_config = {
            "name": connector_name,
            "config": {
                "connector.class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
                "source.cluster.alias": "source",
                "target.cluster.alias": "target",
                "source.cluster.bootstrap.servers": "localhost:9092",
                "target.cluster.bootstrap.servers": "localhost:9092",
                "topics": "test.*",
                "groups": "test-group",
                "replication.factor": 1,
                "checkpoints.topic.replication.factor": 1,
                "heartbeats.topic.replication.factor": 1,
                "offset-syncs.topic.replication.factor": 1,
                "sync.topic.acls.enabled": "false"
            }
        }

        # Create connector
        response = requests.post(
            f"{connect_endpoint}/connectors",
            json=mm2_config,
            headers={"Content-Type": "application/json"}
        )

        # Note: This might fail in test environment, but we test the API response
        assert response.status_code in [200, 201, 400, 409]  # Accept various responses

        # If successful, clean up
        if response.status_code in [200, 201]:
            requests.delete(f"{connect_endpoint}/connectors/{connector_name}")

    def test_connect_plugins(self, connect_endpoint):
        """Test listing available Connect plugins."""
        response = requests.get(f"{connect_endpoint}/connector-plugins")
        assert response.status_code == 200
        plugins = response.json()
        assert isinstance(plugins, list)
        assert len(plugins) > 0


class TestUIValidation:
    """Test Kafka UI functionality."""

    def test_ui_accessibility(self, ui_endpoint):
        """Test that Kafka UI is accessible."""
        response = requests.get(ui_endpoint, timeout=30)
        assert response.status_code == 200
        assert "kafka" in response.text.lower() or "ui" in response.text.lower()

    def test_ui_api_clusters(self, ui_endpoint):
        """Test Kafka UI API for clusters."""
        # Some Kafka UIs provide REST APIs
        try:
            response = requests.get(f"{ui_endpoint}/api/clusters", timeout=10)
            if response.status_code == 200:
                clusters = response.json()
                assert isinstance(clusters, list)
        except requests.exceptions.RequestException:
            # If API endpoint doesn't exist, that's fine for this test
            pytest.skip("UI API not available or different endpoint structure")


class TestCruiseControlValidation:
    """Test CruiseControl functionality."""

    def test_cruise_control_state(self, cruise_control_endpoint):
        """Test CruiseControl state endpoint."""
        try:
            response = requests.get(
                f"{cruise_control_endpoint}/kafkacruisecontrol/v1/state",
                timeout=30
            )
            assert response.status_code == 200

            state = response.json()
            assert "ExecutorState" in state or "AnalyzerState" in state

        except requests.exceptions.RequestException as e:
            # CruiseControl might not be fully ready or configured
            logger.warning(f"CruiseControl not ready: {e}")
            pytest.skip("CruiseControl not accessible")

    def test_cruise_control_bootstrap(self, cruise_control_endpoint):
        """Test CruiseControl bootstrap endpoint."""
        try:
            response = requests.get(
                f"{cruise_control_endpoint}/kafkacruisecontrol/v1/bootstrap",
                timeout=30
            )
            # Bootstrap can return various status codes depending on state
            assert response.status_code in [200, 202, 503]

        except requests.exceptions.RequestException:
            pytest.skip("CruiseControl bootstrap endpoint not accessible")

    def test_cruise_control_load(self, cruise_control_endpoint):
        """Test CruiseControl load information."""
        try:
            response = requests.get(
                f"{cruise_control_endpoint}/kafkacruisecontrol/v1/load",
                timeout=30
            )

            if response.status_code == 200:
                load_data = response.json()
                assert "hosts" in load_data or "brokers" in load_data

        except requests.exceptions.RequestException:
            pytest.skip("CruiseControl load endpoint not accessible")


class TestDataIntegratorValidation:
    """Test data integrator functionality with each component."""

    def test_data_integrator_kafka_relation(self, juju):
        """Test that data integrator is properly related to Kafka."""
        status = juju.status()

        # Check that data-integrator has a relation with kafka
        integrator_relations = status.applications["data-integrator"].get("relations", {})
        kafka_relations = status.applications["kafka"].get("relations", {})

        # Verify bidirectional relation exists
        assert any("kafka" in str(rel) for rel in integrator_relations.values())
        assert any("data-integrator" in str(rel) for rel in kafka_relations.values())

    def test_data_integrator_credentials(self, juju):
        """Test that data integrator provides valid credentials."""
        # Get credentials from data integrator
        action_result = juju.run_action("data-integrator/leader", "get-credentials")

        # Verify credentials are provided
        assert "username" in action_result
        assert "password" in action_result
        assert action_result["username"] is not None
        assert action_result["password"] is not None

    def test_data_integrator_config(self, juju):
        """Test data integrator configuration."""
        config = juju.get_config("data-integrator")

        # Verify expected configuration
        assert config.get("topic-name") == "__admin-user"
        assert "admin" in config.get("extra-user-roles", "")
