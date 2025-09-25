#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""
Tests both single-mode and multi-app mode deployments with all components.
"""

import logging

import pytest
from jubilant import Juju

from tests.integration.terraform.terraform_helpers import (
    TerraformDeployer,
    all_active_idle,
    enable_tls_config,
    get_multi_app_config,
    get_single_mode_config,
)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def terraform_deployer(juju):
    """Terraform deployer instance."""
    model_name = juju.model
    return TerraformDeployer(model_name)


class TestSingleMode:
    """Test deployment in single mode."""

    @pytest.fixture(scope="class", autouse=True)
    def deploy_single_mode_cluster(self, terraform_deployer: TerraformDeployer, juju: Juju):
        """Deploy the cluster in single mode."""
        # Ensure cleanup of any previous state
        terraform_deployer.cleanup()

        config = get_single_mode_config(enable_cruise_control=True)
        tfvars_file = terraform_deployer.create_tfvars(config)

        try:
            terraform_deployer.terraform_init()
            terraform_deployer.terraform_apply(tfvars_file)

            # Wait for all applications to be active
            juju.wait(
                lambda status: all_active_idle(status, "kafka", "kafka-connect", "karapace", "kafka-ui", "data-integrator"),
                delay=5,
                successes=6,
                timeout=3600,
            )

            yield

        finally:
            # done by jubilant at the end of the tests
            # terraform_deployer.terraform_destroy(tfvars_file)
            terraform_deployer.cleanup()

    def test_kafka_deployment(self, juju: Juju):
        """Test that Kafka is deployed and active."""
        status = juju.status()
        assert "kafka" in status.applications
        assert status.applications["kafka"]["application-status"]["current"] == "active"


# class TestMultiAppMode:
#     """Test deployment in multi-app (split) mode."""

#     @pytest.fixture(scope="class", autouse=True)
#     def deploy_multi_app_cluster(self, terraform_deployer, juju):
#         """Deploy the cluster in multi-app mode."""
#         config = get_multi_app_config(enable_cruise_control=True)
#         tfvars_file = terraform_deployer.create_tfvars(config)

#         try:
#             terraform_deployer.terraform_init()
#             terraform_deployer.terraform_apply(tfvars_file)

#             # Wait for all applications to be active
#             juju.wait(all_active=True, timeout=1800)

#             yield

#         finally:
#             terraform_deployer.terraform_destroy(tfvars_file)
#             terraform_deployer.cleanup()

#     def test_split_mode_deployment(self, juju):
#         """Test that both broker and controller apps are deployed."""
#         status = juju.status()

#         # In split mode, we should have separate broker and controller apps
#         # The exact naming depends on the Kafka operator implementation
#         assert "kafka" in status.applications  # broker

#         # Check if there's a separate controller app or if it's handled differently
#         kafka_status = status.applications["kafka"]["application-status"]["current"]
#         assert kafka_status == "active"

#     def test_all_components_deployed_split_mode(self, juju):
#         """Test that all components are deployed and active in split mode."""
#         status = juju.status()

#         expected_apps = ["kafka", "kafka-connect", "karapace", "kafka-ui", "data-integrator"]

#         for app in expected_apps:
#             assert app in status.applications
#             assert status.applications[app]["application-status"]["current"] == "active"

#     def test_cruise_control_in_split_mode(self, juju):
#         """Test CruiseControl functionality in split mode."""
#         config = juju.get_config("kafka")
#         assert "balancer" in config.get("roles", "")


# class TestTLSToggle:
#     """Test enabling and disabling TLS across the cluster."""

#     @pytest.fixture(scope="class")
#     def base_cluster_config(self):
#         """Base cluster configuration without TLS."""
#         return get_single_mode_config(enable_cruise_control=True)

#     @pytest.fixture(scope="class", autouse=True)
#     def deploy_and_toggle_tls(self, terraform_deployer, juju, base_cluster_config):
#         """Deploy cluster without TLS, then enable TLS via Terraform apply."""
#         # First deploy without TLS
#         tfvars_file = terraform_deployer.create_tfvars(base_cluster_config)

#         try:
#             terraform_deployer.terraform_init()
#             terraform_deployer.terraform_apply(tfvars_file)
#             juju.wait(all_active=True, timeout=1800)

#             yield "no_tls"

#             # Deploy TLS certificates operator for TLS testing
#             juju.deploy("self-signed-certificates", config={"ca-common-name": "test-ca"})
#             juju.wait(all_active=True, timeout=600)

#             # Now enable TLS via Terraform apply
#             tls_config = enable_tls_config(base_cluster_config, model_name=juju.model)
#             tls_tfvars_file = terraform_deployer.create_tfvars(tls_config)

#             terraform_deployer.terraform_apply(tls_tfvars_file)
#             juju.wait(all_active=True, timeout=1800)

#             yield "with_tls"

#         finally:
#             terraform_deployer.terraform_destroy(tfvars_file)
#             terraform_deployer.cleanup()

#     def test_initial_deployment_no_tls(self, juju, deploy_and_toggle_tls):
#         """Test that initial deployment works without TLS."""
#         if deploy_and_toggle_tls != "no_tls":
#             pytest.skip("Not in no_tls phase")

#         status = juju.status()
#         assert "kafka" in status.applications
#         assert status.applications["kafka"]["application-status"]["current"] == "active"

#     def test_tls_enabled_successfully(self, juju, deploy_and_toggle_tls):
#         """Test that TLS is enabled successfully via Terraform apply."""
#         if deploy_and_toggle_tls != "with_tls":
#             pytest.skip("Not in with_tls phase")

#         status = juju.status()

#         # Check that TLS relations are established
#         # The exact check depends on how TLS is implemented
#         assert "tls-certificates-operator" in status.applications

#         # Verify all applications are still active after TLS enablement
#         expected_apps = ["kafka", "kafka-connect", "karapace", "kafka-ui", "data-integrator"]
#         for app in expected_apps:
#             assert status.applications[app]["application-status"]["current"] == "active"