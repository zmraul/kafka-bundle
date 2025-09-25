#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Terraform deployment helpers for integration tests."""

import logging
import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
import jubilant


logger = logging.getLogger(__name__)

def all_active_idle(status: jubilant.Status, *apps: str):
    """Helper function for jubilant all units active|idle checks."""
    return jubilant.all_agents_idle(status, *apps) and jubilant.all_active(status, *apps)


class TerraformDeployer:
    """Helper class to manage Terraform deployments for testing."""

    def __init__(self, model_name: str, terraform_dir: str = "terraform"):
        self.model_name = model_name
        self.terraform_dir = Path(terraform_dir).resolve()
        self.tfvars_file = None

    def create_tfvars(self, config: Dict[str, Any]) -> str:
        """Create a .tfvars.json file with the given configuration."""
        self.tfvars_file = tempfile.NamedTemporaryFile(
            mode='w', suffix='.tfvars.json', delete=False
        )

        # Always include model
        config["model"] = self.model_name

        # Write JSON content
        json.dump(config, self.tfvars_file, indent=2)

        self.tfvars_file.close()
        return self.tfvars_file.name

    def get_controller_credentials(self) -> Dict[str, str]:
        """Get Juju controller credentials for Terraform."""
        controller_credentials = yaml.safe_load(
            subprocess.check_output(
                "juju show-controller --show-password",
                stderr=subprocess.PIPE,
                shell=True,
                universal_newlines=True,
            )
        )

        def get_value(obj: dict, key: str):
            """Recursively gets value for given key in nested dict."""
            if key in obj:
                return obj.get(key, "")
            for _, v in obj.items():
                if isinstance(v, dict):
                    item = get_value(v, key)
                    if item is not None:
                        return item

        username = get_value(obj=controller_credentials, key="user")
        password = get_value(obj=controller_credentials, key="password")
        controller_addresses = ",".join(get_value(obj=controller_credentials, key="api-endpoints"))
        ca_cert = get_value(obj=controller_credentials, key="ca-cert")

        return {
            "JUJU_USERNAME": username,
            "JUJU_PASSWORD": password,
            "JUJU_CONTROLLER_ADDRESSES": controller_addresses,
            "JUJU_CA_CERT": ca_cert,
        }

    def terraform_init(self):
        """Initialize Terraform in the terraform directory."""
        result = subprocess.run(
            ["terraform", "init"],
            cwd=self.terraform_dir,
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            raise RuntimeError(f"Terraform init failed: {result.stderr}")

        logger.info(f"\n\nTerraform initialized:\n\n{result.stdout}")

    def terraform_plan(self, tfvars_file: str) -> str:
        """Run terraform plan and return the output."""
        env = self.get_controller_credentials()
        result = subprocess.run(
            ["terraform", "plan", f"-var-file={tfvars_file}"],
            cwd=self.terraform_dir,
            capture_output=True,
            text=True,
            env={**env, **dict(subprocess.os.environ)}
        )
        if result.returncode != 0:
            raise RuntimeError(f"Terraform plan failed: {result.stderr}")

        logger.info(f"\n\nTerraform plan output:\n\n{result.stdout}")
        return result.stdout

    def terraform_apply(self, tfvars_file: str):
        """Apply Terraform configuration."""
        env = self.get_controller_credentials()
        result = subprocess.run(
            ["terraform", "apply", "-auto-approve", f"-var-file={tfvars_file}"],
            cwd=self.terraform_dir,
            capture_output=True,
            text=True,
            env={**env, **dict(subprocess.os.environ)}
        )
        if result.returncode != 0:
            raise RuntimeError(f"Terraform apply failed: {result.stderr}")

        logger.info(f"\n\nTerraform applied:\n\n{result.stdout}")

    def terraform_destroy(self, tfvars_file: Optional[str] = None):
        """Destroy Terraform-managed resources."""
        env = self.get_controller_credentials()
        cmd = ["terraform", "destroy", "-auto-approve"]
        if tfvars_file:
            cmd.append(f"-var-file={tfvars_file}")

        result = subprocess.run(
            cmd,
            cwd=self.terraform_dir,
            capture_output=True,
            text=True,
            env={**env, **dict(subprocess.os.environ)}
        )
        if result.returncode != 0:
            raise RuntimeError(f"Terraform destroy failed: {result.stderr}")

    def cleanup(self):
        """Clean up temporary files."""
        if self.tfvars_file and Path(self.tfvars_file.name).exists():
            Path(self.tfvars_file.name).unlink()

        # Clean up terraform artifacts
        shutil.rmtree(self.terraform_dir / ".terraform", ignore_errors=True)
        for pattern in [".terraform.lock.hcl", "terraform.tfstate*", "*.tfplan"]:
            for file_path in self.terraform_dir.glob(pattern):
                file_path.unlink(missing_ok=True)


def get_single_mode_config(enable_cruise_control: bool = False) -> Dict[str, Any]:
    """Get Terraform configuration for single-mode deployment."""
    config = {
        "profile": "testing",
        "kafka": {
            "units": 3,
            "deployment_mode": "single",  # Explicitly set single mode
        },
        "connect": {"units": 1},
        "karapace": {"units": 1},
        "ui": {"units": 1},
        "integrator": {"units": 1},
    }

    if enable_cruise_control:
        # Add balancer role while preserving existing roles
        config["kafka"]["config"] = {"roles": "broker,balancer"}

    return config


def get_multi_app_config(enable_cruise_control: bool = False) -> Dict[str, Any]:
    """Get Terraform configuration for multi-app (split) mode deployment."""
    config = {
        "profile": "testing",
        "kafka": {
            "units": 3,
            "controller_units": 3,
            "deployment_mode": "split",
        },
        "connect": {"units": 1},
        "karapace": {"units": 1},
        "ui": {"units": 1},
        "integrator": {"units": 1},
    }

    if enable_cruise_control:
        # TODO: Split mode + cruise control limitation
        # In split mode, the Kafka module hardcodes roles to "broker" and "controller"
        # It doesn't preserve the "balancer" role for the broker application, this will
        # be updated with a config option instead of a role.
        pass

    return config


def enable_tls_config(base_config: Dict[str, Any], model_name: str) -> Dict[str, Any]:
    """Modify configuration to enable TLS across all components."""
    tls_config = base_config.copy()

    # For TLS testing, we'll need to add TLS offer
    # This would typically come from a TLS certificates operator
    # placeholder that the test can set up
    tls_config["tls_offer"] = f"admin/{model_name}.self-signed-certificates"

    return tls_config
