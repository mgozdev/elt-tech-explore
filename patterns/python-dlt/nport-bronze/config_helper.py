"""
Configuration Helper for dlt Pipelines
======================================

Utilities for managing pipeline destination configuration.
"""

import sys
from pathlib import Path
import toml


class ConfigManager:
    """Manage dlt pipeline configuration."""

    def __init__(self, config_dir: Path = None):
        self.config_dir = config_dir or Path(__file__).parent / ".dlt"
        self.config_file = self.config_dir / "config.toml"
        self.secrets_file = self.config_dir / "secrets.toml"

    def get_config(self):
        """Load configuration from config.toml."""
        if not self.config_file.exists():
            return {}
        return toml.load(self.config_file)

    def get_secrets(self):
        """Load secrets from secrets.toml."""
        if not self.secrets_file.exists():
            return {}
        return toml.load(self.secrets_file)

    def get_active_destination(self):
        """Get the currently active destination."""
        config = self.get_config()
        return config.get("destination", {}).get("active", "duckdb")

    def get_destination_config(self, destination: str = None):
        """
        Get configuration for a specific destination.

        Returns dict with:
        - name: destination name
        - dataset: dataset name
        - config: destination-specific config
        """
        destination = destination or self.get_active_destination()
        config = self.get_config()
        dest_config = config.get("destination", {}).get(destination, {})

        return {
            "name": destination,
            "dataset": dest_config.get("dataset", "nport_bronze"),
            "config": dest_config.get("config", {})
        }

    def validate_databricks_credentials(self):
        """Validate Databricks credentials are present."""
        secrets = self.get_secrets()
        databricks = secrets.get("destination", {}).get("databricks", {}).get("credentials", {})

        required_fields = ["server_hostname", "http_path", "access_token"]
        missing = [f for f in required_fields if not databricks.get(f)]

        if missing:
            return False, f"Missing Databricks credentials: {', '.join(missing)}"

        # Check for placeholder values
        if databricks.get("access_token") == "<your-databricks-token>":
            return False, "Databricks access token not configured (still has placeholder value)"

        return True, "All Databricks credentials present"

    def show_config(self):
        """Display current configuration."""
        active = self.get_active_destination()
        dest_config = self.get_destination_config()

        print("\n" + "="*60)
        print("CURRENT dlt CONFIGURATION")
        print("="*60)
        print(f"\nActive Destination: {active}")
        print(f"Dataset Name: {dest_config['dataset']}")

        if active == "databricks":
            valid, msg = self.validate_databricks_credentials()
            print(f"\nDatabricks Credentials: {msg}")

            if valid:
                secrets = self.get_secrets()
                db_creds = secrets["destination"]["databricks"]["credentials"]
                print(f"\n  Server: {db_creds.get('server_hostname', 'NOT SET')}")
                print(f"  HTTP Path: {db_creds.get('http_path', 'NOT SET')}")
                print(f"  Catalog: {db_creds.get('catalog', dest_config['config'].get('catalog', 'main'))}")
                print(f"  Access Token: {'*' * 20} (hidden)")
        elif active == "duckdb":
            print("\nDuckDB Configuration: File-based (no credentials needed)")

        print("\n" + "="*60)

    def set_destination(self, destination: str):
        """Set the active destination."""
        if destination not in ["duckdb", "databricks"]:
            print(f"Error: Unknown destination '{destination}'")
            print("Valid options: duckdb, databricks")
            return False

        config = self.get_config()
        if "destination" not in config:
            config["destination"] = {}

        config["destination"]["active"] = destination

        with open(self.config_file, 'w') as f:
            toml.dump(config, f)

        print(f"\n[+] Active destination set to: {destination}")

        if destination == "databricks":
            valid, msg = self.validate_databricks_credentials()
            if not valid:
                print(f"\n[!] Warning: {msg}")
                print(f"[!] Please configure credentials in: {self.secrets_file}")
                print(f"[!] See .dlt/secrets.toml.example for template")

        return True


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Manage dlt pipeline configuration")
    parser.add_argument("--show", action="store_true", help="Show current configuration")
    parser.add_argument("--set-destination", choices=["duckdb", "databricks"],
                        help="Set active destination")
    parser.add_argument("--validate", action="store_true",
                        help="Validate credentials for active destination")

    args = parser.parse_args()

    manager = ConfigManager()

    if args.show or not any(vars(args).values()):
        manager.show_config()

    if args.set_destination:
        manager.set_destination(args.set_destination)

    if args.validate:
        active = manager.get_active_destination()
        if active == "databricks":
            valid, msg = manager.validate_databricks_credentials()
            print(f"\nValidation: {msg}")
            sys.exit(0 if valid else 1)
        else:
            print(f"\n{active} does not require credential validation")


if __name__ == "__main__":
    main()
