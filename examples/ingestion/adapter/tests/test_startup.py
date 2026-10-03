from __future__ import annotations

import base64
import json
import os
import shutil
import stat
import subprocess
from pathlib import Path

EXAMPLE = Path(__file__).resolve().parents[2]
NAMES = (
    "INGEST_ENGINE_API_KEY",
    "INGEST_CDC_WEBHOOK_SECRET",
    "INGEST_WEBHOOK_SECRET_BILLING",
    "INGEST_POSTGRES_PASSWORD",
)


def test_startup_generates_private_credentials_and_loopback_ports(tmp_path: Path) -> None:
    docker = shutil.which("docker")
    assert docker is not None, "Docker Compose is required for the configuration regression"
    shutil.copy(EXAMPLE / "demo.sh", tmp_path)
    shutil.copy(EXAMPLE / "docker-compose.yml", tmp_path)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "docker"
    stub.write_text("#!/usr/bin/env bash\nexit 0\n")
    stub.chmod(0o700)
    env = {k: v for k, v in os.environ.items() if k not in NAMES}
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    previous: dict[str, str] = {}
    for _ in range(2):
        subprocess.run(["bash", "demo.sh"], cwd=tmp_path, env=env, check=True)
        credentials = tmp_path / ".env"
        assert stat.S_IMODE(credentials.stat().st_mode) == 0o600
        values = dict(line.split("=", 1) for line in credentials.read_text().splitlines())
        assert set(values) == set(NAMES)
        for name, value in values.items():
            assert value != previous.get(name)
            if "SECRET" in name:
                assert value.startswith("whsec_")
                assert len(base64.b64decode(value[6:], validate=True)) == 32
            else:
                assert len(bytes.fromhex(value)) == 32
        normalized = subprocess.check_output(
            [docker, "compose", "--profile", "demo", "config", "--format", "json"],
            cwd=tmp_path,
            env=env,
            text=True,
        )
        services = json.loads(normalized)["services"]
        ports = [port for service in services.values() for port in service.get("ports", [])]
        assert len(ports) == 2
        assert all(port["host_ip"] == "127.0.0.1" for port in ports)
        engine_key = services["inputlayer"]["environment"]["INPUTLAYER_BOOTSTRAP_API_KEY"]
        assert engine_key == values[NAMES[0]]
        for service in ("adapter", "demo"):
            settings = services[service]["environment"]
            assert settings["ENGINE_API_KEY"] == values[NAMES[0]]
            assert settings["CDC_WEBHOOK_SECRET"] == values[NAMES[1]]
            assert settings["WEBHOOK_SECRET_BILLING"] == values[NAMES[2]]
        assert services["debezium"]["environment"]["CDC_WEBHOOK_SECRET"] == values[NAMES[1]]
        for service in ("postgres", "debezium"):
            assert services[service]["environment"]["POSTGRES_PASSWORD"] == values[NAMES[3]]
        assert services["demo"]["environment"]["DATABASE_URL"] == (
            f"postgresql://postgres:{values[NAMES[3]]}@postgres:5432/shop"
        )
        previous = values
