#!/usr/bin/env python3
"""Exercise a packaged Gretty product without Gradle or a source checkout at runtime."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import zipfile


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def smoke(archive, root):
    with zipfile.ZipFile(archive) as package:
        package.extractall(root)
    product = root / "appliance"
    config = json.loads((product / "conf/server.json").read_text())
    assert {w["contextPath"] for w in config["webApps"]} == {
        "/mgmt", "/engine", "/etl", "/retrieval"
    }
    port = free_port()
    config["serverConfig"]["httpPort"] = port
    (product / "conf/server.json").write_text(json.dumps(config))
    xml = root / "appliances.xml"
    xml.write_text("<appliances><appliance><identity>standalone_smoke</identity>"
                   f"<cluster_inetport>127.0.0.1:{free_port()}</cluster_inetport>" +
                   "".join(f"<{c}_url>http://127.0.0.1:{port}/{c}/bpl</{c}_url>"
                           for c in ("mgmt", "engine", "etl", "retrieval")) +
                   f"<data_retrieval_url>http://127.0.0.1:{port}/retrieval</data_retrieval_url>"
                   "</appliance></appliances>")
    env = os.environ.copy()
    # Do not let the developer's production configuration leak into this isolated test.
    for key in list(env):
        if key.startswith("ARCHAPPL_"):
            del env[key]
    env.update(ARCHAPPL_APPLIANCES=str(xml), ARCHAPPL_MYIDENTITY="standalone_smoke",
               ARCHAPPL_PERSISTENCE_LAYER="org.epics.archiverappliance.config.persistence.JDBM2Persistence",
               ARCHAPPL_PERSISTENCE_LAYER_JDBM2FILENAME=str(root / "config.jdbm2"))
    for tier in ("SHORT", "MEDIUM", "LONG"):
        env[f"ARCHAPPL_{tier}_TERM_FOLDER"] = str(root / tier.lower())
    missing = subprocess.run(["bash", str(product / "appliance.sh"), "run"],
                             env={k: v for k, v in env.items() if k != "ARCHAPPL_APPLIANCES"},
                             capture_output=True)
    assert missing.returncode != 0 and b"ARCHAPPL_APPLIANCES" in missing.stderr
    for cycle in range(2):
        log = root / f"run-{cycle}.log"
        with log.open("w") as output:
            proc = subprocess.Popen(["bash", str(product / "appliance.sh"), "run"],
                                    cwd=root, env=env, stdin=subprocess.PIPE,
                                    stdout=output, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 180
                while time.monotonic() < deadline:
                    if proc.poll() is not None:
                        raise RuntimeError(f"Launcher exited: {proc.returncode}")
                    try:
                        states = []
                        for component in ("mgmt", "engine", "etl", "retrieval"):
                            with urllib.request.urlopen(
                                    f"http://127.0.0.1:{port}/{component}/bpl/startupState", timeout=3) as response:
                                states.append(json.load(response)["status"])
                        if all(s == "STARTUP_COMPLETE" for s in states):
                            break
                    except (OSError, ValueError, KeyError):
                        pass
                    time.sleep(2)
                else:
                    raise RuntimeError("Timed out waiting for all four components")
            finally:
                if proc.poll() is None:
                    # Gretty's foreground 'run' waits for a key before joining
                    # its runner. Keep stdin open during polling, then stop it.
                    proc.stdin.write(b"\n")
                    proc.stdin.flush()
                    try:
                        proc.wait(timeout=60)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        raise
                proc.stdin.close()
        assert list(root.glob("config.jdbm2*")), "Persistence files missing after shutdown"
    print(f"PASS: packaged startup, four contexts, shutdown and restart ({root})")


if __name__ == "__main__":
    # Retain logs and runtime files on failure for diagnosis.
    workspace = Path(tempfile.mkdtemp(prefix="archappl-gretty-smoke-"))
    print(f"Smoke-test workspace: {workspace}", flush=True)
    smoke(Path(sys.argv[1]).resolve(), workspace)
