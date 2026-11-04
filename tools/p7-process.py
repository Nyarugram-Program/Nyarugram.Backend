"""Manage the prepared local P7 stack while retaining its database volumes."""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.request

action, directory = sys.argv[1:]
if action not in ("start", "stop"):
    raise ValueError("Expected start or stop")
root = Path(directory).resolve()
if not str(root).startswith("/mnt/d/"):
    raise ValueError("P7 dependencies and state must remain on D")
manifest = json.loads((root / "runtime-manifest.json").read_text())
mount = Path("/mnt/botgram-p7")
docker = ["docker", "-H", "unix:///tmp/botgram-p7-docker.sock"]


def processes(required):
    matches = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            args = (entry / "cmdline").read_bytes().split(b"\0")
            if all(value.encode() in args for value in required):
                matches.append(int(entry.name))
        except FileNotFoundError:
            pass
    if len(matches) > 1:
        raise RuntimeError("Multiple processes match the task-owned command")
    return matches


def stop(pids):
    for pid in pids:
        os.kill(pid, signal.SIGTERM)
        until = time.monotonic() + 35
        while Path("/proc", str(pid)).exists():
            try:
                state = Path("/proc", str(pid), "stat").read_text().split(") ", 1)[1][0]
            except FileNotFoundError:
                break
            if state == "Z":
                break
            if time.monotonic() > until:
                raise TimeoutError("Task process did not shut down normally")
            time.sleep(.2)


def launch(args, log, env=None, cwd=None):
    (root / "logs").mkdir(exist_ok=True)
    with (root / "logs" / log).open("ab") as output:
        return subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=output, stderr=output,
                                start_new_session=True, env=env, cwd=cwd)


def container(name):
    value = json.loads(subprocess.check_output(docker + ["inspect", name], text=True))[0]
    expected = manifest["containers"][name]
    if value["Image"] != expected["imageId"] or value["HostConfig"]["NetworkMode"] != "host":
        raise RuntimeError("Task container ownership or image mismatch")
    if not any(item["Source"] == str(mount / expected["dataDirectory"]) for item in value["Mounts"]):
        raise RuntimeError("Task container data directory mismatch")
    return value


def hosts():
    result = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            args = (entry / "cmdline").read_bytes().split(b"\0")
            if str(root / "private").encode() in args and b"--contentRoot" in args:
                dlls = [item.decode() for item in args if item.endswith(b"/Botgram.Backend.dll")]
                if len(dlls) == 1 and dlls[0].startswith(str(root) + "/"):
                    result.append((int(entry.name), dlls[0]))
        except FileNotFoundError:
            pass
    if len(result) > 1:
        raise RuntimeError("Multiple P7 hosts use this private configuration")
    return result


if action == "stop":
    stop([pid for pid, _ in hosts()])
    stop(processes(["kafka.Kafka", str(root / "kafka/server.properties")]))
    if subprocess.run(docker + ["info"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
        for name in manifest["containers"]:
            if container(name)["State"]["Running"]:
                subprocess.run(docker + ["stop", name], check=True, stdout=subprocess.DEVNULL)
    stop(processes(["--data-root=" + str(mount / "docker")]))
    stop(processes(["--address", "/tmp/botgram-p7-containerd.sock"]))
    (root / "process.json").write_text(json.dumps({"status": "stopped", "pid": None, "url": manifest["url"]}, indent=2))
    print("Stopped the isolated P7 stack. Data and filesystem image retained.")
    sys.exit(0)

if subprocess.run(["findmnt", "--mountpoint", str(mount)], stdout=subprocess.DEVNULL).returncode:
    mount.mkdir(exist_ok=True)
    subprocess.run(["mount", "-o", "loop", str(root / "runtime.ext4"), str(mount)], check=True)
device = subprocess.check_output(["findmnt", "-n", "-o", "SOURCE", "--mountpoint", str(mount)], text=True).strip()
backing = subprocess.check_output(["/usr/sbin/losetup", "-n", "-O", "BACK-FILE", device], text=True).strip()
if Path(backing).resolve() != root / "runtime.ext4":
    raise RuntimeError("P7 mount is not backed by the expected D-drive image")
if not processes(["--address", "/tmp/botgram-p7-containerd.sock"]):
    launch(["/usr/bin/containerd", "--root", str(root / "containerd"), "--state", "/tmp/botgram-p7-containerd",
            "--address", "/tmp/botgram-p7-containerd.sock"], "containerd.log")
if not processes(["--data-root=" + str(mount / "docker")]):
    launch(["bash", str(root / "start-docker.sh")], "docker.log")
for _ in range(60):
    probe = subprocess.run(docker + ["info", "--format", "{{.DockerRootDir}}"], capture_output=True, text=True)
    if probe.returncode == 0:
        if probe.stdout.strip() != str(mount / "docker"):
            raise RuntimeError("Unexpected Docker data root")
        break
    time.sleep(.5)
else:
    raise TimeoutError("Task-owned Docker did not start")
for name in manifest["containers"]:
    current = container(name)
    if name == "botgram-p7-scylla" and not current["State"]["Running"]:
        subprocess.run(docker + ["cp", str(root / "scylla-node-exporter"), name + ":/etc/default/scylla-node-exporter"], check=True)
    if not current["State"]["Running"]:
        subprocess.run(docker + ["start", name], check=True, stdout=subprocess.DEVNULL)
if not processes(["kafka.Kafka", str(root / "kafka/server.properties")]):
    launch(["bash", str(root / "kafka/start.sh")], "kafka.log")
dll = Path(manifest["dll"]).resolve()
if not dll.is_relative_to(root / "releases") or hashlib.sha256(dll.read_bytes()).hexdigest() != manifest["sha256"]:
    raise RuntimeError("Prepared Release DLL hash or path differs from the runtime manifest")
existing = hosts()
if existing and existing[0][1] != str(dll):
    stop([existing[0][0]])
    existing = []
if not existing:
    env = os.environ.copy()
    env.update(ASPNETCORE_ENVIRONMENT="Development", DOTNET_CLI_HOME="/mnt/d/BotgramBuild/dotnet-linux-home",
               TMPDIR="/mnt/d/BotgramBuild/tmp")
    # Dependencies can take time to reconnect. Retry only after the previous host has exited.
    child = None
    for _ in range(3):
        child = launch([manifest["dotnet"], str(dll), "--contentRoot", str(root / "private")],
                       "host.log", env, str(root / "private"))
        time.sleep(6)
        if child.poll() is None:
            break
        time.sleep(3)
    if child.poll() is not None:
        raise RuntimeError("P7 host exited during startup. Inspect the private runtime logs.")
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
until = time.monotonic() + 90
while time.monotonic() < until:
    try:
        with opener.open(manifest["url"] + "/health", timeout=15) as response:
            health = json.load(response)
        if health["fullStackReady"]:
            (root / "process.json").write_text(json.dumps({"status": "running", "pid": hosts()[0][0], "dll": str(dll),
                                                         "sha256": manifest["sha256"], "url": manifest["url"]}, indent=2))
            print("P7 ready at " + manifest["url"] + ". Four real components connected.")
            break
    except OSError:
        pass
    time.sleep(1)
else:
    raise TimeoutError("P7 readiness did not recover. Processes retained for diagnosis.")
