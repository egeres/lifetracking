import re
import subprocess

from tomlkit import dumps, parse


def get_latest_git_tag():
    result = subprocess.run(
        args=["git", "describe", "--abbrev=0", "--tags"], capture_output=True, text=True
    )
    if result.returncode != 0:
        msg = "Failed to fetch latest Git tag."
        raise ValueError(msg)
    return result.stdout.strip()


def update_version_in_pyproject_toml(version):
    with open("pyproject.toml") as f:
        content = parse(f.read())

    if version.startswith("v"):
        version = version[1:]
    content["tool"]["poetry"]["version"] = version  # type: ignore

    with open("pyproject.toml", "w") as f:
        f.write(dumps(content))


def main():
    latest_tag = get_latest_git_tag()
    if not re.match(r"^v?\d+\.\d+\.\d+$", latest_tag):
        msg = (
            f"Invalid Git tag: {latest_tag}."
            "Must be in semantic versioning format (e.g., 1.2.3)"
        )
        raise ValueError(msg)

    update_version_in_pyproject_toml(latest_tag)
    print(f"Updated version in pyproject.toml to {latest_tag}")


if __name__ == "__main__":
    main()
