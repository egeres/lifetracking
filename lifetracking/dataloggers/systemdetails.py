from __future__ import annotations

import os
import platform
import sys
from datetime import datetime, timezone


def get_system_details(way_this_info_was_added: str) -> dict[str, str]:
    """Gets the details of the system"""

    assert isinstance(way_this_info_was_added, str)

    return {
        "os.login()": os.getlogin(),
        "platform.system()": platform.system(),
        "machine_name": str(os.getenv("COMPUTERNAME", os.getenv("HOSTNAME"))),
        "way_this_info_was_added": way_this_info_was_added,
        "name_of_the_script": sys.argv[0],
        "datetime_of_annotation": datetime.now(timezone.utc).isoformat(),
    }
