__version__ = "0.1.0"

from typing import Tuple


def parse_name_with_id(name: str) -> Tuple[str, int, str]:
    parts = name.split("_")
    if len(parts) == 1:
        if parts[0].isdigit():
            return "", int(parts[0]), ""
    elif len(parts) == 2:
        if parts[0].isdigit():
            return "", int(parts[0]), "_" + parts[1]
        elif parts[1].isdigit():
            return parts[0] + "_", int(parts[1]), ""
    elif len(parts) == 3:
        if parts[1].isdigit():
            return parts[0] + "_", int(parts[1]), "_" + parts[2]
    return None, None, None
