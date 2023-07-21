from datetime import datetime as dt


def stair_case(string: str) -> str:
    return ''.join([char.lower() if en % 2 else char.upper() for en, char in enumerate(string)])


def coalesce_spaces(string: str) -> str:
    new_str = string.split()
    return ' '.join(new_str)


def append_date(string: str) -> str:
    lines = string.rstrip().split("\n")
    new_line = []
    for i in lines:
        new_line.append(f"{i} {dt.now()}\n")
    return "".join(new_line)
