import click
from pathlib import Path
import shutil
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map

from data_wrangling import parse_name_with_id
from functools import partial

GROUP_BY = 1000


@click.command()
@click.argument("source_folder")
@click.argument("target_folder")
@click.option("--parallel/--not-parallel", default=False)
def main(source_folder, target_folder, parallel):
    source_folder = Path(source_folder)
    target_folder = Path(target_folder)
    target_folder.mkdir(parents=True)

    total = sum(1 for p in source_folder.iterdir())
    max_id = max(get_id_number(p.stem) for p in source_folder.iterdir())
    digits = len(str(max_id))

    for i in range(0, max_id // GROUP_BY + 1):
        group_folder = target_folder / f"{i:03d}"
        group_folder.mkdir(exist_ok=True)

    if parallel:
        process_map(
            partial(copy_and_rename, target_folder=target_folder, digits=digits),
            source_folder.iterdir(),
            total=total,
        )
    else:
        for source in tqdm(source_folder.iterdir(), total=total):
            copy_and_rename(source, target_folder, digits)


def copy_and_rename(source: Path, target_folder: Path, digits: int):
    prefix, id_number, suffix = parse_name_with_id(source.stem)
    if id_number is None:
        return
    group_folder = f"{id_number // GROUP_BY:03d}"
    target = (
        target_folder
        / group_folder
        / f"{prefix}{id_number:0{digits}d}{suffix}{source.suffix}"
    )

    shutil.copy(source, target)


def get_id_number(name: str) -> int:
    _, id_number, __ = parse_name_with_id(name)
    if id_number is None:
        return -1
    return id_number


if __name__ == "__main__":
    main(None, None)
