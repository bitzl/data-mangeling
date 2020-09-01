import click
from pathlib import Path
import shutil
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map
from functools import partial

GROUP_BY = 1000


@click.command()
@click.argument("source_folder")
@click.argument("target_folder")
def main(source_folder, target_folder):
    source_folder = Path(source_folder)
    target_folder = Path(target_folder)
    target_folder.mkdir(parents=True)

    total = sum(1 for p in source_folder.iterdir() if p.stem.isdecimal())
    max_id = max(int(p.stem) for p in source_folder.iterdir() if p.stem.isdecimal())
    digits = len(str(max_id))

    for i in range(0, max_id // GROUP_BY + 1):
        group_folder = target_folder / f"{i:03d}"
        group_folder.mkdir(exist_ok=True)

    thread_map(
        partial(copy_and_rename, target=target_folder, digits=digits),
        source_folder.iterdir(),
        total=total,
    )


def copy_and_rename(source: Path, target_folder: Path, digits: int):
    if not source.stem.isdecimal():
        return
    number = int(source.stem)
    group_folder = f"{number // GROUP_BY:03d}"
    target = target_folder / group_folder / f"{number:0{digits}d}{source.suffix}"

    shutil.copy(source, target)


if __name__ == "__main__":
    main(None, None)
