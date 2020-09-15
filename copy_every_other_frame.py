import click
from pathlib import Path
import shutil
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map

from data_wrangling import parse_name_with_id
from functools import partial


@click.command()
@click.argument("source_folder")
@click.argument("target_folder")
@click.option("--every", default=2)
def main(source_folder, target_folder, every):
    source_folder = Path(source_folder)
    target_folder = Path(target_folder)
    target_folder.mkdir(parents=True)

    click.secho("Counting paths to copy...")
    total = sum(1 for p in paths_to_copy(source_folder, every))

    click.secho("Copying...")
    for source in tqdm(paths_to_copy(source_folder, every), total=total):
        shutil.copy(source, target_folder / source.name)

    click.secho("Done.")


def paths_to_copy(source: Path, every: int):
    last_id = None
    ids_to_copy = sorted(set(get_id_number(path.stem) for path in source.iterdir()))
    ids_to_copy = set(ids_to_copy[::every])
    for path in sorted(source.iterdir()):
        id_number = get_id_number(path)
        if id_number in ids_to_copy:
            yield path


def get_id_number(name: str) -> int:
    _, id_number, __ = parse_name_with_id(name)
    if id_number is None:
        return -1
    return id_number


if __name__ == "__main__":
    main(None, None)
