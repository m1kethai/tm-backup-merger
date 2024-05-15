#################################
# tm_merge.py ###################
#################################

import argparse
import logging
from pathlib import Path
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext
import datetime
import random
import toml
from calendar import monthrange

# Setup argument parser
parser = argparse.ArgumentParser(description="Merge Time Machine snapshots to a Borg repository.")
parser.add_argument('--enable-logging', action='store_true', help="Enable detailed logging.")
parser.add_argument('--config-file', type=str, default='config.toml', help="Path to configuration file.")
parser.add_argument('--parallel', action='store_true', help="Enable parallel processing of snapshots.")
parser.add_argument('--test', action='store_true', help="Enable test mode - use dummy snapshots in the '.test_bkps' directory.")
args = parser.parse_args()

# Load configuration
config = toml.load(args.config_file)
paths = config['paths']
backup = config['backup']
test = config['test']

# Configure logging
if args.enable_logging:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

# Determine directories based on mode
tm_snapshots_dir = Path(test['test_snapshots_dir'] if args.test else paths['tm_snapshots_dir'])
home_dir = Path(paths['home_dir'])
borg_repo = Path(paths['borg_repo'])

# Initialize Borg repo if necessary
if not borg_repo.exists():
    subprocess.run(['borg', 'init', '--encryption=' + backup['encryption'], str(borg_repo)], check=True)

# Generate dummy snapshots for testing
def gen_dummy_snapshots():
    for year in range(2011, 2023):
        for month in range(1, 13):
            days_in_month = monthrange(year, month)[1]
            for day in range(1, days_in_month + 1):
                timestamp = random.randint(111111, 999999)
                snapshot_dir = f'{year}-{month:02}-{day:02}-{timestamp}'
                snapshot_path = tm_snapshots_dir / snapshot_dir
                snapshot_path.mkdir(parents=True, exist_ok=True)

# Function to process include and exclude rules
def filter_files(path, patterns):
    return any(path.match(pat) for pat in patterns)

# Function to list and write snapshot files to include or exclude
def list_snapshot_files(snapshot_path, archive_name):
    include_patterns = backup['include_patterns']
    exclude_patterns = backup['exclude_patterns']

    keep_files = []
    ignore_files = []

    for path in snapshot_path.glob('**/*'):
        relative_path = path.relative_to(snapshot_path)
        if filter_files(path, exclude_patterns):
            ignore_files.append(str(relative_path))
        else:
            keep_files.append(str(relative_path))

    write_file_lists(archive_name, keep_files, ignore_files)
    return keep_files, ignore_files

def write_file_lists(archive_name, keep_files, ignore_files):
    with open(f"{archive_name}__keep.txt", 'w') as kf, open(f"{archive_name}__ignore.txt", 'w') as inf:
        kf.write('\n'.join(keep_files))
        inf.write('\n'.join(ignore_files))

# Function to create a Borg archive from a snapshot
def create_borg_archive(snapshot_path, archive_name):
    keep_files, ignore_files = list_snapshot_files(snapshot_path, archive_name)
    if args.enable_logging:
        logger.info(f"Prepared to backup {archive_name}: {len(keep_files)} files, ignoring {len(ignore_files)} files.")

    response = input(f"Proceed with backup {archive_name}? (Y/n) ").strip().lower() or 'y'
    if response != 'y':
        if args.enable_logging:
            logger.info(f"Backup aborted by user for {archive_name}.")
        return

    execute_backup(snapshot_path, archive_name)

def execute_backup(snapshot_path, archive_name):
    snapshot_home_path = snapshot_path / home_dir
    try:
        subprocess.run([
            'borg', 'create',
            '--filter', 'AME',
            '--exclude-caches',
            '--exclude', str(snapshot_home_path / 'Library'),
            *['--exclude', pat for pat in backup['exclude_patterns']],
            f'{str(borg_repo)}::{archive_name}',
            str(snapshot_home_path)
        ], check=True)
        if args.enable_logging:
            logger.info(f"Archive created: {archive_name}")
    except subprocess.CalledProcessError as e:
        if args.enable_logging:
            logger.error(f"Failed to create archive {archive_name}: {e}")

# Function to process all snapshots
def process_snapshots():
    if args.test:
        gen_dummy_snapshots()
    futures = []
    with ThreadPoolExecutor() if args.parallel else nullcontext() as executor:
        for snapshot in tm_snapshots_dir.iterdir():
            if snapshot.is_dir():
                archive_name = f'snapshot-{snapshot.name}'
                if args.parallel:
                    futures.append(executor.submit(create_borg_archive, snapshot, archive_name))
                else:
                    create_borg_archive(snapshot, archive_name)
        if args.parallel:
            for future in as_completed(futures):
                future.result()  # to raise any exceptions caught during the execution

    if args.enable_logging:
        logger.info("All snapshots have been processed and added to the Borg repository.")

process_snapshots()
