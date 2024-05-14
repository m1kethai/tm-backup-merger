#################################
# tm_merge.py ###################
#################################

import argparse
import logging
from pathlib import Path
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext
import sys

# Setup argument parser
parser = argparse.ArgumentParser(description="Merge Time Machine snapshots to a Borg repository.")
parser.add_argument('--enable-logging', action='store_true', help="Enable detailed logging.")
parser.add_argument('--config-file', type=str, help="Path to configuration file.")
parser.add_argument('--parallel', action='store_true', help="Enable parallel processing of snapshots.")
args = parser.parse_args()

# Configure logging
if args.enable_logging:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

# Function to read configuration from file
def load_config(path):
    config = {}
    with open(path, 'r') as file:
        for line in file:
            key, value = line.strip().split('=')
            config[key.strip()] = value.strip()
    return config

# Load configuration if specified
if args.config_file:
    config = load_config(args.config_file)
    tm_snapshots_dir = Path(config.get('TM_SNAPSHOTS_DIR', '/run/media/mikepc/Time Machine/Backups.backupdb/Comp-215'))
    borg_repo = Path(config.get('BORG_REPO', '/home/mikepc/BACKUP/BORG/TMBKP'))
    home_dir = Path(config.get('HOME_DIR', 'HDD - Data/Users/mike.thai'))
else:
    tm_snapshots_dir = Path('/run/media/mikepc/Time Machine/Backups.backupdb/Comp-215')
    borg_repo = Path('/home/mikepc/BACKUP/BORG/TMBKP')
    home_dir = Path('HDD - Data/Users/mike.thai')

# Initialize Borg repository if not already done
if not borg_repo.exists():
    subprocess.run(['borg', 'init', '--encryption=none', str(borg_repo)], check=True)

# Function to list and write snapshot files to include or exclude
def list_snapshot_files(snapshot_path, archive_name):
    include_patterns = [str(snapshot_path / home_dir)]
    exclude_patterns = [
        '**/Library', '*.tmp', '*.iso', '*.vdi', '*.vmdk', '*.ova',
        '*.vbox', '*.img', '*.dmg', '**/node_modules/*', '**/.git/*', '**/build/*'
    ]

    # Prepare to list files
    keep_files = []
    ignore_files = []

    for path in snapshot_path.glob('**/*'):
        relative_path = path.relative_to(snapshot_path)
        if any(path.match(pat) for pat in exclude_patterns):
            ignore_files.append(str(relative_path))
        else:
            keep_files.append(str(relative_path))

    # Write files to directories
    with open(f"{archive_name}__keep.txt", 'w') as kf, open(f"{archive_name}__ignore.txt", 'w') as inf:
        kf.write('\n'.join(keep_files))
        inf.write('\n'.join(ignore_files))

    return keep_files, ignore_files

# Function to create a Borg archive from a snapshot
def create_borg_archive(snapshot_path, archive_name):
    keep_files, ignore_files = list_snapshot_files(snapshot_path, archive_name)
    if args.enable_logging:
        logger.info(f"Prepared to backup {archive_name}: {len(keep_files)} files, ignoring {len(ignore_files)} files.")

    # Confirm with the user
    response = input(f"Proceed with backup {archive_name}? (Y/n) ").strip().lower() or 'y'
    if response != 'y':
        if args.enable_logging:
            logger.info(f"Backup aborted by user for {archive_name}.")
        return

    # Execute backup
    snapshot_home_path = snapshot_path / home_dir
    try:
        subprocess.run([
            'borg', 'create',
            '--filter', 'AME',
            '--exclude-caches',
            '--exclude', str(snapshot_home_path / 'Library'),
            '--exclude', '*.tmp',       # Add your specific exclusions here
            '--exclude', '*.iso',
            '--exclude', '*.vdi',
            '--exclude', '*.vmdk',
            '--exclude', '*.ova',
            '--exclude', '*.vbox',
            '--exclude', '*.img',
            '--exclude', '*.dmg',
            '--exclude', '**/node_modules/*',
            '--exclude', '**/.git/*',
            '--exclude', '**/build/*',
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

process_snapshots()

if args.enable_logging:
    logger.info("All snapshots have been processed and added to the Borg repository.")
