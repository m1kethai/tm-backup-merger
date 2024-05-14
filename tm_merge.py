#################################
# tm_merge.py ###################
#################################

from pathlib import Path
import subprocess

# Define paths
tm_snapshots_dir = Path('/run/media/mikethai/Time Machine/Backups.backupdb/Comp-215')
borg_repo = Path('/home/mikethai/BACKUP/BORG/TMBKP')
home_dir = Path('HDD - Data/Users/mike.thai')

# Initialize Borg repository if not already done
if not borg_repo.exists():
    subprocess.run(['borg', 'init', '--encryption=none', str(borg_repo)], check=True)

# Function to create a Borg archive from a snapshot
def create_borg_archive(snapshot_path, archive_name):
    snapshot_home_path = snapshot_path / home_dir
    try:
        subprocess.run([
            'borg', 'create',
            '--filter', 'AME',
            '--exclude-caches',
            '--exclude', str(snapshot_home_path / 'Library'),
            '--exclude', '*.tmp',
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
    except subprocess.CalledProcessError as e:
        print(f"Failed to create archive {archive_name}: {e}")

# Iterate over snapshots and create Borg archives
for snapshot in tm_snapshots_dir.iterdir():
    if snapshot.is_dir():
        archive_name = f'snapshot-{snapshot.name}'
        create_borg_archive(snapshot, archive_name)

print("All snapshots have been processed and added to the Borg repository.")
