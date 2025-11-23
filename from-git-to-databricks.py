#!/usr/bin/env python3
"""
Git to Databricks Pipeline

Required environment variables:
    GITHUB_REPO       - e.g., github.com/user/repo.git
    GITHUB_PAT        - GitHub personal access token
    DATABRICKS_HOST   - e.g., https://your-workspace.azuredatabricks.net
    DATABRICKS_TOKEN  - Databricks personal access token

Optional environment variables:
    BRANCH_NAME       - Git branch (default: main)
    LOCAL_FOLDER      - Local clone path (default: ./repo)
    DBFS_PATH         - Databricks destination (default: /FileStore/sql-files)
    LOG_DIR           - Log directory (default: ./logs)
"""

import os
import sys
import json
import base64
import hashlib
import logging
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
import requests
from dotenv import load_dotenv

load_dotenv()


# ===========================================
# CONFIG - From env file
# ===========================================
def get_config() -> dict:
    required = ["GITHUB_REPO", "GITHUB_PAT", "DATABRICKS_HOST", "DATABRICKS_TOKEN"]
    missing = [var for var in required if not os.environ.get(var)]
    
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

#If optionals are missing, sets a default    
    return {
        "github_repo": os.environ["GITHUB_REPO"],
        "github_pat": os.environ["GITHUB_PAT"],
        "databricks_host": os.environ["DATABRICKS_HOST"].rstrip("/"),
        "databricks_token": os.environ["DATABRICKS_TOKEN"],
        "branch_name": os.environ.get("BRANCH_NAME", "main"),
        "local_folder": Path(os.environ.get("LOCAL_FOLDER", "./repo")),
        "volume_path": os.environ.get("VOLUME_PATH", "/Volumes/catalog/schema/volume"),
        "log_dir": Path(os.environ.get("LOG_DIR", "./logs")),
    }


# ===========================================
# LOGGING SETUP
# ===========================================
def setup_logging(log_dir: Path) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"pipeline_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return log_file


# ===========================================
# CHECKSUM FUNCTIONS (for change detection)
# ===========================================
class ChecksumTracker:
    def __init__(self, checksum_file: Path):
        self.checksum_file = checksum_file
        self.old_checksums: Dict[str, str] = {}
        self.new_checksums: Dict[str, str] = {}
        self._load()
    
    def _load(self):
        if self.checksum_file.exists():
            with open(self.checksum_file, "r") as f:
                for line in f:
                    if "=" in line:
                        file_path, checksum = line.strip().split("=", 1)
                        self.old_checksums[file_path] = checksum
    
    def save(self):
        with open(self.checksum_file, "w") as f:
            for file_path, checksum in self.new_checksums.items():
                f.write(f"{file_path}={checksum}\n")
    
    @staticmethod
    def get_checksum(file_path: Path) -> str:
        with open(file_path, "rb") as f:
            return hashlib.md5(f.read()).hexdigest()
    
    def has_changed(self, relative_path: str, checksum: str) -> bool:
        self.new_checksums[relative_path] = checksum
        old = self.old_checksums.get(relative_path)
        return old is None or old != checksum


# ===========================================
# GIT OPERATIONS
# ===========================================
def git_sync(config: dict) -> bool:
    repo_url = f"https://{config['github_pat']}@{config['github_repo']}"
    local_folder = config["local_folder"]
    branch = config["branch_name"]
    
    try:
        if local_folder.exists():
            logging.info("Repo exists, pulling latest changes...")
            subprocess.run(
                ["git", "-C", str(local_folder), "remote", "set-url", "origin", repo_url],
                check=True, capture_output=True
            )
            subprocess.run(
                ["git", "-C", str(local_folder), "fetch", "origin", branch],
                check=True, capture_output=True
            )
            subprocess.run(
                ["git", "-C", str(local_folder), "reset", "--hard", f"origin/{branch}"],
                check=True, capture_output=True
            )
        else:
            logging.info("Cloning repo...")
            subprocess.run(
                ["git", "clone", "-b", branch, "--single-branch", "--depth", "1", repo_url, str(local_folder)],
                check=True, capture_output=True
            )
        
        logging.info("Git sync complete")
        return True
        
    except subprocess.CalledProcessError as e:
        logging.error(f"Git operation failed: {e.stderr.decode() if e.stderr else str(e)}")
        return False


# ===========================================
# DATABRICKS UPLOAD
# ===========================================
class DatabricksUploader:
    def __init__(self, host: str, token: str, volume_path: str):
        self.host = host
        self.token = token
        self.volume_path = volume_path.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {token}"
        }
    
    def upload_file(self, local_file: Path, relative_path: str) -> bool:
        try:
            # Unity Catalog Volumes use the Files API
            # Path format: /Volumes/catalog/schema/volume/path/to/file
            upload_url = f"{self.host}/api/2.0/fs/files{self.volume_path}/{relative_path}"
            
            with open(local_file, "rb") as f:
                file_content = f.read()
            
            response = requests.put(
                upload_url,
                headers={**self.headers, "Content-Type": "application/octet-stream"},
                data=file_content
            )
            response.raise_for_status()
            return True
            
        except requests.RequestException as e:
            logging.error(f"Upload failed for {relative_path}: {e}")
            return False


# ===========================================
# MAIN PIPELINE
# ===========================================
def main():
    try:
        config = get_config()
    except EnvironmentError as e:
        print(f"Configuration error: {e}")
        sys.exit(1)
    
    log_file = setup_logging(config["log_dir"])
    logging.info("=== Pipeline started ===")
    logging.info(f"Log file: {log_file}")
    
    # Step 1: Git sync
    logging.info("Step 1: Syncing from GitHub")
    if not git_sync(config):
        logging.error("Git sync failed, aborting")
        sys.exit(1)
    
    # Step 2: Detect changes and upload
    logging.info("Step 2: Detecting changed files")
    
    checksum_file = config["log_dir"] / ".checksums"
    tracker = ChecksumTracker(checksum_file)
    uploader = DatabricksUploader(
        config["databricks_host"],
        config["databricks_token"],
        config["volume_path"]
    )
    
    uploaded, skipped, failed = 0, 0, 0
    
    for file in config["local_folder"].rglob("*"):
        # Skip directories and anything inside .git
        if not file.is_file() or ".git" in file.parts:
            continue
        
        relative_path = str(file.relative_to(config["local_folder"]))
        checksum = tracker.get_checksum(file)
        
        if tracker.has_changed(relative_path, checksum):
            logging.info(f"Uploading: {relative_path}")
            if uploader.upload_file(file, relative_path):
                logging.info(f"Uploaded: {relative_path}")
                uploaded += 1
            else:
                logging.error(f"Failed: {relative_path}")
                failed += 1
        else:
            logging.info(f"Skipped (unchanged): {relative_path}")
            skipped += 1
    
    tracker.save()
    
    # Summary
    logging.info("=== Pipeline complete ===")
    logging.info(f"Uploaded: {uploaded} | Skipped: {skipped} | Failed: {failed}")
    logging.info(f"Files available at: {config['volume_path']}")
    
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()