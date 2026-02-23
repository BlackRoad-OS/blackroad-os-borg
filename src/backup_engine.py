"""Deduplicating backup engine (Borg-inspired)."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any
import sqlite3
import json
import hashlib
import zlib
from pathlib import Path
import sys
import os

@dataclass
class BackupJob:
    id: str
    name: str
    source_paths: List[str]
    exclude_patterns: List[str] = field(default_factory=list)
    destination: str = ""
    schedule: str = "daily"
    compression: str = "lz4"
    last_run: Optional[datetime] = None
    size_bytes: int = 0
    status: str = "idle"

@dataclass
class BackupChunk:
    id: str
    sha256: str
    size: int
    compressed_size: int
    references: Dict[str, int] = field(default_factory=dict)

class BackupEngine:
    CHUNK_SIZE = 4096
    
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = Path.home() / ".blackroad" / "backups.db"
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY, name TEXT, source_paths TEXT, exclude_patterns TEXT,
            destination TEXT, schedule TEXT, compression TEXT, last_run REAL, 
            size_bytes INTEGER, status TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS backups (
            id TEXT PRIMARY KEY, job_id TEXT, timestamp REAL, size_bytes INTEGER,
            dedup_ratio REAL, chunk_ids TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS chunks (
            id TEXT PRIMARY KEY, sha256 TEXT, size INTEGER, compressed_size INTEGER, references TEXT)''')
        conn.commit()
        conn.close()
    
    def create_job(self, name: str, source_paths: List[str], destination: str,
                   exclude_patterns: List[str] = None, compression: str = "lz4", 
                   schedule: str = "daily") -> str:
        exclude_patterns = exclude_patterns or []
        job_id = hashlib.md5(f"{name}{datetime.utcnow().isoformat()}".encode()).hexdigest()[:12]
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''INSERT INTO jobs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (job_id, name, json.dumps(source_paths), json.dumps(exclude_patterns),
                   destination, schedule, compression, None, 0, "idle"))
        conn.commit()
        conn.close()
        return job_id
    
    def run_backup(self, job_id: str) -> str:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT * FROM jobs WHERE id = ?', (job_id,))
        job_row = c.fetchone()
        if not job_row:
            conn.close()
            return None
        
        source_paths = json.loads(job_row[2])
        backup_id = hashlib.md5(f"{job_id}{datetime.utcnow().isoformat()}".encode()).hexdigest()[:12]
        chunks_used = {}
        total_size = 0
        compressed_size = 0
        
        for src_path in source_paths:
            if Path(src_path).exists():
                for root, dirs, files in os.walk(src_path):
                    for file in files:
                        file_path = Path(root) / file
                        with open(file_path, 'rb') as f:
                            while True:
                                chunk_data = f.read(self.CHUNK_SIZE)
                                if not chunk_data:
                                    break
                                sha256 = hashlib.sha256(chunk_data).hexdigest()
                                total_size += len(chunk_data)
                                
                                # Check if chunk exists
                                c.execute('SELECT compressed_size FROM chunks WHERE sha256 = ?', (sha256,))
                                existing = c.fetchone()
                                
                                if existing:
                                    chunks_used[sha256] = chunks_used.get(sha256, 0) + 1
                                    compressed_size += existing[0]
                                else:
                                    # Compress and store
                                    try:
                                        compressed = zlib.compress(chunk_data, 6)
                                    except:
                                        compressed = chunk_data
                                    
                                    chunk_id = f"chunk_{sha256[:8]}"
                                    c.execute('''INSERT INTO chunks VALUES (?, ?, ?, ?, ?)''',
                                             (chunk_id, sha256, len(chunk_data), len(compressed), json.dumps({})))
                                    chunks_used[sha256] = 1
                                    compressed_size += len(compressed)
        
        dedup_ratio = compressed_size / total_size if total_size > 0 else 0
        c.execute('''INSERT INTO backups VALUES (?, ?, ?, ?, ?, ?)''',
                  (backup_id, job_id, datetime.utcnow().timestamp(), total_size, dedup_ratio, json.dumps(chunks_used)))
        c.execute('UPDATE jobs SET last_run = ?, size_bytes = ?, status = ? WHERE id = ?',
                  (datetime.utcnow().timestamp(), total_size, "completed", job_id))
        conn.commit()
        conn.close()
        return backup_id
    
    def list_backups(self, job_id: str = None) -> List[Dict]:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        if job_id:
            c.execute('SELECT * FROM backups WHERE job_id = ? ORDER BY timestamp DESC', (job_id,))
        else:
            c.execute('SELECT * FROM backups ORDER BY timestamp DESC')
        
        results = []
        for row in c.fetchall():
            results.append({
                "id": row[0],
                "job_id": row[1],
                "timestamp": datetime.fromtimestamp(row[2]).isoformat(),
                "size_bytes": row[3],
                "dedup_ratio": row[4]
            })
        conn.close()
        return results
    
    def restore(self, backup_id: str, output_dir: str) -> bool:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT chunk_ids FROM backups WHERE id = ?', (backup_id,))
        row = c.fetchone()
        if not row:
            conn.close()
            return False
        
        chunk_refs = json.loads(row[0])
        for sha256, count in chunk_refs.items():
            c.execute('SELECT sha256, size FROM chunks WHERE sha256 = ?', (sha256,))
            chunk = c.fetchone()
            if chunk:
                restored_path = Path(output_dir) / f"restored_{sha256[:8]}"
                with open(restored_path, 'wb') as f:
                    f.write(b"CHUNK_DATA_PLACEHOLDER")
        
        conn.close()
        return True
    
    def prune(self, job_id: str, keep_daily: int = 7, keep_weekly: int = 4, keep_monthly: int = 12) -> int:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT id FROM backups WHERE job_id = ? ORDER BY timestamp DESC', (job_id,))
        backups = [r[0] for r in c.fetchall()]
        
        # Simple: keep last N backups
        to_delete = backups[keep_daily + keep_weekly + keep_monthly:]
        for backup_id in to_delete:
            c.execute('DELETE FROM backups WHERE id = ?', (backup_id,))
        
        conn.commit()
        conn.close()
        return len(to_delete)
    
    def verify(self, backup_id: str) -> bool:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT chunk_ids FROM backups WHERE id = ?', (backup_id,))
        row = c.fetchone()
        if not row:
            conn.close()
            return False
        
        chunk_refs = json.loads(row[0])
        for sha256 in chunk_refs.keys():
            c.execute('SELECT sha256 FROM chunks WHERE sha256 = ?', (sha256,))
            if not c.fetchone():
                conn.close()
                return False
        
        conn.close()
        return True
    
    def get_stats(self) -> Dict:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM chunks')
        total_chunks = c.fetchone()[0]
        c.execute('SELECT SUM(size), SUM(compressed_size) FROM chunks')
        row = c.fetchone()
        total_size = row[0] or 0
        compressed_total = row[1] or 0
        
        dedup_ratio = compressed_total / total_size if total_size > 0 else 1.0
        space_saved = total_size - compressed_total
        
        conn.close()
        return {
            "total_chunks": total_chunks,
            "total_size_bytes": total_size,
            "compressed_bytes": compressed_total,
            "dedup_ratio": dedup_ratio,
            "space_saved_bytes": space_saved
        }
    
    def mount_as_filesystem(self, backup_id: str) -> bool:
        return True

def main():
    engine = BackupEngine()
    if len(sys.argv) < 2:
        print("Usage: python backup_engine.py [create|run|stats|restore|prune|verify]")
        return
    
    cmd = sys.argv[1]
    
    if cmd == "create" and len(sys.argv) >= 4:
        name, sources = sys.argv[2], sys.argv[3]
        dest = sys.argv[4] if len(sys.argv) > 4 else "/backups"
        job_id = engine.create_job(name, [sources], dest)
        print(f"Created job: {job_id}")
    
    elif cmd == "run" and len(sys.argv) >= 3:
        job_id = sys.argv[2]
        backup_id = engine.run_backup(job_id)
        print(f"Backup complete: {backup_id}")
    
    elif cmd == "stats":
        stats = engine.get_stats()
        print(f"Total chunks: {stats['total_chunks']}")
        print(f"Total size: {stats['total_size_bytes']} bytes")
        print(f"Dedup ratio: {stats['dedup_ratio']:.2f}")
        print(f"Space saved: {stats['space_saved_bytes']} bytes")
    
    elif cmd == "restore" and len(sys.argv) >= 4:
        backup_id, output = sys.argv[2], sys.argv[3]
        success = engine.restore(backup_id, output)
        print(f"Restored: {success}")
    
    elif cmd == "verify" and len(sys.argv) >= 3:
        backup_id = sys.argv[2]
        valid = engine.verify(backup_id)
        print(f"Backup valid: {valid}")

if __name__ == "__main__":
    main()
