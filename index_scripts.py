import os
import sqlite3
import duckdb
import time
from datetime import datetime

# --- 設定 ---
# 1. 検索対象のスクリプトフォルダ
SOURCE_DIR = r"\\LS720D7A9\TakashiBK\投資\MyPython"

# 2. Obsidianの出力先フォルダ
OBSIDIAN_DIR = r"C:\Users\ensyu\Documents\Obsidian保管庫\ensyu_star_capital\python"

# 3. データベースの保存先（パスとファイル名を個別に指定）
DB_FOLDER = r"\\LS720D7A9\TakashiBK\投資\MyPython" # フォルダパス
DB_NAME = "python_script_list.db" # ファイル名

# 除外したいフォルダ名
EXCLUDE_DIRS = {".venv", "__pycache__", ".git", ".ipynb_checkpoints"}

def extract_metadata(lines):
    """# --- metadata --- セクションからメタデータを抽出"""
    metadata = {"description": "", "システム構成図": ""}
    in_metadata = False
    
    for line in lines:
        line = line.strip()
        if line == "# --- metadata ---":
            in_metadata = not in_metadata
            continue
            
        if in_metadata and line.startswith("#"):
            line = line[1:].strip()  # Remove the '#' prefix
            if ":" in line:
                key, value = line.split(":", 1)
                key = key.strip()
                value = value.strip()
                if key in metadata:
                    metadata[key] = value
    
    return metadata

def extract_info(file_path):
    """ファイルから概要とコード内容を抽出"""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            content = "".join(lines)
            description = "説明なし"
            if not lines:
                return description, content, {}

            # メタデータを抽出
            metadata = extract_metadata(lines)
            
            # 既存の説明文抽出ロジック（メタデータにdescriptionがない場合に使用）
            first_line = lines[0].strip()
            if first_line.startswith(('"""', "'''")):
                desc_lines = []
                quote_type = first_line[:3]
                first_content = first_line[3:]
                if first_content:
                    desc_lines.append(first_content)
                for line in lines[1:]:
                    if quote_type in line:
                        desc_lines.append(line.split(quote_type)[0])
                        break
                    desc_lines.append(line.strip())
                description = " ".join(desc_lines).strip()
            elif first_line.startswith("#"):
                description = first_line.replace("#", "").strip()
                
            # メタデータにdescriptionがあれば上書き
            if metadata["description"]:
                description = metadata["description"]
                
            return description, content, metadata
    except Exception as e:
        return f"エラー: {str(e)}", "", {"description": "", "システム構成図": ""}

def run():
    start_time = time.time()
    
    # Obsidianフォルダの作成
    if not os.path.exists(OBSIDIAN_DIR):
        os.makedirs(OBSIDIAN_DIR)
        
    # --- 代替案の工夫：DBフォルダの自動作成 ---
    if not os.path.exists(DB_FOLDER):
        os.makedirs(DB_FOLDER)
        print(f"DBフォルダを作成しました: {DB_FOLDER}")

    # フルパスの結合
    db_full_path = os.path.join(DB_FOLDER, DB_NAME)

    con = duckdb.connect(db_full_path)
    con.execute("""
        CREATE TABLE IF NOT EXISTS scripts (
            path TEXT PRIMARY KEY, 
            name TEXT, 
            description TEXT, 
            content TEXT, 
            last_updated TIMESTAMP
        )
    """)

    processed_files = []

    for root, dirs, files in os.walk(SOURCE_DIR):
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]

        for file in files:
            if file.endswith(".py") and file != "index_scripts.py":
                full_path = os.path.join(root, file)
                desc, code, metadata = extract_info(full_path)
                mtime = datetime.fromtimestamp(os.path.getmtime(full_path))

                con.execute("""
                    INSERT OR REPLACE INTO scripts (path, name, description, content, last_updated)
                    VALUES (?, ?, ?, ?, ?)
                """, (full_path, file, desc, code, mtime))

                md_filename = f"{file}.md"
                output_path = os.path.join(OBSIDIAN_DIR, md_filename)
                
                with open(output_path, "w", encoding="utf-8") as f:
                    # YAML front matter for Obsidian properties
                    f.write("---\n")
                    f.write(f"title: {file}\n")
                    f.write(f"description: {desc}\n")
                    f.write(f"システム構成図: {metadata.get('システム構成図', '')}\n")
                    f.write(f"created: {mtime.strftime('%Y-%m-%dT%H:%M:%S+09:00')}\n")
                    f.write(f"updated: {mtime.strftime('%Y-%m-%dT%H:%M:%S+09:00')}\n")
                    f.write("tags:\n")
                    f.write("  - python_script\n")
                    f.write("  - tools\n")
                    f.write("aliases: \n")
                    f.write("  - " + file + "\n")
                    f.write("---\n\n")
                    
                    f.write(f"# {file}\n\n")
                    f.write(f"> [!abstract] 概要\n")
                    f.write(f"> {desc}\n\n")
                    f.write("## スクリプト情報\n")
                    f.write(f"- **フルパス**: `{full_path}`\n")
                    f.write(f"- **最終更新**: {mtime.strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write("\n---\n\n")
                    f.write("## ソースコード\n\n")
                    f.write("```python\n")
                    f.write(code)
                    f.write("\n```\n")
                
                processed_files.append(file)

    con.close()
    
    end_time = time.time()
    elapsed_time = end_time - start_time

    print("-" * 30)
    print(f"完了！")
    print(f"処理ファイル数: {len(processed_files)} 個")
    print(f"実行時間: {elapsed_time:.2f} 秒")
    print(f"DB保存先: {db_full_path}")
    print(f"Obsidian出力先: {OBSIDIAN_DIR}")
    print("-" * 30)

if __name__ == "__main__":
    run()