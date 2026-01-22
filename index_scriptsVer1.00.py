import os
import sqlite3
import duckdb
from datetime import datetime

# --- 設定 ---
#SOURCE_DIR = r"C:\path\to\your\scripts"  # Pythonファイルが入っているフォルダ
#OBSIDIAN_DIR = r"C:\path\to\your\obsidian\vault\Scripts_Catalog" # 出力先

SOURCE_DIR = r"\\LS720D7A9\TakashiBK\投資\MyPython"  # Pythonファイルが入っているフォルダ
OBSIDIAN_DIR = r"C:\Users\ensyu\Documents\Obsidian保管庫\ensyu_star_capital" # 出力先



DB_PATH = "script_index.db"

def extract_info(file_path):
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()
        content = "".join(lines)
        # 1行目のコメントを取得（# または """）
        description = "説明なし"
        if lines:
            first_line = lines[0].strip()
            if first_line.startswith(("#", '"""', "'''")):
                description = first_line.replace("#", "").replace('"""', "").strip()
        return description, content

def run():
    if not os.path.exists(OBSIDIAN_DIR):
        os.makedirs(OBSIDIAN_DIR)

    # DuckDBのセットアップ
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS scripts (path TEXT PRIMARY KEY, name TEXT, description TEXT, content TEXT, last_updated TIMESTAMP)")

    for root, _, files in os.walk(SOURCE_DIR):
        for file in files:
            if file.endswith(".py") and file != "index_scripts.py":
                full_path = os.path.join(root, file)
                desc, code = extract_info(full_path)
                mtime = datetime.fromtimestamp(os.path.getmtime(full_path))

                # DBに保存（更新があれば上書き）
                con.execute("""
                    INSERT OR REPLACE INTO scripts (path, name, description, content, last_updated)
                    VALUES (?, ?, ?, ?, ?)
                """, (full_path, file, desc, code, mtime))

                # Obsidian用の個別ノート作成
                md_filename = f"{file}.md"
                with open(os.path.join(OBSIDIAN_DIR, md_filename), "w", encoding="utf-8") as f:
                    f.write(f"# {file}\n\n")
                    f.write(f"- **場所**: `{full_path}`\n")
                    f.write(f"- **最終更新**: {mtime}\n")
                    f.write(f"- **概要**: {desc}\n\n")
                    f.write(f"## コード\n```python\n{code}\n```\n")

    con.close()
    print(f"完了！ {OBSIDIAN_DIR} を確認してください。")

if __name__ == "__main__":
    run()