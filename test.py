import duckdb
import glob
import os
import time
from datetime import timedelta
from pathlib import Path

# ==========================================
# 設定情報
# ==========================================
input_path = r'\\LS720D7A9\TakashiBK\投資\無尽蔵日別株価\RawData無尽蔵日別株価(2020-2025年RawData)\*.csv'
output_file = 'stock_data.parquet'
error_log_file = 'import_errors.csv'

def main():
    start_time = time.time()
    print("--- プロセスを開始します ---")
    print(f"開始時刻: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")

    # 1. ファイル一覧を取得
    files = glob.glob(input_path)
    if not files:
        print("エラー: ファイルが見つかりません。")
        return

    num_files = len(files)
    print(f"読み込み対象ファイル数: {num_files} 件")

    # 2. DuckDBの準備
    con = duckdb.connect()
    try:
        con.execute("INSTALL encodings; LOAD encodings;")
        
        # 最初に空のParquetファイル（またはテンポラリテーブル）を作るための準備
        # 1つ目のファイルをベースにテーブルを作成
        first_file = Path(files[0]).as_posix()
        
        print("\n処理を開始します...")
        
        # エラー記録用リスト
        error_list = []
        processed_count = 0
        
        # 初回フラグ（初回のみテーブル作成、2回目以降は挿入）
        first_run = True

        for f in files:
            file_path = Path(f).as_posix()
            processed_count += 1
            
            try:
                # 1ファイルずつ読み込んで一時テーブル（all_data）に追加
                if first_run:
                    con.execute(f"""
                        CREATE TABLE all_data AS 
                        SELECT * FROM read_csv_auto('{file_path}', encoding='shift_jis', all_varchar=True);
                    """)
                    first_run = False
                else:
                    con.execute(f"""
                        INSERT INTO all_data 
                        SELECT * FROM read_csv_auto('{file_path}', encoding='shift_jis', all_varchar=True);
                    """)
                
                # 100ファイルごとに進捗表示
                if processed_count % 100 == 0:
                    current_duration = timedelta(seconds=int(time.time() - start_time))
                    print(f"進捗: {processed_count}/{num_files} 完了... (経過時間: {current_duration})")

            except Exception as e:
                # エラーが起きたファイルと内容を記録
                error_list.append({"file": f, "error": str(e)})

        # 3. まとめてParquetに出力
        print("\nParquetファイルに書き出し中...")
        con.execute(f"COPY all_data TO '{output_file}' (FORMAT PARQUET);")
        
        # 4. エラーログの保存
        if error_list:
            import pandas as pd
            errors_df = pd.DataFrame(error_list)
            errors_df.to_csv(error_log_file, index=False, encoding='utf-8-sig')
            print(f"⚠️ {len(error_list)} 件のファイルでエラーが発生しました。詳細は '{error_log_file}' を確認してください。")
        else:
            print("✅ すべてのファイルが正常に処理されました。")

        # 最終統計
        end_time = time.time()
        total_duration = timedelta(seconds=int(end_time - start_time))
        print("="*40)
        print(f"完了！ 出力先: {output_file}")
        print(f"合計経過時間: {total_duration}")
        print("="*40)

    except Exception as e:
        print(f"\n❌ 致命的なエラーが発生しました:\n{e}")
    finally:
        con.close()

if __name__ == "__main__":
    main()