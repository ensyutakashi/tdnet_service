import duckdb
import os

def check_tables():
    db_path = r"\\LS720D7A9\TakashiBK\投資\TDNET\TDnet適時情報開示サービス\tdnet.duckdb"

    if not os.path.exists(db_path):
        print(f"エラー: ファイルが見つかりません: {db_path}")
        return

    try:
        con = duckdb.connect(database=db_path, read_only=True)
        
        # すべてのテーブルを表示
        tables = con.execute("SHOW TABLES").fetchall()
        print("データベース内のテーブル:")
        for table in tables:
            print(f"  - {table[0]}")
        
        # 各テーブルの構造を表示
        for table in tables:
            table_name = table[0]
            print(f"\n--- テーブル '{table_name}' の構造 ---")
            columns = con.execute(f"DESCRIBE {table_name}").fetchall()
            for col in columns:
                print(f"  {col[0]}: {col[1]}")
            
            # データ数を表示
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"  データ数: {count}件")
            
            # サンプルデータを表示（最初の3行）
            if count > 0:
                print(f"  サンプルデータ:")
                sample = con.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
                for i, row in enumerate(sample, 1):
                    print(f"    {i}: {row}")

    except Exception as e:
        print(f"エラー: {e}")
    
    finally:
        if 'con' in locals():
            con.close()

if __name__ == "__main__":
    check_tables()
