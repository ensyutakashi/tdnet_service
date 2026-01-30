import duckdb
import os
import time
from datetime import datetime

def get_latest_tdnet_data(max_retries=3, retry_delay=1):
    db_path = r"\\LS720D7A9\TakashiBK\投資\TDNET\TDnet適時情報開示サービス\tdnet.duckdb"

    if not os.path.exists(db_path):
        print(f"エラー: ファイルが見つかりません: {db_path}")
        return

    con = None
    for attempt in range(1, max_retries + 1):
        try:
            # 他のプロセスが使用中でも読み取りを試行
            con = duckdb.connect(
                database=db_path, 
                read_only=True,
                config={
                    'access_mode': 'READ_ONLY'
                }
            )

            query = """
            SELECT 
                "公開日", 
                "時刻", 
                "コード", 
                "会社名", 
                "表題" 
            FROM disclosure_info 
            ORDER BY "公開日" DESC, "時刻" DESC 
            LIMIT 1
            """

            result = con.execute(query).fetchone()

            if result:
                koukai_bi, shikoku, code, company, title = result
                # 検索用データとして一つにまとめる
                search_data = f"{koukai_bi}&{shikoku}&{code}&{company}&{title}"
                
                print("\n--- 最新の適時開示情報 ---")
                print(f"検索用データ: {search_data}")
                print("\n--- 詳細情報 ---")
                print(f"公開日  : {koukai_bi}")
                print(f"時刻    : {shikoku}")
                print(f"コード  : {code}")
                print(f"会社名  : {company}")
                print(f"表題    : {title}")
                
                # 検索用データを返す
                return search_data
            else:
                print("データが見つかりませんでした。")
            
            # 成功したらループを抜ける
            break

        except Exception as e:
            if attempt < max_retries:
                wait_time = retry_delay * attempt
                print(f"\nリトライ中... ({attempt}/{max_retries}) - {datetime.now().strftime('%H:%M:%S')}")
                print(f"エラー: {str(e)}")
                print(f"{wait_time}秒後に再試行します...")
                time.sleep(wait_time)
            else:
                print("\nエラー: データベースに接続できませんでした。")
                print("以下の点を確認してください:")
                print("1. DBeaverでデータベースが開かれていないか確認してください")
                print("2. 他のアプリケーションでファイルがロックされていないか確認してください")
                print(f"最終エラー: {str(e)}")
    
        finally:
            if con is not None:
                try:
                    con.close()
                except:
                    pass  # クローズに失敗しても無視

if __name__ == "__main__":
    get_latest_tdnet_data(max_retries=5, retry_delay=2)  # 最大5回、2秒間隔でリトライ