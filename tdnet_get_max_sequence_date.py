import duckdb
import os
import sys
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup
import csv
import json

BASE = "https://www.release.tdnet.info/inbs/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}

def fetch_page_html(sess: requests.Session, page_path: str) -> Optional[str]:
    """ページHTML取得（簡易リトライ）。成功時は文字列、失敗時 None。"""
    url = BASE + page_path
    for i in range(3):  # リトライ 3回
        try:
            r = sess.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 200 and "main-list-table" in r.text:
                r.encoding = r.apparent_encoding or "utf-8"
                return r.text
        except requests.RequestException:
            pass
        time.sleep(1 + i)
    return None

def parse_rows(html: str, date_str: str) -> List[Dict]:
    """main-list-table をパースして、行辞書のリストを返す。"""
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", id="main-list-table")
    rows: List[Dict] = []
    if not table:
        return rows

    pub_date = datetime.strptime(date_str, "%Y%m%d").date()
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 7:
            continue

        # 0:時刻, 1:コード, 2:会社名
        t_time = tds[0].get_text(strip=True)
        t_code = tds[1].get_text(strip=True)
        t_name = tds[2].get_text(strip=True)

        # 3:表題（PDFリンク）
        title_td = tds[3]
        a = title_td.find("a")
        title_txt = title_td.get_text(strip=True)
        pdf_url = BASE + a["href"].lstrip("./") if (a and a.get("href")) else None

        # 4:XBRL（zipリンクがあるときのみ）
        x_td = tds[4]
        xa = x_td.find("a")
        x_url = BASE + xa["href"].lstrip("./") if (xa and xa.get("href")) else None
        x_text = "XBRL" if x_url else ""

        # 5:上場取引所, 6:更新履歴
        place = tds[5].get_text(strip=True)
        hist = tds[6].get_text(strip=True)

        # 時刻をDuckDBのTIMESTAMPフォーマットに変換（公開日+時刻）
        time_obj = datetime.strptime(t_time, "%H:%M")
        full_timestamp = datetime.combine(pub_date, time_obj.time())
        
        # 秒まで含めたフォーマット（2026-01-28 18:30:00）
        formatted_time = full_timestamp.strftime('%Y-%m-%d %H:%M:%S')

        rows.append({
            "時刻": formatted_time,
            "コード": t_code, 
            "会社名": t_name,
            "表題": title_txt, 
            "表題URL": pdf_url,
            "XBRL": x_text, 
            "XBRLURL": x_url,
            "上場取引所": place, 
            "更新履歴": hist,
            "公開日": pub_date.strftime('%Y-%m-%d'),
        })
    return rows

def scrape_one_day(date_str: str) -> List[Dict]:
    """指定日の全ページ（100件単位）を走査して結合。"""
    sess = requests.Session()
    all_rows: List[Dict] = []
    page = 1
    while True:
        page_path = f"I_list_{page:03d}_{date_str}.html"
        html = fetch_page_html(sess, page_path)
        if not html:
            break
        rows = parse_rows(html, date_str)
        if not rows:
            break
        all_rows.extend(rows)
        page += 1
        if page > 50:
            break
    return all_rows

def get_max_sequence_date():
    db_path = r"C:\Users\ensyu\Documents\Speculation\TDnet\TDnet適時情報開示サービス\tdnet.duckdb"
    if not os.path.exists(db_path):
        print(f"エラー: ファイルが見つかりません: {db_path}")
        return None

    try:
        con = duckdb.connect(database=db_path, read_only=True)
        query = """
        SELECT 公開日, 連番, 会社名, 表題
        FROM disclosure_info 
        WHERE 連番 = (SELECT MAX(連番) FROM disclosure_info)
        """
        result = con.execute(query).fetchone()
        con.close()
        
        if result:
            print(f"連番の最大値: {result[1]}")
            print(f"公開日: {result[0]}")
            print(f"会社名: {result[2]}")
            print(f"表題: {result[3]}")
            return result[0]
        return None
    except Exception as e:
        print(f"エラー: {e}")
        return None

def download_data_for_date(target_date):
    if isinstance(target_date, str):
        target_date = datetime.strptime(target_date, '%Y-%m-%d').date()
    date_str = target_date.strftime('%Y%m%d')
    print(f"\n=== {date_str} のデータをダウンロード ===")
    try:
        day_start = time.time()
        rows = scrape_one_day(date_str)
        day_end = time.time()
        if rows:
            print(f"{date_str} 取得完了 ({len(rows)}件, {day_end - day_start:.2f}秒)")
            return rows
        return []
    except Exception as e:
        print(f"エラー: {e}")
        return []

def download_data_since_date(start_date):
    """指定日以降の全データを取得"""
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    
    all_data = []
    current_date = start_date
    today = datetime.now().date()
    
    while current_date <= today:
        date_str = current_date.strftime('%Y%m%d')
        print(f"\n=== {date_str} のデータをダウンロード ===")
        try:
            day_start = time.time()
            rows = scrape_one_day(date_str)
            day_end = time.time()
            if rows:
                all_data.extend(rows)
                print(f"{date_str} 取得完了 ({len(rows)}件, {day_end - day_start:.2f}秒)")
            else:
                print(f"{date_str} データなし")
        except Exception as e:
            print(f"{date_str} エラー: {e}")
        
        current_date += timedelta(days=1)
        
        # 1日の最大ループ防止
        if current_date > start_date + timedelta(days=365):
            print("警告: 365日を超えるため処理を停止します")
            break
    
    return all_data

def get_count_from_db(target_date):
    db_path = r"C:\Users\ensyu\Documents\Speculation\TDnet\TDnet適時情報開示サービス\tdnet.duckdb"
    if not os.path.exists(db_path):
        return None
    try:
        con = duckdb.connect(database=db_path, read_only=True)
        query = "SELECT COUNT(*) FROM disclosure_info WHERE 公開日 = ?"
        result = con.execute(query, [target_date]).fetchone()
        con.close()
        return result[0] if result else 0
    except Exception as e:
        print(f"データベースエラー: {e}")
        return None

def save_tdnet_data_to_csv(new_data: List[Dict], target_date: str):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"TDNET抽出データ_{target_date}_{timestamp}.csv"
    try:
        with open(csv_filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['時刻', 'コード', '会社名', '表題', '表題URL', 'XBRL', 'XBRLURL', '上場取引所', '更新履歴', '公開日'])
            for row in new_data:
                writer.writerow([row["時刻"], row["コード"], row["会社名"], row["表題"], row.get("表題URL", ""), row["XBRL"], row.get("XBRLURL", ""), row["上場取引所"], row["更新履歴"], row["公開日"]])
        print(f"✅ TDnet抽出データを保存しました: {csv_filename}")
        return csv_filename
    except Exception as e:
        print(f"TDnetデータ保存エラー: {e}")
        return None

def save_db_data_to_csv(target_date: str, db_path: str):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"DB比較対象データ_{target_date}_{timestamp}.csv"
    try:
        con = duckdb.connect(database=db_path, read_only=True)
        # すべての列を出力
        query = """
        SELECT * FROM disclosure_info 
        WHERE 公開日 = ?
        """
        db_records = con.execute(query, [target_date]).fetchall()
        
        # 列名を取得
        columns = [desc[0] for desc in con.description]
        con.close()
        
        with open(csv_filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)  # すべての列名を書き込み
            for record in db_records:
                writer.writerow(record)
        print(f"✅ DB比較対象データ（全列）を保存しました: {csv_filename}")
        return csv_filename
    except Exception as e:
        print(f"DBデータ保存エラー: {e}")
        return None

def get_diff_only(new_data: List[Dict], target_date: str, db_path: str) -> List[Dict]:
    """DLデータとDBデータを比較して差分リストのみを返す（保存はしない）"""
    if not new_data:
        return []
    try:
        con = duckdb.connect(database=db_path, read_only=True)
        query = """
        SELECT 
            strftime(時刻, '%Y-%m-%d %H:%M:%S'), 
            コード, 会社名, 表題, 表題リンク, 公開日
        FROM disclosure_info 
        WHERE 公開日 = ?
        """
        db_records = con.execute(query, [target_date]).fetchall()
        con.close()
        
        if not db_records:
            return new_data
        
        db_set = set()
        for record in db_records:
            db_set.add((
                str(record[0]),
                str(record[1]) if record[1] else "",
                str(record[2]) if record[2] else "",
                str(record[3]) if record[3] else "",
                str(record[4]) if record[4] else "",
                str(record[5]) if record[5] else ""
            ))
        
        diff_data = []
        for row in new_data:
            comparison_key = (
                str(row["時刻"]),
                str(row["コード"]),
                str(row["会社名"]),
                str(row["表題"]),
                str(row.get("表題URL", "")),
                str(row["公開日"])
            )
            if comparison_key not in db_set:
                diff_data.append(row)
        return diff_data
    except Exception as e:
        print(f"比較エラー: {e}")
        return []

def get_max_sequence_number(db_path: str) -> int:
    """連番の最大値を取得"""
    try:
        con = duckdb.connect(database=db_path, read_only=True)
        result = con.execute("SELECT MAX(連番) FROM disclosure_info").fetchone()
        con.close()
        return result[0] if result and result[0] else 0
    except Exception as e:
        print(f"連番取得エラー: {e}")
        return 0

def get_db_columns(db_path: str) -> List[str]:
    """DBの列名を取得"""
    try:
        con = duckdb.connect(database=db_path, read_only=True)
        result = con.execute("SELECT * FROM disclosure_info LIMIT 1")
        columns = [desc[0] for desc in result.description]
        con.close()
        return columns
    except Exception as e:
        print(f"列名取得エラー: {e}")
        return []

def save_diff_to_csv(diff_data: List[Dict], date_str: str, data_type: str, db_path: str):
    """データをソートしてDB列構成でCSV保存（連番付与）"""
    if not diff_data:
        print(f"✅ {data_type}はありません")
        return None

    # DB列構成を取得
    db_columns = get_db_columns(db_path)
    if not db_columns:
        print("DB列構成の取得に失敗しました")
        return None
    
    # 連番の最大値を取得
    max_seq = get_max_sequence_number(db_path)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"TDNET_{date_str}_{data_type}_{timestamp}.csv"
    try:
        # 時刻,コード,会社名,表題,表題URLの昇順でソート
        sorted_data = sorted(diff_data, key=lambda x: (
            x["公開日"],
            x["時刻"], 
            x["コード"], 
            x["会社名"], 
            x["表題"], 
            x.get("表題URL", "")
        ))
        
        with open(csv_filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(db_columns)  # DB列構成でヘッダー出力
            
            for i, row in enumerate(sorted_data):
                # 連番を付与（MAX+1から開始）
                current_seq = max_seq + i + 1
                
                # DB列構成に合わせてデータを作成
                csv_row = []
                for col in db_columns:
                    if col == "連番":
                        csv_row.append(current_seq)
                    elif col == "時刻":
                        csv_row.append(row["時刻"])
                    elif col == "コード":
                        csv_row.append(row["コード"])
                    elif col == "会社名":
                        csv_row.append(row["会社名"])
                    elif col == "表題":
                        csv_row.append(row["表題"])
                    elif col == "表題_URL":  
                        csv_row.append(row.get("表題URL", ""))
                    elif col == "XBRL":
                        csv_row.append(row["XBRL"])
                    elif col == "XBRL_URL":  
                        csv_row.append(row.get("XBRLURL", ""))
                    elif col == "上場取引所":
                        csv_row.append(row["上場取引所"])
                    elif col == "更新履歴":
                        csv_row.append(row["更新履歴"])
                    elif col == "公開日":
                        csv_row.append(row["公開日"])
                    else:
                        # その他の列はブランク
                        csv_row.append("")
                
                writer.writerow(csv_row)
        
        print(f"✅ {data_type}をDB列構成で保存しました（連番: {max_seq+1}〜{max_seq+len(sorted_data)}）: {csv_filename}")
        return csv_filename
    except Exception as e:
        print(f"CSV保存エラー: {e}")
        return None

def main():
    print("=== 連番最大値の日付取得と差分抽出（全期間ソート版） ===")
    db_path = r"C:\Users\ensyu\Documents\Speculation\TDnet\TDnet適時情報開示サービス\tdnet.duckdb"
    max_date = get_max_sequence_date()
    
    if not max_date:
        print("データベースから日付を取得できませんでした")
        return
    
    max_date_str = max_date.strftime('%Y-%m-%d') if isinstance(max_date, datetime) else str(max_date).split()[0]
    print(f"max_date: {max_date_str}")
    
    # max_date以降の全データを取得
    print(f"\n=== {max_date_str} 以降の全データを取得開始 ===")
    all_new_data = download_data_since_date(max_date_str)
    
    if all_new_data:
        print(f"\n=== 取得結果 ===")
        print(f"総取得件数: {len(all_new_data)}件")
        
        # 1. max_date当日の差分を抽出
        max_date_data = [row for row in all_new_data if row["公開日"] == max_date_str]
        diff_in_max_date = get_diff_only(max_date_data, max_date_str, db_path)
        
        # 2. max_dateより後の新規データを抽出
        after_max_date_data = [row for row in all_new_data if row["公開日"] > max_date_str]
        
        # 3. 全ての対象データ（差分 + 新規分）を統合
        final_diff_list = diff_in_max_date + after_max_date_data
        
        if final_diff_list:
            # 一括ソートして保存
            save_diff_to_csv(final_diff_list, max_date_str, "更新分差分データ", db_path)
        else:
            print("✅ 新規または差分データはありません")

        # レポート表示用
        db_count = get_count_from_db(max_date_str)
        if db_count is not None:
            print(f"\n=== {max_date_str} の状況 ===")
            print(f"DB件数: {db_count}件 / DL件数: {len(max_date_data)}件 / 差分: {len(max_date_data) - db_count:+d}件")

        # 個別確認用ファイルの出力（任意）
        save_tdnet_data_to_csv(max_date_data, max_date_str)
        save_db_data_to_csv(max_date_str, db_path)

    else:
        print("データが取得できませんでした")

if __name__ == "__main__":
    main()