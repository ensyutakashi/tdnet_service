import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import time
import win32com.client
import pythoncom

# ================= config =================
EXCEL_FILE = r'\\LS720D7A9\TakashiBK\投資\TDNET\TDnet適時情報開示サービス\TDnet適時開示情報.xlsm'
BASE_DIR = os.path.dirname(EXCEL_FILE)
SHEET_NAME = '適時開示情報'
PDF_FOLDER = os.path.join(BASE_DIR, "TDnet(決算短信)-PDF-随時追加分")
XBRL_FOLDER = os.path.join(BASE_DIR, "TDnet(決算短信)XBRL-随時追加分")
START_ROW_INDEX = 41652
MAX_WORKERS = 15
# ==========================================

def get_timestamp_msg(msg):
    return f"{msg} {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}"

def download_file(url, save_path):
    try:
        if not url or not str(url).startswith('http'):
            return "失敗: URL不正"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, timeout=30, headers=headers)
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                f.write(response.content)
            return "成功" if os.path.getsize(save_path) > 0 else "失敗: 空ファイル"
        return f"失敗: ステータス {response.status_code}"
    except Exception as e:
        return f"失敗: {str(e)}"

def main():
    script_start_time = time.time() # 全体開始時間
    print(f"--- スクリプト開始 [{datetime.now().strftime('%H:%M:%S')}] ---")
    
    os.makedirs(PDF_FOLDER, exist_ok=True)
    os.makedirs(XBRL_FOLDER, exist_ok=True)

    pythoncom.CoInitialize()
    excel = None
    wb = None
    
    try:
        # Excel接続
        try:
            excel = win32com.client.GetActiveObject("Excel.Application")
        except:
            excel = win32com.client.Dispatch("Excel.Application")
            excel.Visible = True
        
        # ブック取得
        wb = None
        for open_wb in excel.Workbooks:
            if open_wb.FullName.lower() == EXCEL_FILE.lower():
                wb = open_wb
                break
        if not wb:
            wb = excel.Workbooks.Open(EXCEL_FILE)

        ws = wb.Worksheets(SHEET_NAME)
        max_row = ws.Cells(ws.Rows.Count, "A").End(-4162).Row

        # 列特定
        headers = ws.Rows(1).Value[0]
        def find_col(name, default):
            try: return headers.index(name) + 1
            except: return default

        COL_PDF_URL = find_col("表題", 4)
        COL_XBRL_URL = find_col("XBRL", 5)
        COL_FILENAME = find_col("ファイル名(連番+公開日+時刻+(種別)+決算月+4Q+コード+会社名+表題)", 13)
        COL_PDF_RES = find_col("pdfDL", 14)
        COL_XBRL_RES = find_col("xbrlDL", 15)

        # タスク抽出
        tasks = []
        for r in range(START_ROW_INDEX, max_row + 1):
            fname = ws.Cells(r, COL_FILENAME).Value
            p_res = ws.Cells(r, COL_PDF_RES).Value
            x_res = ws.Cells(r, COL_XBRL_RES).Value
            
            p_url = None
            if not p_res or str(p_res).strip() in ["", "None"]:
                c = ws.Cells(r, COL_PDF_URL)
                p_url = c.Hyperlinks(1).Address if c.Hyperlinks.Count > 0 else c.Value
            
            x_url = None
            if not x_res or str(x_res).strip() in ["", "None"]:
                c = ws.Cells(r, COL_XBRL_URL)
                x_url = c.Hyperlinks(1).Address if c.Hyperlinks.Count > 0 else c.Value

            if (p_url or x_url) and fname:
                tasks.append({"row": r, "fname": fname, "p_url": p_url, "x_url": x_url})

        if not tasks:
            print("処理対象の新規データはありません。")
            return

        # ダウンロード実行
        print(f"ダウンロード開始: {len(tasks)}件 (並列数:{MAX_WORKERS})")
        dl_start_time = time.time()
        
        results = []
        def execute_task(t):
            res = {"row": t["row"], "p_msg": None, "x_msg": None}
            if t["p_url"] and str(t["p_url"]).startswith("http"):
                fn = str(t["fname"]) if str(t["fname"]).lower().endswith(".pdf") else f"{t['fname']}.pdf"
                res["p_msg"] = get_timestamp_msg(download_file(t["p_url"], os.path.join(PDF_FOLDER, fn)))
            if t["x_url"] and str(t["x_url"]).startswith("http"):
                fn = str(t["fname"]).replace(".pdf", "").replace(".PDF", "") + ".zip"
                res["x_msg"] = get_timestamp_msg(download_file(t["x_url"], os.path.join(XBRL_FOLDER, fn)))
            return res

        # 進捗表示付きで実行
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_task = {executor.submit(execute_task, t): t for t in tasks}
            completed_count = 0
            for future in as_completed(future_to_task):
                results.append(future.result())
                completed_count += 1
                if completed_count % 10 == 0 or completed_count == len(tasks):
                    print(f"  進捗: {completed_count}/{len(tasks)} 件完了...", end="\r")

        dl_end_time = time.time()
        print(f"\nダウンロード完了。Excelに書き込んでいます...")

        # 書き込み
        excel.ScreenUpdating = False
        p_cnt, x_cnt = 0, 0
        for r in results:
            if r["p_msg"]:
                ws.Cells(r["row"], COL_PDF_RES).Value = r["p_msg"]
                if "成功" in r["p_msg"]: p_cnt += 1
            if r["x_msg"]:
                ws.Cells(r["row"], COL_XBRL_RES).Value = r["x_msg"]
                if "成功" in r["x_msg"]: x_cnt += 1
        
        wb.Save()
        
        # --- 時間計算 ---
        total_elapsed = time.time() - script_start_time
        dl_elapsed = dl_end_time - dl_start_time
        avg_speed = dl_elapsed / len(tasks) if tasks else 0

        print("\n" + "="*45)
        print(f" 【処理結果概要】")
        print(f"  総実行時間　: {int(total_elapsed // 60)}分 {int(total_elapsed % 60)}秒")
        print(f"  DL純処理時間: {int(dl_elapsed // 60)}分 {int(dl_elapsed % 60)}秒")
        print(f"  平均DL速度  : {avg_speed:.2f} 秒/件")
        print(f"  新規PDF取得 : {p_cnt} 件")
        print(f"  新規XBRL取得: {x_cnt} 件")
        print("="*45)

    except Exception as e:
        print(f"\n致命的なエラー: {e}")
    finally:
        if excel: excel.ScreenUpdating = True
        ws = None; wb = None; excel = None
        pythoncom.CoUninitialize()
        print(f"--- スクリプト終了 [{datetime.now().strftime('%H:%M:%S')}] ---")

if __name__ == "__main__":
    main()