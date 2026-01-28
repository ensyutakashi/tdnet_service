import os
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import time
import win32com.client

# ================= config =================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)

EXCEL_FILE = os.path.join(BASE_DIR, "TDnet適時開示情報.xlsm")
SHEET_NAME = '適時開示情報'

PDF_FOLDER = os.path.join(BASE_DIR, "TDnet(決算短信)-PDF-随時追加分")
XBRL_FOLDER = os.path.join(BASE_DIR, "TDnet(決算短信)XBRL-随時追加分")

START_ROW_INDEX = 40287
MAX_WORKERS = 15
# ==========================================

def get_timestamp_msg(msg):
    return f"{msg} {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}"

def download_file(url, save_path):
    try:
        if not url or not str(url).startswith('http'):
            return "失敗: URL不正"
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                f.write(response.content)
            return "成功" if os.path.getsize(save_path) > 0 else "失敗: 空ファイル"
        return f"失敗: ステータス {response.status_code}"
    except Exception as e:
        return f"失敗: {str(e)}"

def main():
    start_time = time.time()
    os.makedirs(PDF_FOLDER, exist_ok=True)
    os.makedirs(XBRL_FOLDER, exist_ok=True)

    print(f"Excelを制御しています... (win32com使用)")
    try:
        excel = win32com.client.GetActiveObject("Excel.Application")
    except:
        excel = win32com.client.Dispatch("Excel.Application")
    
    excel.Visible = True 
    
    try:
        wb = excel.Workbooks.Open(EXCEL_FILE)
    except Exception as e:
        print(f"エラー: Excelファイルを開けませんでした。{e}")
        return

    ws = wb.Worksheets(SHEET_NAME)
    # 最終行を取得
    max_row = ws.Cells(ws.Rows.Count, "A").End(-4162).Row # -4162 = xlUp

    # --- 列番号の定義 (実際のExcelに合わせました) ---
    # A=1, B=2, C=3, D=4, E=5, ...
    COL_FILENAME = 24 # X列: ファイル名(連番+...)
    COL_PDF_URL = 5   # E列: PDF(ハイパーリンク)
    COL_PDF_RES = 13  # M列: pdfDL
    COL_XBRL_URL = 14 # N列: XBRL(ハイパーリンク)
    COL_XBRL_RES = 15 # O列: xbrlDL
    # ----------------------------------------------

    print(f"処理を開始します (開始行: {START_ROW_INDEX} から {max_row} まで)")

    pdf_count = 0
    xbrl_count = 0

    # 行のリストを作成
    row_indices = list(range(START_ROW_INDEX, max_row + 1))

    def process_row(r):
        nonlocal pdf_count, xbrl_count
        
        # 1. PDFダウンロード
        pdf_res_val = ws.Cells(r, COL_PDF_RES).Value
        if pdf_res_val is None or str(pdf_res_val).strip() == "":
            url_cell = ws.Cells(r, COL_PDF_URL)
            url = url_cell.Hyperlinks(1).Address if url_cell.Hyperlinks.Count > 0 else url_cell.Value
            fname = ws.Cells(r, COL_FILENAME).Value
            
            if fname and url and str(url).startswith("http"):
                if not str(fname).lower().endswith('.pdf'): fname = str(fname) + '.pdf'
                save_path = os.path.join(PDF_FOLDER, fname)
                res = download_file(url, save_path)
                ws.Cells(r, COL_PDF_RES).Value = get_timestamp_msg(res)
                if "成功" in res: pdf_count += 1

        # 2. XBRLダウンロード
        xbrl_res_val = ws.Cells(r, COL_XBRL_RES).Value
        if xbrl_res_val is None or str(xbrl_res_val).strip() == "":
            url_cell = ws.Cells(r, COL_XBRL_URL)
            url = url_cell.Hyperlinks(1).Address if url_cell.Hyperlinks.Count > 0 else url_cell.Value
            fname = ws.Cells(r, COL_FILENAME).Value
            
            if fname and url and str(url).startswith("http"):
                fname_zip = str(fname).replace('.pdf', '').replace('.PDF', '') + '.zip'
                save_path = os.path.join(XBRL_FOLDER, fname_zip)
                res = download_file(url, save_path)
                ws.Cells(r, COL_XBRL_RES).Value = get_timestamp_msg(res)
                if "成功" in res: xbrl_count += 1
        
        if r % 100 == 0:
            print(f"進捗: {r} 行目をチェック中...")

    # 並列実行
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(process_row, row_indices)

    # 上書き保存
    wb.Save()
    print(f"\n成功: {os.path.basename(EXCEL_FILE)} に直接書き込み保存しました。")

    elapsed_sec = int(time.time() - start_time)
    print("\n" + "="*40)
    print(f" 処理完了！ ({elapsed_sec // 60}分 {elapsed_sec % 60}秒)")
    print(f" 新規PDF: {pdf_count} 件 / 新規XBRL: {xbrl_count} 件")
    print("="*40)

if __name__ == "__main__":
    main()