import win32com.client
import os
import pythoncom

# ================= config =================
# ファイルの場所（ネットワークパス）
EXCEL_FILE = r'\\LS720D7A9\TakashiBK\投資\TDNET\TDnet適時情報開示サービス\TDnet適時開示情報.xlsm'
SHEET_NAME = '適時開示情報'
START_ROW = 41650  # 処理を開始する行
COL_M = 13  # 対象列 (M列)
COL_P = 16  # 記録列 (P列)

# Excelの定数定義
xlUp = -4162 
# ==========================================

def convert_forbidden_chars():
    print(f"処理を開始します: {os.path.basename(EXCEL_FILE)}")
    
    # 禁則文字の定義
    mapping = {
        "/": "／", "\\": "￥", ":": "：", "*": "＊", 
        "?": "？", '"': "＂", "<": "＜", ">": "＞", "|": "｜"
    }
    
    # COMの初期化（ネットワーク越しやスレッド処理での安定化のため）
    pythoncom.CoInitialize()
    
    excel = None
    wb = None
    
    try:
        try:
            # 既にExcelが開いているか確認
            excel = win32com.client.GetActiveObject("Excel.Application")
        except:
            # 開いていなければ新しく起動
            excel = win32com.client.Dispatch("Excel.Application")
        
        excel.Visible = True
        excel.DisplayAlerts = False

        # ブックを開く
        wb = excel.Workbooks.Open(EXCEL_FILE)
        ws = wb.Worksheets(SHEET_NAME)

        # 最終行の取得（xlUpを数値 -4162 で代用）
        last_row = ws.Cells(ws.Rows.Count, COL_M).End(xlUp).Row

        if last_row < START_ROW:
            print(f"処理対象の行が見つかりませんでした。 (最終行: {last_row})")
            return

        print(f"最終行: {last_row} (処理範囲: {START_ROW}行目 〜)")

        # データの読み込み
        m_range = ws.Range(ws.Cells(START_ROW, COL_M), ws.Cells(last_row, COL_M))
        m_values = m_range.Value

        new_m_values = []
        p_values = []
        change_count = 0

        # メモリ上での変換処理
        for i, val in enumerate(m_values):
            # valはタプル(値,)なので[0]を取り出す
            original_text = str(val[0]) if val[0] is not None else ""
            replaced_text = original_text
            changed_chars = []

            for half, full in mapping.items():
                if half in replaced_text:
                    replaced_text = replaced_text.replace(half, full)
                    changed_chars.append(half)

            if replaced_text != original_text:
                new_m_values.append([replaced_text])
                p_values.append([",".join(changed_chars)])
                change_count += 1
            else:
                new_m_values.append([original_text])
                p_values.append([None])

            if (i + 1) % 1000 == 0:
                print(f"  進捗: {START_ROW + i} / {last_row} 行目処理中...")

        # Excelへの書き戻し
        if new_m_values:
            ws.Range(ws.Cells(START_ROW, COL_M), ws.Cells(last_row, COL_M)).Value = new_m_values
            ws.Range(ws.Cells(START_ROW, COL_P), ws.Cells(last_row, COL_P)).Value = p_values

        wb.Save()
        print("-" * 30)
        print(f"完了しました。")
        print(f"修正行数: {change_count} 行")
        print("-" * 30)

    except Exception as e:
        print(f"エラーが発生しました: {e}")
    finally:
        # 終了処理
        excel.DisplayAlerts = True
        pythoncom.CoUninitialize()

if __name__ == "__main__":
    convert_forbidden_chars()