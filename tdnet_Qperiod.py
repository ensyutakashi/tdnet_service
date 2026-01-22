
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TDnet適時開示情報.xlsm の決算期・四半期自動判定スクリプト
ルール: 〈依頼プロンプト〉TDnet適時開示 決算期・四半期取得.md に準拠
追加機能: J列（種別）の自動判定
"""

import re
import unicodedata
import time  # 経過時間計測用に追加
from datetime import datetime
from calendar import monthrange
import openpyxl
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, PatternFill, Border, Side

# 元号→西暦の変換マッピング
ERA_TO_YEAR = {
    '令和': 2018,
    '平成': 1988,
    '昭和': 1925,
    '大正': 1911,
    '明治': 1867
}

def normalize_text(text):
    """
    NFKC正規化で全角→半角変換、連続スペースを1個に圧縮
    漢数字の全体置換は禁止（パターン内限定変換のみ）
    """
    if not text or not isinstance(text, str):
        return ""
    # NFKC正規化（全角→半角）
    normalized = unicodedata.normalize('NFKC', text)
    # 連続スペースを1個に圧縮
    normalized = re.sub(r'\s+', ' ', normalized)
    return normalized.strip()

def era_to_western(era_name, era_year_str):
    """
    元号年を西暦に変換
    例: 令和7年 → 2025年
    """
    base_year = ERA_TO_YEAR.get(era_name)
    if base_year is None:
        return None
    
    if era_year_str == '元':
        era_year = 1
    else:
        try:
            era_year = int(era_year_str)
        except ValueError:
            return None
    
    return base_year + era_year

def extract_report_type(text):
    """
    表題から種別を判定（優先度順）
    業績予想 -> 事業計画 -> 中期経営 -> 決算説明 -> 決算短信
    """
    normalized = normalize_text(text)
    
    # 優先順位に基づいたリスト
    keywords = [
        "業績予想",
        "事業計画",
        "中期経営",
        "決算説明",
        "決算短信"
    ]
    
    for kw in keywords:
        if kw in normalized:
            return kw
    return None

def extract_fiscal_period(text):
    """
    表題から年度（年, 月）を抽出
    戻り値: (year, month) または None
    """
    normalized = normalize_text(text)
    
    # パターン1: 西暦パターン (YYYY年M月期)
    pattern1 = r'(\d{4})年([1-9]|1[0-2])月(期)?'
    match = re.search(pattern1, normalized)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        return (year, month)
    
    # パターン2: 元号パターン
    pattern2 = r'(令和|平成|昭和|大正|明治)(元|\d+)年([1-9]|1[0-2])月(期)?'
    match = re.search(pattern2, normalized)
    if match:
        era_name = match.group(1)
        era_year_str = match.group(2)
        month = int(match.group(3))
        year = era_to_western(era_name, era_year_str)
        if year:
            return (year, month)
    
    return None

def get_month_end_date(year, month):
    """
    指定年月の月末日を返す（yy/mm/dd形式の文字列）
    """
    last_day = monthrange(year, month)[1]
    return datetime(year, month, last_day)

def extract_quarter(text):
    """
    表題から四半期を判定（優先度順）
    戻り値: '1Q', '2Q', '3Q', '4Q' または None
    """
    normalized = normalize_text(text)
    
    # 1) 明示表記の検出
    # パターン: [1-4]Q または Q[1-4]
    pattern1 = r'([1-4])\s*Q|Q\s*([1-4])'
    match = re.search(pattern1, normalized, re.IGNORECASE)
    if match:
        q = match.group(1) or match.group(2)
        return f"{q}Q"
    
    # パターン: 第[一二三四１２３４1-4]四半期（パターン内限定変換）
    pattern2 = r'第\s*([一二三四１２３４1-4])\s*四\s*半\s*期'
    match = re.search(pattern2, normalized)
    if match:
        q_char = match.group(1)
        # パターン内限定変換（漢数字→算用数字）
        q_map = {
            '一': '1', '二': '2', '三': '3', '四': '4',
            '１': '1', '２': '2', '３': '3', '４': '4',
            '1': '1', '2': '2', '3': '3', '4': '4'
        }
        q = q_map.get(q_char, '4')
        return f"{q}Q"
    
    # パターン: Quarter [1-4]
    pattern3 = r'Quarter\s*([1-4])'
    match = re.search(pattern3, normalized, re.IGNORECASE)
    if match:
        return f"{match.group(1)}Q"
    
    # ローマ数字パターン: 第Ⅰ・Ⅱ・Ⅲ・Ⅳ四半期
    pattern4 = r'第\s*([ⅠⅡⅢⅣ])\s*四\s*半\s*期'
    match = re.search(pattern4, normalized)
    if match:
        roman_map = {'Ⅰ': '1', 'Ⅱ': '2', 'Ⅲ': '3', 'Ⅳ': '4'}
        q = roman_map.get(match.group(1), '4')
        return f"{q}Q"
    
    # 2) 語句のマッピング
    if re.search(r'上半期|上期|中間期|中間', normalized):
        return '2Q'
    if re.search(r'下半期|下期|通期', normalized):
        return '4Q'
    
    # 3) 年度だけがある（四半期明示が無い） → 4Q
    # この判定は extract_fiscal_period で年度が抽出できた場合に適用
    # （呼び出し側で処理）
    
    return None

def validate_results(ws, start_row, max_row):
    """
    検収チェック（ルール8に準拠）
    """
    print("\n=== 検収チェック開始 ===")
    
    # チェック1: 第X四半期を含む行が4Qになっていないか
    error_count_1 = 0
    for row_idx in range(start_row, min(max_row + 1, start_row + 1000)):
        d_value = ws[f'D{row_idx}'].value
        l_value = ws[f'L{row_idx}'].value
        if d_value and isinstance(d_value, str) and l_value == '4Q':
            if '第' in d_value and '四半期' in d_value:
                error_count_1 += 1
                if error_count_1 <= 5:  # 最初の5件のみ表示
                    print(f"  警告: 行{row_idx} - 第X四半期なのに4Q: {d_value[:50]}")
    
    if error_count_1 == 0:
        print("  ✓ チェック1: 第X四半期が4Qになっている行は0件")
    else:
        print(f"  ✗ チェック1: 第X四半期が4Qになっている行が{error_count_1}件")
    
    # チェック2: 通期表記の行が4Qになっているか
    totsuki_count = 0
    totsuki_4q_count = 0
    for row_idx in range(start_row, min(max_row + 1, start_row + 1000)):
        d_value = ws[f'D{row_idx}'].value
        l_value = ws[f'L{row_idx}'].value
        if d_value and isinstance(d_value, str) and '通期' in d_value:
            totsuki_count += 1
            if l_value == '4Q':
                totsuki_4q_count += 1
    
    if totsuki_count > 0:
        if totsuki_4q_count == totsuki_count:
            print(f"  ✓ チェック2: 通期表記{totsuki_count}件すべてが4Q")
        else:
            print(f"  ✗ チェック2: 通期表記{totsuki_count}件中{totsuki_4q_count}件のみ4Q")
    
    # チェック3: K列の日付が月末か（サンプルチェック）
    month_end_errors = 0
    checked = 0
    for row_idx in range(start_row, min(max_row + 1, start_row + 100)):
        k_value = ws[f'K{row_idx}'].value
        if k_value and isinstance(k_value, datetime):
            checked += 1
            if k_value.day != monthrange(k_value.year, k_value.month)[1]:
                month_end_errors += 1
                if month_end_errors <= 3:
                    print(f"  警告: 行{row_idx} - K列が月末でない: {k_value}")
    
    if month_end_errors == 0:
        print(f"  ✓ チェック3: サンプル{checked}件すべてが月末日")
    else:
        print(f"  ✗ チェック3: サンプル{checked}件中{month_end_errors}件が月末でない")
    
    print("=== 検収チェック完了 ===\n")

def process_tdnet_file(file_path, start_row=36479):
    """
    TDnet適時開示情報.xlsm を処理
    D列から種別（J列）、決算期（K列）、四半期（L列）を判定し書き込む
    """
    start_time = time.time()  # 開始時刻
    print(f"ファイルを読み込み中: {file_path}")
    try:
        wb = openpyxl.load_workbook(file_path, keep_vba=True)
    except Exception as e:
        print(f"エラー: ファイルを読み込めませんでした: {e}")
        return 0
    
    # アクティブシートを取得（または適切なシート名を指定）
    ws = wb.active
    print(f"シート名: {ws.title}")
    
    # データ範囲を確認
    max_row = ws.max_row
    print(f"最大行数: {max_row}, 開始行: {start_row}")
    

    
    processed_count = 0
    updated_count = 0
    
    # 処理開始
    print(f"\n処理を開始します...")
    for row_idx in range(start_row, max_row + 1):
        # D列（表題）を取得
        d_cell = ws[f'D{row_idx}']
        title = d_cell.value
        
        if not title or not isinstance(title, str):
            continue
        
        processed_count += 1
        
        # --- 追加処理: 種別（J列）の判定と上書き ---
        report_type = extract_report_type(title)
        if report_type:
            j_cell = ws[f'J{row_idx}']
            j_cell.value = report_type


        # --- 既存処理: 決算期（K列）と四半期（L列）の判定 ---
        fiscal_period = extract_fiscal_period(title)
        if fiscal_period:
            year, month = fiscal_period
            month_end_date = get_month_end_date(year, month)
            
            # K列に書き込み（Excel日付型）
            k_cell = ws[f'K{row_idx}']
            k_cell.value = month_end_date
            k_cell.number_format = 'yy/mm/dd'

            
            # 四半期（L列）の判定
            quarter = extract_quarter(title)
            if not quarter:
                # 年度だけがある（四半期明示が無い） → 4Q
                quarter = '4Q'
            
            # L列に書き込み
            l_cell = ws[f'L{row_idx}']
            l_cell.value = quarter

            
            updated_count += 1
            
            if updated_count % 100 == 0:
                print(f"処理中... {row_idx}行目まで処理済み（{updated_count}件更新）")
    
    print(f"\n処理完了:")
    print(f"  処理行数: {processed_count}")
    print(f"  更新件数: {updated_count}")
    
    # 検収チェック
    validate_results(ws, start_row, max_row)
    
    # ファイルを保存
    print(f"ファイルを保存中...")
    try:
        wb.save(file_path)
        print("保存完了！")
    except Exception as e:
        print(f"エラー: ファイルを保存できませんでした: {e}")
        return updated_count
    
    end_time = time.time()  # 終了時刻
    elapsed_time = end_time - start_time
    print(f"総経過時間: {elapsed_time:.2f} 秒")
    
    return updated_count

if __name__ == '__main__':
    file_path = r'\\LS720D7A9\TakashiBK\投資\TDNET\TDnet適時情報開示サービス\TDnet適時開示情報.xlsm'
        # start_row = 1000であればExcelも1000行目からスタート 
    start_row = 39649
    
    try:
        process_tdnet_file(file_path, start_row)
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()