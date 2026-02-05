#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import unicodedata
import time
import os
from datetime import datetime
from calendar import monthrange
import win32com.client  # pywin32を使用

# =================================================================
# 1. 設定エリア
# =================================================================
TARGET_FILE_PATH = r'\\LS720D7A9\TakashiBK\投資\TDNET\TDnet適時情報開示サービス\TDnet適時開示情報.xlsm'
START_ROW = 41651
# =================================================================

ERA_TO_YEAR = {
    '令和': 2018, '平成': 1988, '昭和': 1925, '大正': 1911, '明治': 1867
}

def normalize_text(text):
    if not text or not isinstance(text, str): return ""
    normalized = unicodedata.normalize('NFKC', text)
    return re.sub(r'\s+', ' ', normalized).strip()

def era_to_western(era_name, era_year_str):
    base_year = ERA_TO_YEAR.get(era_name)
    if base_year is None: return None
    era_year = 1 if era_year_str == '元' else int(era_year_str)
    return base_year + era_year

def extract_report_type(text):
    normalized = normalize_text(text)
    keywords = ["業績予想", "事業計画", "中期経営", "決算説明", "決算短信"]
    for kw in keywords:
        if kw in normalized: return kw
    return None

def extract_fiscal_period(text):
    normalized = normalize_text(text)
    p1 = r'(\d{4})年([1-9]|1[0-2])月(期)?'
    m1 = re.search(p1, normalized)
    if m1: return (int(m1.group(1)), int(m1.group(2)))
    p2 = r'(令和|平成|昭和|大正|明治)(元|\d+)年([1-9]|1[0-2])月(期)?'
    m2 = re.search(p2, normalized)
    if m2:
        year = era_to_western(m2.group(1), m2.group(2))
        if year: return (year, int(m2.group(3)))
    return None

def extract_quarter(text):
    normalized = normalize_text(text)
    p1 = r'([1-4])\s*Q|Q\s*([1-4])'
    m1 = re.search(p1, normalized, re.IGNORECASE)
    if m1: return f"{m1.group(1) or m1.group(2)}Q"
    p2 = r'第\s*([一二三四１２３４1-4])\s*四\s*半\s*期'
    m2 = re.search(p2, normalized)
    if m2:
        q_map = {'一':'1','二':'2','三':'3','四':'4','１':'1','２':'2','３':'3','４':'4','1':'1','2':'2','3':'3','4':'4'}
        return f"{q_map.get(m2.group(1), '4')}Q"
    if re.search(r'上半期|上期|中間期|中間', normalized): return '2Q'
    if re.search(r'下半期|下期|通期', normalized): return '4Q'
    return None

def process_with_win32com(file_path, start_row):
    start_time = time.time()
    
    print(f"Excelを操作中...")
    try:
        # Excelアプリケーションに接続
        excel = win32com.client.GetActiveObject("Excel.Application")
    except:
        # Excelが起動していない場合は新しく起動
        excel = win32com.client.Dispatch("Excel.Application")
    
    excel.Visible = True # 処理を見えるようにする
    
    # 目的のブックを探す、なければ開く
    target_wb = None
    for wb in excel.Workbooks:
        if wb.FullName.lower() == file_path.lower():
            target_wb = wb
            break
    
    if not target_wb:
        print(f"ファイルを開きます: {file_path}")
        target_wb = excel.Workbooks.Open(file_path)

    ws = target_wb.ActiveSheet
    # A列の最終行を取得（ExcelのxlUpを使用）
    max_row = ws.Cells(ws.Rows.Count, "A").End(-4162).Row # -4162 = xlUp

    if max_row < start_row:
        print("処理対象の行がありません。")
        return

    # 1. データの読み取り (D列: タイトルを一括取得)
    titles = ws.Range(ws.Cells(start_row, 4), ws.Cells(max_row, 4)).Value
    
    # 書き込み用データ作成 (J, K, L列分)
    output_data = []
    updated_count = 0

    # 2. ロジック処理
    for i, row in enumerate(titles):
        title = row[0]
        row_rtype = ""
        row_period = ""
        row_q = ""
        
        if title and isinstance(title, str):
            # 種別判定
            row_rtype = extract_report_type(title)
            
            # 決算期・四半期判定
            period = extract_fiscal_period(title)
            if period:
                last_day = monthrange(period[0], period[1])[1]
                # 日付形式を文字列で作成（Excelへの流し込み用）
                row_period = f"{period[0]}/{period[1]}/{last_day}"
                row_q = extract_quarter(title) or '4Q'
                updated_count += 1
        
        # 取得した既存の値を保持しつつ、新しく判定したものをセット
        # (J列, K列, L列) の形式でリスト化
        output_data.append([row_rtype, row_period, row_q])

    # 3. データの書き込み (J列〜L列の範囲を一括更新)
    if output_data:
        write_range = ws.Range(ws.Cells(start_row, 10), ws.Cells(max_row, 12))
        write_range.Value = output_data
        # K列の書式設定（yy/mm/dd）
        ws.Range(ws.Cells(start_row, 11), ws.Cells(max_row, 11)).NumberFormat = "yy/mm/dd"

    end_time = time.time()
    
    # 結果出力
    print("-" * 40)
    print(f"【処理結果】")
    print(f"全対象行数: {len(titles)}件")
    print(f"判定成功数: {updated_count}件")
    print(f"処理時間  : {end_time - start_time:.2f}秒")
    print("-" * 40)
    print("完了しました。Excelは開いたままですので、内容を確認して保存してください。")

if __name__ == '__main__':
    print(f"--- TDnet 判定スクリプト (pywin32版: 開いたまま更新) ---")
    try:
        process_with_win32com(TARGET_FILE_PATH, START_ROW)
    except Exception as e:
        print(f"エラーが発生しました: {e}")