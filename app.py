import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time
import io
from urllib.parse import quote
import zipfile
import unicodedata
import datetime
import gc

# ==========================================
# ページ基本設定
# ==========================================
st.set_page_config(
    page_title="Market Detective | ヤフオク一括価格調整",
    page_icon="📊",
    layout="wide"
)

# モダンなUIデザインのためのカスタムCSS
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stButton>button {
        width: 100%; border-radius: 8px; height: 3.5em;
        background-color: #1e3a8a; color: white; font-weight: bold; font-size: 1.1em;
    }
    .stDataFrame { border-radius: 10px; box-shadow: 0 4px 12px rgba(0,0,0,0.05); }
    h1 { color: #1e3a8a; font-weight: 800; border-bottom: 2px solid #e5e7eb; padding-bottom: 0.5rem; }
    .status-box { padding: 1rem; border-radius: 8px; background-color: #ffffff; border-left: 5px solid #1e3a8a; margin-bottom: 1rem; }
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# ロジック関数：スクレイピング
# ==========================================
def get_yahoo_auction_prices(keyword):
    """ヤフオクから該当品番の価格と送料を取得し、最安値・最高値を返す"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    encoded_keyword = quote(f"{keyword} -海外")
    search_url = f"https://auctions.yahoo.co.jp/search/search?p={encoded_keyword}&va={encoded_keyword}&ex_chk=1&is_postage_mode=1&istatus=1&price_type=fixed&s1=cbids&o1=a"
    
    try:
        response = requests.get(search_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        items = soup.select("li.Product")
        if not items:
            return 0, 0

        total_prices = []
        for item in items:
            try:
                item_text = item.get_text()
                if "即決" not in item_text and "定額" not in item_text:
                    continue
                if "海外" in item_text:
                    continue
                status_el = item.select_one(".Product__status")
                if status_el:
                    status_text = status_el.get_text()
                    if "新品" not in status_text and "未使用" not in status_text:
                        continue
                price_el = item.select_one(".Product__priceValue")
                if not price_el:
                    continue
                price_text = price_el.get_text()
                price = int(re.sub(r"[^\d]", "", price_text))
                
                postage = 0
                postage_el = item.select_one(".Product__postage")
                if postage_el:
                    postage_text = postage_el.get_text()
                    if not any(word in postage_text for word in ["送料無料", "出品者負担"]):
                        postage_nums = re.findall(r"\d+", postage_text.replace(",", ""))
                        if postage_nums:
                            postage = int(postage_nums[0])
                total_prices.append(price + postage)
            except Exception:
                continue
        if not total_prices:
            return 0, 0
        return min(total_prices), max(total_prices)
    except Exception:
        return 0, 0

# ==========================================
# ロジック関数：価格算出と利益判定
# ==========================================
def calculate_recommended_price(row, min_total, max_total):
    """要件に基づき推奨価格と粗利率を算出する"""
    if min_total == 0:
        return 0, 0.0, "❌ 取得不可", "市場データ取得不可のため手動確認"

    brand_type = str(row['ブランド区分']).strip()
    is_own_brand = (brand_type == "自社ブランド")
    cat_path = str(row.get('カテゴリパス', ''))
    management_id = str(row.get('管理品番', ''))
    
    # 外装品（特別ルール）の判定条件：カテゴリパスに特定文字列、または管理品番に「PARTS」を含む
    is_exterior = (
        "9外装品 エアロ グリル レンズ等" in cat_path or
        "バイク用品" in cat_path or
        "PARTS" in management_id
    )
    
    cost = row['下代']
    own_shipping = row['送料']
    
    # 自社ブランド または 外装品判定がTrueの場合は、最安値と最高値の中間値をターゲットにする（特別ルール）
    if is_own_brand or is_exterior:
        target_total = (min_total + max_total) / 2
    else:
        target_total = min_total
        
    rec_price_incl_tax = target_total - own_shipping
    if rec_price_incl_tax <= 0:
        return 0, 0.0, "❌ 設定不可", "自社送料が市場の総額を上回るため設定不可"
        
    rec_price_excl_tax = rec_price_incl_tax / 1.1
    if rec_price_excl_tax > 0:
        margin = (rec_price_excl_tax - cost) / rec_price_excl_tax
    else:
        margin = -1.0
        
    if margin < 0.25:
        target_excl_tax = cost / 0.75
        final_rec_price_incl_tax = target_excl_tax * 1.1
        final_margin = 0.25
        status_flag = "⚠️ 粗利補正済"
        reason = "市場価格ベースでは粗利率が25％を下回るため、粗利25％確保ラインに価格を調整しました"
    else:
        final_rec_price_incl_tax = rec_price_incl_tax
        final_margin = margin
        status_flag = "✓ 適正"
        reason = "市場価格ベースで設定（粗利クリア）"
        
    return round(final_rec_price_incl_tax), round(final_margin, 3), status_flag, reason

# ==========================================
# ロジック関数：B列（検索用品番）抽出の確定コード
# ==========================================
def get_search_part_number(row):
    """確定ロジックに基づきB列の品番を抽出する（最強フォールバック付き）"""
    if pd.notna(row.get('name')) and any(k in str(row['name']) for k in ['BMW', 'ベンツ']):
        return row['管理品番']
        
    text = row.get('additional1', '')
    if pd.isna(text):
        return row['管理品番']
        
    text = str(text)
    text = unicodedata.normalize('NFKC', text)
    text = text.replace('\n', ' ').replace('\r', ' ')
    text = re.sub(r'<[^>]+>', ' ', text)
    # あらゆる種類のダッシュやハイフンに似た記号を半角ハイフンに強制統一
    text = re.sub(r'[ー−—–‐‑‒―－~〜～_]', '-', text)
    
    management_num = str(row['管理品番']).strip().upper()
    
    # 「純正」と「品番」の間にゴミ文字が入っていても許容する (.*?を使用)
    parts = re.split(r'純正.*?品番', text)
    if len(parts) > 1:
        target_text = " ".join(parts[1:])
        candidates = re.findall(r'[A-Za-z0-9\-]+', target_text)
        
        valid_cands = []
        for cand in candidates:
            if len(cand) >= 5 and re.search(r'\d', cand):
                valid_cands.append(cand)
        
        if valid_cands:
            # ① ハイフンあり かつ ハイフンを抜くと管理品番と一致するものを「大本命」として最優先
            for c in valid_cands:
                if '-' in c and c.replace('-', '').upper() == management_num:
                    return c
                    
            # ② 次に、ハイフンがあるものを優先
            hyphen_cands = [c for c in valid_cands if '-' in c]
            if hyphen_cands:
                return hyphen_cands[0]
                
            # ③ なければ最初の候補
            return valid_cands[0]
            
    # 【最強のセーフティネット】
    # もし「純正品番」という言葉自体がうまく見つけられなかった場合でも、
    # テキスト全体の中から「ハイフンを抜くと管理品番と一致する」文字があればそれを引っ張り出す！
    all_candidates = re.findall(r'[A-Za-z0-9\-]+', text)
    for cand in all_candidates:
        if '-' in cand and cand.replace('-', '').upper() == management_num:
            return cand
            
    return row['管理品番']

# ==========================================
# ロジック関数：前処理（データ結合と列生成）
# ==========================================
def preprocess_masters(df_list, df_smile, df_ys, df_cost):
    """3つのマスタを結合し、分析に必要なA〜F列を生成する"""
    def find_col(df, target):
        for col in df.columns:
            if str(col).strip().lower() == target.lower():
                return col
        return None

    def clean_id(s):
        """管理品番の表記揺れを強力に排除（大文字化・ハイフン統一・枝番除去）"""
        if pd.isna(s): return "nan"
        s = str(s).strip().upper()
        # 全角ハイフンや各種記号を半角ハイフンに置換
        s = re.sub(r'[ー−—–‐‑‒―－~〜～_]', '-', s)
        val = s.split('-')[0]
        # 無効な値を統一（メモリ爆発対策）
        if val in ['NAN', 'NONE', 'NULL', '']: return "nan"
        return val

    # YSマスタ
    ys_required = ['code', 'name', 'additional1', 'ship-weight']
    missing_ys = [c for c in ys_required if find_col(df_ys, c) is None]
    if missing_ys:
        st.error(f"❌ YSマスタに必須項目「{', '.join(missing_ys)}」が存在しません。")
        st.stop()

    # SMILEマスタ
    smile_part_col = find_col(df_smile, '品番') or find_col(df_smile, '商品コード') or find_col(df_smile, '商品ｺｰﾄﾞ')
    smile_price_col = find_col(df_smile, '標準売上単価')
    if not smile_part_col or not smile_price_col:
        st.error("❌ SMILEマスタに必須項目が存在しません。")
        st.stop()
    
    # メモリダイエット：必要な列だけに絞る
    smile_cols = [c for c in [smile_part_col, smile_price_col] if c is not None]
    df_smile = df_smile[smile_cols].copy()

    # 下代マスタ
    cost_part_col = find_col(df_cost, '品番') or df_cost.columns[0]
    cost_status_col = find_col(df_cost, '在庫商品名')
    cost_price_col = find_col(df_cost, '仕入単価')
    if not cost_status_col or not cost_price_col:
        st.error("❌ 下代マスタに必須項目が存在しません。")
        st.stop()

    # メモリダイエット：必要な列だけに絞る
    cost_cols = [c for c in [cost_part_col, cost_status_col, cost_price_col] if c is not None]
    df_cost = df_cost[cost_cols].copy()
    
    # YSマスタの列特定
    ys_code_col = find_col(df_ys, 'code')
    ys_name_col = find_col(df_ys, 'name')
    ys_add1_col = find_col(df_ys, 'additional1')
    ys_weight_col = find_col(df_ys, 'ship-weight')
    ys_path_col = find_col(df_ys, 'path')
    ys_price_col = find_col(df_ys, 'price')

    # メモリダイエット：必要な列だけに絞る
    ys_cols = [c for c in [ys_code_col, ys_name_col, ys_add1_col, ys_weight_col, ys_path_col, ys_price_col] if c is not None]
    df_ys = df_ys[ys_cols].copy()

    # メモリ解放
    gc.collect()

    # マスタ集約
    df_smile = df_smile.copy()
    df_smile['管理品番'] = df_smile[smile_part_col].apply(clean_id)
    # 無効な行を除去（メモリ爆発対策）
    df_smile = df_smile[df_smile['管理品番'] != 'nan'].copy()
    df_smile[smile_price_col] = pd.to_numeric(df_smile[smile_price_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    df_smile_agg = df_smile.groupby('管理品番').agg({smile_price_col: 'median'}).reset_index()
    
    df_cost = df_cost.copy()
    # 在庫フィルター：空欄ではないものを対象
    df_cost = df_cost[df_cost[cost_status_col].notna()].copy()
    df_cost['管理品番'] = df_cost[cost_part_col].apply(clean_id)
    # 無効な行を除去
    df_cost = df_cost[df_cost['管理品番'] != 'nan'].copy()
    df_cost[cost_price_col] = pd.to_numeric(df_cost[cost_price_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    df_cost_agg = df_cost.groupby('管理品番').agg({cost_price_col: 'median'}).reset_index()

    df_ys = df_ys.copy()
    df_ys['管理品番'] = df_ys[ys_code_col].apply(clean_id)
    # 無効な行を除去
    df_ys = df_ys[df_ys['管理品番'] != 'nan'].copy()
    # 結合前の重複排除（メモリ爆発の根本原因を物理的に遮断）
    df_ys = df_ys.drop_duplicates(subset=['管理品番'], keep='first')
    
    if ys_price_col:
        df_ys[ys_price_col] = pd.to_numeric(df_ys[ys_price_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)

    # データ結合
    list_part_col = df_list.columns[0]
    df_base = df_list[[list_part_col]].rename(columns={list_part_col: '管理品番'}).copy()
    df_base['管理品番'] = df_base['管理品番'].apply(clean_id)
    # 無効な行を除去
    df_base = df_base[df_base['管理品番'] != 'nan'].copy()

    df_merged = pd.merge(df_base, df_smile_agg, on='管理品番', how='left')
    df_merged = pd.merge(df_merged, df_cost_agg, on='管理品番', how='left')
    df_merged = pd.merge(df_merged, df_ys, on='管理品番', how='left')

    df_merged['priority_shipping'] = df_merged[ys_name_col].astype(str).str.contains('送料185円').fillna(False)
    df_merged = df_merged.sort_values(by=['管理品番', 'priority_shipping'], ascending=[True, False])
    df_merged = df_merged.drop_duplicates(subset=['管理品番'], keep='first')

    # B列: 検索用品番の確定ロジック適用
    df_merged['検索用品番'] = df_merged.rename(columns={
        ys_name_col: 'name', 
        ys_add1_col: 'additional1'
    }).apply(get_search_part_number, axis=1)

    # A〜F列の自動生成
    processed_data = []
    oem_list = ["BREMI", "FAE", "BOSCH", "Febi Bilstein", "NGK", "HELLA", "VEMO", "PAGID", "ERLING", "大野ゴム", "RAICAM", "DEPO", "MAHLE", "三ツ星ベルト", "Miyako", "Kashimura", "ALIC", "ミヤコ"]
    own_brand_keywords = ["HAPAD", "CAPSOL", "純正品", "閃-SEN-"]

    for _, row in df_merged.iterrows():
        management_id = str(row['管理品番'])
        search_id = row['検索用品番']
        name = str(row.get(ys_name_col, ''))
        
        path_val = row.get(ys_path_col, '')
        path_val = str(path_val) if pd.notna(path_val) else ""
        
        # 販売価格の数値変換 (SMILE優先、0ならYSで補完)
        price_val = row.get(smile_price_col, 0)
        price = int(float(price_val)) if pd.notna(price_val) else 0
        
        if price == 0 and ys_price_col:
            ys_p_val = row.get(ys_price_col, 0)
            price = int(float(ys_p_val)) if pd.notna(ys_p_val) else 0
        
        shipping = 0
        if "送料185円" in name:
            shipping = 185
        else:
            ship_weight = row.get(ys_weight_col, 0)
            weight_map = {0: 770, 100: 1100, 1: 1650, 1000: 3300}
            try:
                if pd.notna(ship_weight):
                    w_val = int(float(str(ship_weight).replace(',', '')))
                    shipping = weight_map.get(w_val, 770)
                else:
                    shipping = 770
            except:
                shipping = 770
            
        cost_val = row.get(cost_price_col, 0)
        cost = int(float(cost_val)) if pd.notna(cost_val) else 0
        
        brand_type = "社外品"
        if any(kw in name for kw in own_brand_keywords):
            brand_type = "自社ブランド"
        else:
            for oem in oem_list:
                if oem.lower() in name.lower():
                    brand_type = "社外品"
                    search_id = f"{search_id} {oem}"
                    break
        
        processed_data.append({
            '管理品番': management_id,
            '検索用品番': search_id,
            '販売価格': price,
            '送料': shipping,
            '下代': cost,
            'ブランド区分': brand_type,
            'カテゴリパス': path_val
        })
        
    return pd.DataFrame(processed_data)

# ==========================================
# Streamlit メイン UI
# ==========================================
def main():
    st.title("📊 Market Detective")
    st.markdown("マスタデータをアップロードして、ヤフオク価格調査と利益判定を全自動で行います。")
    
    with st.sidebar:
        st.header("📂 データアップロード")
        file_list = st.file_uploader("1. 管理品番リスト (A列に対象品番)", type=["xlsx", "csv"])
        file_smile = st.file_uploader("2. SMILEマスタ (システム出力の.txt / .zip)", type=["txt", "csv", "xlsx", "zip"])
        file_ys = st.file_uploader("3. YSマスタ (.csv / .xlsx / .zip)", type=["csv", "xlsx", "zip"])
        file_cost = st.file_uploader("4. 下代マスタ (.csv / .xlsx / .zip)", type=["csv", "xlsx", "zip"])
        
        st.divider()
        st.header("⚙️ 設定")
        st.info("待機時間を設けてヤフオクのサーバー負荷を抑えながら実行します（1件あたり約2.5秒）。")

    if file_list and file_smile and file_ys and file_cost:
        try:
            def robust_read_csv(file_obj, header=0):
                encodings = ['cp932', 'utf-8', 'utf-16']
                for enc in encodings:
                    try:
                        if hasattr(file_obj, 'seek'):
                            file_obj.seek(0)
                        return pd.read_csv(file_obj, encoding=enc, sep=None, engine='python', header=header)
                    except Exception:
                        continue
                raise ValueError("ファイルの文字コードを判別できませんでした")

            def load_df(file, has_noise=False):
                h = None if has_noise else 0
                if file.name.endswith('.zip'):
                    with zipfile.ZipFile(file) as z:
                        inner_file_name = z.namelist()[0]
                        with z.open(inner_file_name) as f:
                            if inner_file_name.endswith('.xlsx'):
                                df = pd.read_excel(f, header=h)
                            else:
                                content = io.BytesIO(f.read())
                                df = robust_read_csv(content, header=h)
                elif file.name.endswith('.xlsx'):
                    df = pd.read_excel(file, header=h)
                else:
                    df = robust_read_csv(file, header=h)
                
                if has_noise:
                    keywords = ["仕入単価", "在庫商品名"]
                    for i in range(min(15, len(df))):
                        row_vals = [str(v) for v in df.iloc[i].values]
                        if all(any(kw in str(v) for v in row_vals) for kw in keywords):
                            df.columns = df.iloc[i]
                            df = df.iloc[i+1:].reset_index(drop=True)
                            break
                return df
            
            with st.spinner("ファイルを読み込み中..."):
                df_l = load_df(file_list)
                df_s = load_df(file_smile)
                df_y = load_df(file_ys)
                df_c = load_df(file_cost, has_noise=True)
            
            st.success("✅ 4つのマスタファイルを読み込みました。")
            st.divider()

            with st.spinner("マスタデータを統合中..."):
                df_input = preprocess_masters(df_l, df_s, df_y, df_c)
            
            if df_input.empty:
                st.error("前処理結果が空です。")
                return

            st.subheader("🛠 統合データプレビュー")
            st.dataframe(df_input.drop(columns=['カテゴリパス']), use_container_width=True, hide_index=True)

            output_preview = io.BytesIO()
            with pd.ExcelWriter(output_preview, engine='openpyxl') as writer:
                df_input.drop(columns=['カテゴリパス']).to_excel(writer, index=False, sheet_name='統合データ確認')
            
            st.download_button(
                label="📥 統合後データ確認用 (テスト用Excel) をダウンロード",
                data=output_preview.getvalue(),
                file_name="統合後データ確認用.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            st.divider()

            if st.button("🚀 ヤフオク価格調査と分析を開始する"):
                results = []
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                total_rows = len(df_input)
                for i, row in df_input.iterrows():
                    management_id = str(row['管理品番'])
                    search_keyword = str(row['検索用品番'])
                    
                    status_text.markdown(f"<div class='status-box'>🔎 調査中 ({i+1}/{total_rows}): <b>{management_id}</b> (検索: {search_keyword})</div>", unsafe_allow_html=True)
                    min_p, max_p = get_yahoo_auction_prices(search_keyword)
                    
                    final_rec, final_m, status, reason = calculate_recommended_price(row, min_p, max_p)
                    
                    # 価格未設定時の表示改善
                    orig_price = row['販売価格']
                    if orig_price == 0:
                        orig_price_display = "未設定"
                        reason = "【マスタ価格未設定（正常）】" + reason
                        if status == "✓ 適正":
                            status = "❕ 新規設定"
                    else:
                        orig_price_display = orig_price

                    results.append({
                        "管理品番": management_id,
                        "検索用品番": search_keyword,
                        "元販売価格(込)": orig_price_display,
                        "ブランド区分": row['ブランド区分'],
                        "ヤフオク最安値": min_p if min_p > 0 else "取得不可",
                        "ヤフオク最高値": max_p if max_p > 0 else "取得不可",
                        "推奨価格(込)": final_rec if final_rec > 0 else "-",
                        "粗利率(税抜)": f"{final_m*100:.1f}%" if final_rec > 0 else "-",
                        "ステータス": status,
                        "備考（調整理由）": reason
                    })
                    
                    progress_bar.progress((i + 1) / total_rows)
                    time.sleep(2.5)
                
                status_text.success(f"✅ 全 {total_rows} 件の分析が完了しました。")
                df_result = pd.DataFrame(results)
                st.dataframe(df_result, use_container_width=True, hide_index=True)

                df_export = df_result[
                    (df_result["推奨価格(込)"] != "-") & 
                    (df_result["元販売価格(込)"] != df_result["推奨価格(込)"])
                ].copy()

                df_excluded = df_result[
                    (df_result["推奨価格(込)"] == "-") | 
                    (df_result["元販売価格(込)"] == df_result["推奨価格(込)"])
                ].copy()

                if not df_export.empty:
                    output_result = io.BytesIO()
                    with pd.ExcelWriter(output_result, engine='openpyxl') as writer:
                        df_export.to_excel(writer, index=False, sheet_name='価格調整結果')

                    df_smile_export = pd.DataFrame({
                        "管理品番": df_export["管理品番"],
                        "新価格": df_export["推奨価格(込)"]
                    })

                    today_str = datetime.datetime.now().strftime("%Y/%m/%d")
                    df_kintone_export = pd.DataFrame({
                        "レコードの開始行": ["*"] * len(df_export),
                        "管理品番": df_export["管理品番"].values,
                        "変更日付_c": [today_str] * len(df_export),
                        "変更前": df_export["元販売価格(込)"].values,
                        "変更後": df_export["推奨価格(込)"].values,
                        "備考": ("価格調整 " + df_export["粗利率(税抜)"].astype(str)).values,
                        "担当者_g": [""] * len(df_export),
                        "変更日付_h": [""] * len(df_export),
                        "変更内容": [""] * len(df_export),
                        "担当者_j": [""] * len(df_export),
                        "マーク": [""] * len(df_export)
                    })
                    df_kintone_export.columns = [
                        "レコードの開始行", "管理品番", "変更日付", "変更前", "変更後", 
                        "備考", "担当者", "変更日付", "変更内容", "担当者", "マーク"
                    ]

                    output_smile = io.BytesIO()
                    with pd.ExcelWriter(output_smile, engine='openpyxl') as writer:
                        df_smile_export.to_excel(writer, index=False)

                    output_kintone = io.BytesIO()
                    with pd.ExcelWriter(output_kintone, engine='openpyxl') as writer:
                        df_kintone_export.to_excel(writer, index=False)

                    st.divider()
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.download_button(
                            label="📥 調整済みリストをダウンロード",
                            data=output_result.getvalue(),
                            file_name="market_detective_result.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    with col2:
                        st.download_button(
                            label="📥 SMILE用インポートファイルをダウンロード",
                            data=output_smile.getvalue(),
                            file_name=f"smile_import_{datetime.datetime.now().strftime('%Y%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    with col3:
                        st.download_button(
                            label="📥 キントーン用インポートファイルをダウンロード",
                            data=output_kintone.getvalue(),
                            file_name=f"kintone_import_{datetime.datetime.now().strftime('%Y%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    with col4:
                        if not df_excluded.empty:
                            output_excluded = io.BytesIO()
                            with pd.ExcelWriter(output_excluded, engine='openpyxl') as writer:
                                df_excluded.to_excel(writer, index=False, sheet_name='除外リスト')
                            st.download_button(
                                label="📥 除外リストをダウンロード",
                                data=output_excluded.getvalue(),
                                file_name="market_detective_excluded.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                        else:
                            st.button("📥 除外リスト (0件)", disabled=True)

                    st.info("""💡 基幹システムへのインポート手順
ダウンロードしたファイルは、以下のインポート専用フォルダに保存（または移動）してください。
""")
                    st.code(r"\\192.168.1.77\【新】共有\【アシロボ】作業フォルダ\販促\価格更新", language="text")
                    st.markdown("※上のパスをコピーして、エクスプローラーのアドレスバーに貼り付けるとすぐに開けます。")
                else:
                    st.warning("価格変動があった商品が見つかりませんでした。")
                
        except Exception as e:
            st.error(f"エラーが発生しました: {e}")
    else:
        st.info("サイドバーから 4 つのマスタファイルをアップロードしてください。")

if __name__ == "__main__":
    main()
