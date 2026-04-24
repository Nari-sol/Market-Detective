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
    
    soup = None
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
    finally:
        if soup:
            soup.decompose() # リソース解放

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
    
    is_exterior = (
        "9外装品 エアロ グリル レンズ等" in cat_path or
        "バイク用品" in cat_path or
        "PARTS" in management_id
    )
    
    cost = row['下代']
    if cost <= 0:
        return 0, 0.0, "⏭️ 除外（原価不明）", "下代マスタから原価が取得できなかったため、価格変更を見送りました"
    own_shipping = row['送料']
    
    if is_own_brand or is_exterior:
        target_total = (min_total + max_total) / 2
    else:
        target_total = min_total
        
    rec_price_incl_tax = target_total - own_shipping
    if rec_price_incl_tax <= 0:
        return 0, 0.0, "❌ 設定不可", "自社送料が市場の総額を上回るため設定不可"
        
    # 端数カットによる安全な価格算出（税抜10円単位で切り捨て）
    adjusted_excl_tax = int((rec_price_incl_tax / 1.1) // 10) * 10
    adjusted_incl_tax = int(adjusted_excl_tax * 1.1)
    
    if adjusted_excl_tax > 0:
        margin = (adjusted_excl_tax - cost) / adjusted_excl_tax
    else:
        margin = -1.0
        
    if margin < 0.25:
        return 0, margin, "⏭️ 除外（粗利未達）", "市場価格ベースの粗利率が25％を下回るため、価格変更を見送りました"
    else:
        return adjusted_incl_tax, round(margin, 3), "✓ 適正", "市場価格ベースで設定（粗利クリア）"

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
    text = re.sub(r'[ー−—–‐‑‒―－~〜～_]', '-', text)
    
    management_num = str(row['管理品番']).strip().upper()
    
    parts = re.split(r'純正.*?品番', text)
    if len(parts) > 1:
        target_text = " ".join(parts[1:])
        candidates = re.findall(r'[A-Za-z0-9\-]+', target_text)
        
        valid_cands = []
        for cand in candidates:
            if len(cand) >= 5 and re.search(r'\d', cand):
                valid_cands.append(cand)
        
        if valid_cands:
            for c in valid_cands:
                if '-' in c and c.replace('-', '').upper() == management_num:
                    return c
            hyphen_cands = [c for c in valid_cands if '-' in c]
            if hyphen_cands:
                return hyphen_cands[0]
            return valid_cands[0]
            
    all_candidates = re.findall(r'[A-Za-z0-9\-]+', text)
    for cand in all_candidates:
        if '-' in cand and cand.replace('-', '').upper() == management_num:
            return cand
            
    return row['管理品番']

# ==========================================
# ロジック関数：ファイル読み込みヘルパー (グローバル)
# ==========================================
def robust_read_csv(file_obj, header=0, nrows=None):
    """文字コードを自動判別し、dtype=strで読み込む（メモリ節約）"""
    encodings = ['cp932', 'utf-8', 'utf-16']
    for enc in encodings:
        try:
            if hasattr(file_obj, 'seek'):
                file_obj.seek(0)
            return pd.read_csv(file_obj, encoding=enc, sep=None, engine='python', header=header, nrows=nrows, dtype=str)
        except Exception:
            continue
    raise ValueError("ファイルの文字コードを判別できませんでした")

def load_df(file, has_noise=False):
    """ヘッダーを軽量に特定してから全体を読み込む"""
    target_header = 0
    
    # 1. ヘッダー検索が必要な場合 (下代マスタ等)
    if has_noise:
        keywords = ["仕入単価", "在庫商品名"]
        # 最初の20行だけを読み込んでヘッダー行を特定する (メモリ節約)
        if file.name.endswith('.zip'):
            with zipfile.ZipFile(file) as z:
                inner_name = z.namelist()[0]
                with z.open(inner_name) as f:
                    if inner_name.endswith('.xlsx'):
                        df_peek = pd.read_excel(f, nrows=20, header=None, dtype=str)
                    else:
                        df_peek = robust_read_csv(f, header=None, nrows=20)
        elif file.name.endswith('.xlsx'):
            df_peek = pd.read_excel(file, nrows=20, header=None, dtype=str)
        else:
            df_peek = robust_read_csv(file, header=None, nrows=20)
            
        for i in range(len(df_peek)):
            row_vals = [str(v) for v in df_peek.iloc[i].values]
            if all(any(kw in str(v) for v in row_vals) for kw in keywords):
                target_header = i
                break
        
        # ファイルポインタを先頭に戻す
        if hasattr(file, 'seek'): file.seek(0)

    # 2. 全体読み込み (dtype=strを指定し、解凍データはストリームのまま渡す)
    if file.name.endswith('.zip'):
        with zipfile.ZipFile(file) as z:
            inner_name = z.namelist()[0]
            with z.open(inner_name) as f:
                if inner_name.endswith('.xlsx'):
                    return pd.read_excel(f, header=target_header, dtype=str)
                else:
                    return robust_read_csv(f, header=target_header)
    elif file.name.endswith('.xlsx'):
        return pd.read_excel(file, header=target_header, dtype=str)
    else:
        return robust_read_csv(file, header=target_header)

# ==========================================
# ロジック関数：前処理（データ結合と列生成）
# ==========================================
def preprocess_masters(file_list, file_smile, file_ys, file_cost):
    """ベースリストに存在する品番だけをマスタから抽出して結合し、メモリを極小化する"""
    def find_col(df, target):
        for col in df.columns:
            if str(col).strip().lower() == target.lower():
                return col
        return None

    def clean_id(s):
        if pd.isna(s): return "nan"
        s = str(s).strip().upper()
        s = re.sub(r'[ー−—–‐‑‒―－~〜～_]', '-', s)
        val = s.split('-')[0]
        if val in ['NAN', 'NONE', 'NULL', '']: return "nan"
        return val

    # 1. ベースリストの読み込み
    df_raw = load_df(file_list)
    list_part_col = df_raw.columns[0]
    df_base = df_raw[[list_part_col]].rename(columns={list_part_col: '管理品番'}).copy()
    df_base['管理品番'] = df_base['管理品番'].apply(clean_id)
    # 無効なキーを排除
    df_base = df_base[~df_base['管理品番'].astype(str).str.lower().isin(['nan', 'none', ''])].copy()
    # ターゲット品番の抽出（これを使ってマスタを絞り込む）
    target_ids = set(df_base['管理品番'].unique())
    del df_raw
    gc.collect()

    # 2. SMILEマスタの処理
    df_raw = load_df(file_smile)
    smile_part_col = find_col(df_raw, '品番') or find_col(df_raw, '商品コード') or find_col(df_raw, '商品ｺｰﾄﾞ')
    smile_price_col = find_col(df_raw, '標準売上単価')
    if not smile_part_col or not smile_price_col:
        st.error("❌ SMILEマスタに必須項目が存在しません。")
        st.stop()
    
    df_smile_agg = df_raw[[smile_part_col, smile_price_col]].copy()
    df_smile_agg['管理品番'] = df_smile_agg[smile_part_col].apply(clean_id)
    # 【最重要】ターゲット以外を即座に破棄
    df_smile_agg = df_smile_agg[df_smile_agg['管理品番'].isin(target_ids)].copy()
    df_smile_agg[smile_price_col] = pd.to_numeric(df_smile_agg[smile_price_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    df_smile_agg = df_smile_agg.groupby('管理品番').agg({smile_price_col: 'median'}).reset_index()
    del df_raw
    gc.collect()

    # 3. 下代マスタの処理
    df_raw = load_df(file_cost, has_noise=True)
    cost_part_col = find_col(df_raw, '品番') or df_raw.columns[0]
    cost_status_col = find_col(df_raw, '在庫商品名')
    cost_price_col = find_col(df_raw, '仕入単価')
    if not cost_status_col or not cost_price_col:
        st.error("❌ 下代マスタに必須項目が存在しません。")
        st.stop()
    
    df_cost_agg = df_raw[[cost_part_col, cost_status_col, cost_price_col]].copy()
    df_cost_agg = df_cost_agg[df_cost_agg[cost_status_col].notna()].copy()
    df_cost_agg['管理品番'] = df_cost_agg[cost_part_col].apply(clean_id)
    # 【最重要】ターゲット以外を即座に破棄
    df_cost_agg = df_cost_agg[df_cost_agg['管理品番'].isin(target_ids)].copy()
    df_cost_agg[cost_price_col] = pd.to_numeric(df_cost_agg[cost_price_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    df_cost_agg = df_cost_agg.groupby('管理品番').agg({cost_price_col: 'median'}).reset_index()
    del df_raw
    gc.collect()

    # 4. YSマスタの処理
    df_raw = load_df(file_ys)
    ys_code_col = find_col(df_raw, 'code')
    ys_name_col = find_col(df_raw, 'name')
    ys_add1_col = find_col(df_raw, 'additional1')
    ys_weight_col = find_col(df_raw, 'ship-weight')
    ys_path_col = find_col(df_raw, 'path')
    ys_price_col = find_col(df_raw, 'price')
    
    ys_cols = [c for c in [ys_code_col, ys_name_col, ys_add1_col, ys_weight_col, ys_path_col, ys_price_col] if c is not None]
    df_ys_clean = df_raw[ys_cols].copy()
    df_ys_clean['管理品番'] = df_ys_clean[ys_code_col].apply(clean_id)
    # 【最重要】ターゲット以外を即座に破棄
    df_ys_clean = df_ys_clean[df_ys_clean['管理品番'].isin(target_ids)].copy()
    df_ys_clean = df_ys_clean.drop_duplicates(subset=['管理品番'], keep='first')
    if ys_price_col:
        df_ys_clean[ys_price_col] = pd.to_numeric(df_ys_clean[ys_price_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    del df_raw
    gc.collect()

    # 5. 結合と後処理
    df_merged = pd.merge(df_base, df_smile_agg, on='管理品番', how='left')
    del df_smile_agg
    df_merged = pd.merge(df_merged, df_cost_agg, on='管理品番', how='left')
    del df_cost_agg
    df_merged = pd.merge(df_merged, df_ys_clean, on='管理品番', how='left')
    del df_ys_clean
    gc.collect()

    df_merged['priority_shipping'] = df_merged[ys_name_col].astype(str).str.contains('送料185円').fillna(False)
    df_merged = df_merged.sort_values(by=['管理品番', 'priority_shipping'], ascending=[True, False])
    df_merged = df_merged.drop_duplicates(subset=['管理品番'], keep='first')

    df_merged['検索用品番'] = df_merged.rename(columns={
        ys_name_col: 'name', 
        ys_add1_col: 'additional1'
    }).apply(get_search_part_number, axis=1)

    processed_data = []
    oem_list = ["BREMI", "FAE", "BOSCH", "Febi Bilstein", "NGK", "HELLA", "VEMO", "PAGID", "ERLING", "大野ゴム", "RAICAM", "DEPO", "MAHLE", "三ツ星ベルト", "Miyako", "Kashimura", "ALIC", "ミヤコ"]
    own_brand_keywords = ["HAPAD", "CAPSOL", "純正品", "閃-SEN-"]

    for _, row in df_merged.iterrows():
        management_id = str(row['管理品番'])
        search_id = row['検索用品番']
        name = str(row.get(ys_name_col, ''))
        path_val = str(row.get(ys_path_col, '')) if pd.notna(row.get(ys_path_col)) else ""
        
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
                shipping = weight_map.get(int(float(str(ship_weight).replace(',', ''))), 770)
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
            '管理品番': management_id, '検索用品番': search_id, '販売価格': price,
            '送料': shipping, '下代': cost, 'ブランド区分': brand_type, 'カテゴリパス': path_val
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
        file_list = st.file_uploader("1. 管理品番リスト", type=["xlsx", "csv"])
        file_smile = st.file_uploader("2. SMILEマスタ", type=["txt", "csv", "xlsx", "zip"])
        file_ys = st.file_uploader("3. YSマスタ", type=["csv", "xlsx", "zip"])
        file_cost = st.file_uploader("4. 下代マスタ", type=["csv", "xlsx", "zip"])
        st.divider()
        st.header("⚙️ 設定")
        st.info("待機時間を設けてヤフオクのサーバー負荷を抑えながら実行します（1件あたり2.5秒）。")

    if file_list and file_smile and file_ys and file_cost:
        # ファイル変更検知とキャッシュクリア
        current_files = [
            (file_list.name, file_list.size),
            (file_smile.name, file_smile.size),
            (file_ys.name, file_ys.size),
            (file_cost.name, file_cost.size),
        ]
        if 'last_files' not in st.session_state or st.session_state['last_files'] != current_files:
            if 'integrated_data' in st.session_state:
                del st.session_state['integrated_data']
            if 'analysis_results' in st.session_state:
                del st.session_state['analysis_results']
            st.session_state['last_files'] = current_files

        try:
            st.success("✅ 4つのマスタファイルを認識しました。分析を開始します。")
            st.divider()
            
            # 統合処理のキャッシュ判定
            if 'integrated_data' not in st.session_state:
                with st.spinner("マスタデータを「ターゲット抽出」で統合中..."):
                    df_input = preprocess_masters(file_list, file_smile, file_ys, file_cost)
                    st.session_state['integrated_data'] = df_input
            else:
                df_input = st.session_state['integrated_data']
            
            if df_input.empty:
                st.error("前処理結果が空です。")
                return

            st.subheader("🛠 統合データプレビュー")
            st.dataframe(df_input.drop(columns=['カテゴリパス']), use_container_width=True, hide_index=True)

            output_preview = io.BytesIO()
            with pd.ExcelWriter(output_preview, engine='openpyxl') as writer:
                df_input.drop(columns=['カテゴリパス']).to_excel(writer, index=False, sheet_name='統合データ確認')
            st.download_button(label="📥 統合後データ確認用Excelをダウンロード", data=output_preview.getvalue(), file_name="統合後データ確認用.xlsx")

            st.divider()
            
            # 分析実行のトリガー判定
            run_analysis = st.button("🚀 ヤフオク価格調査と分析を開始する")
            if run_analysis or 'analysis_results' in st.session_state:
                
                # キャッシュがない場合のみ分析を実行
                if 'analysis_results' not in st.session_state:
                    results = []
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    total_rows = len(df_input)
                    for i, row in df_input.iterrows():
                        management_id = str(row['管理品番'])
                        search_keyword = str(row['検索用品番'])
                        status_text.markdown(f"<div class='status-box'>🔎 調査中 ({i+1}/{total_rows}): <b>{management_id}</b></div>", unsafe_allow_html=True)
                        min_p, max_p = get_yahoo_auction_prices(search_keyword)
                        final_rec, final_m, status, reason = calculate_recommended_price(row, min_p, max_p)
                        
                        orig_price = row['販売価格']
                        if orig_price == 0:
                            orig_price_display = "未設定"
                            reason = "【マスタ価格未設定（正常）】" + reason
                            if status == "✓ 適正": status = "❕ 新規設定"
                        else:
                            orig_price_display = orig_price
                            # 価格変更が不要な場合の表示上書き
                            if final_rec > 0 and final_rec == orig_price:
                                status = "⏸️ 変更なし"
                                reason = "現在の価格がすでに適正なため、更新を見送りました"

                        results.append({
                            "管理品番": management_id, "検索用品番": search_keyword, "元販売価格(込)": orig_price_display,
                            "ブランド区分": row['ブランド区分'], "ヤフオク最安値": min_p if min_p > 0 else "取得不可",
                            "ヤフオク最高値": max_p if max_p > 0 else "取得不可", "推奨価格(込)": final_rec if final_rec > 0 else "-",
                            "粗利率(税抜)": f"{final_m*100:.1f}%" if final_rec > 0 else "-", "ステータス": status, "備考（調整理由）": reason
                        })
                        if i % 10 == 0: gc.collect()
                        progress_bar.progress((i + 1) / total_rows)
                        time.sleep(2.5)
                    
                    status_text.success(f"✅ 全 {total_rows} 件の分析が完了しました。")
                    df_result = pd.DataFrame(results)
                    
                    df_export = df_result[(df_result["推奨価格(込)"] != "-") & (df_result["元販売価格(込)"] != df_result["推奨価格(込)"])].copy()
                    df_excluded = df_result[(df_result["推奨価格(込)"] == "-") | (df_result["元販売価格(込)"] == df_result["推奨価格(込)"])].copy()

                    # エクスポートファイルの作成
                    output_result = io.BytesIO()
                    output_smile = io.BytesIO()
                    output_kintone = io.BytesIO()
                    output_ex = io.BytesIO()

                    if not df_export.empty:
                        with pd.ExcelWriter(output_result, engine='openpyxl') as writer:
                            df_export.to_excel(writer, index=False, sheet_name='価格調整結果')

                        df_smile_export = pd.DataFrame({"管理品番": df_export["管理品番"], "新価格": df_export["推奨価格(込)"]})
                        today_str = datetime.datetime.now().strftime("%Y/%m/%d")
                        
                        # キントーン用：重複ヘッダーを許容するため、全11列で構成
                        df_kintone_export = pd.DataFrame({
                            0: ["*"] * len(df_export), 1: df_export["管理品番"].values,
                            2: [today_str] * len(df_export), 3: df_export["元販売価格(込)"].values,
                            4: df_export["推奨価格(込)"].values, 5: ("価格調整 " + df_export["粗利率(税抜)"].astype(str)).values,
                            6: [""] * len(df_export), 7: [""] * len(df_export), 8: [""] * len(df_export), 
                            9: [""] * len(df_export), 10: [""] * len(df_export)
                        })
                        header_row = ["レコードの開始行", "管理品番", "変更日付", "変更前", "変更後", "備考", "担当者", "変更日付", "変更内容", "担当者", "マーク"]
                        df_kintone_export.loc[-1] = header_row
                        df_kintone_export.index = df_kintone_export.index + 1
                        df_kintone_export = df_kintone_export.sort_index()

                        with pd.ExcelWriter(output_smile, engine='openpyxl') as writer: df_smile_export.to_excel(writer, index=False)
                        with pd.ExcelWriter(output_kintone, engine='openpyxl') as writer: 
                            df_kintone_export.to_excel(writer, index=False, header=False)
                    
                    if not df_excluded.empty:
                        with pd.ExcelWriter(output_ex, engine='openpyxl') as writer:
                            df_excluded.to_excel(writer, index=False, sheet_name='除外リスト')

                    # 結果をキャッシュに保存
                    st.session_state['analysis_results'] = {
                        'df_result': df_result,
                        'df_export': df_export,
                        'df_excluded': df_excluded,
                        'output_result': output_result.getvalue(),
                        'output_smile': output_smile.getvalue() if not df_export.empty else None,
                        'output_kintone': output_kintone.getvalue() if not df_export.empty else None,
                        'output_ex': output_ex.getvalue() if not df_excluded.empty else None
                    }
                
                # キャッシュから結果を表示
                res = st.session_state['analysis_results']
                st.dataframe(res['df_result'], use_container_width=True, hide_index=True)

                if not res['df_export'].empty:
                    st.divider()
                    col1, col2, col3, col4 = st.columns(4)
                    with col1: st.download_button(label="📥 調整済みリスト", data=res['output_result'], file_name="market_detective_result.xlsx")
                    with col2: st.download_button(label="📥 SMILE用", data=res['output_smile'], file_name="smile_import.xlsx")
                    with col3: st.download_button(label="📥 キントーン用", data=res['output_kintone'], file_name="kintone_import.xlsx")
                    with col4:
                        if res['output_ex']:
                            st.download_button(label="📥 除外リスト", data=res['output_ex'], file_name="market_detective_excluded.xlsx")
                    st.info("💡 基幹システムへのインポート専用フォルダ: \\\\192.168.1.77\\【新】共有\\【アシロボ】作業フォルダ\\販促\\価格更新")
                else:
                    st.warning("価格変動があった商品が見つかりませんでした。")
                
        except Exception as e:
            st.error(f"エラーが発生しました: {e}")
    else:
        st.info("サイドバーから 4 つのマスタファイルをアップロードしてください。")

if __name__ == "__main__":
    main()
