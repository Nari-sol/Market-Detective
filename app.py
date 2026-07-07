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
def generate_search_keywords(part_numbers, manufacturer):
    """ヤフオクのOR検索(vo)に渡すための品番バリエーション文字列を生成する (リストを受け取る)"""
    if isinstance(part_numbers, str):
        part_numbers = [part_numbers]
        
    all_variants = []
    manuf_clean = str(manufacturer).strip().upper()
    
    # メーカーの特定と代表名のマッピング
    detected_maker = ""
    if any(x in manuf_clean for x in ["トヨタ", "TOYOTA"]): detected_maker = "トヨタ"
    elif any(x in manuf_clean for x in ["日産", "NISSAN"]): detected_maker = "日産"
    elif any(x in manuf_clean for x in ["ホンダ", "HONDA"]): detected_maker = "ホンダ"
    elif any(x in manuf_clean for x in ["スズキ", "SUZUKI"]): detected_maker = "スズキ"
    elif any(x in manuf_clean for x in ["マツダ", "MAZDA"]): detected_maker = "マツダ"
    elif any(x in manuf_clean for x in ["ダイハツ", "DAIHATSU"]): detected_maker = "ダイハツ"
    elif any(x in manuf_clean for x in ["スバル", "SUBARU"]): detected_maker = "スバル"
    elif any(x in manuf_clean for x in ["レクサス", "LEXUS"]): detected_maker = "レクサス"
    elif any(x in manuf_clean for x in ["BMW"]): detected_maker = "BMW"
    elif any(x in manuf_clean for x in ["MINI", "ミニ"]): detected_maker = "MINI"
    elif any(x in manuf_clean for x in ["BENZ", "ベンツ", "メルセデス"]): detected_maker = "ベンツ"
    elif any(x in manuf_clean for x in ["AUDI", "アウディ"]): detected_maker = "アウディ"
    elif any(x in manuf_clean for x in ["VW", "フォルクスワーゲン"]): detected_maker = "VW"
    elif any(x in manuf_clean for x in ["PORSCHE", "ポルシェ"]): detected_maker = "ポルシェ"
    
    # マイナス検索キーワードの設定
    minus_kw = ""
    if detected_maker in ["トヨタ", "日産", "ホンダ", "スズキ", "マツダ", "ダイハツ", "スバル", "レクサス"]:
        minus_kw = " -ベンツ -メルセデス -BMW -MINI -アウディ -VW -ポルシェ"
    elif detected_maker in ["ベンツ", "BMW", "MINI", "アウディ", "VW", "ポルシェ"]:
        minus_kw = " -トヨタ -日産 -ホンダ -スズキ -ダイハツ -マツダ -スバル"
    
    for part_number in part_numbers:
        # 品番自体の記号を除去したもの
        base = re.sub(r'[^\w]', '', str(part_number)) 
        if not base:
            continue
            
        # ハイフンなしのベース品番の追加
        all_variants.append(base)
            
        # --- 1. 輸入車ルール ---
        if detected_maker in ["BMW", "MINI"]:
            if len(base) == 11 and base.isdigit():
                h_form = f"{base[:2]}-{base[2:4]}-{base[4:5]}-{base[5:8]}-{base[8:11]}"
                all_variants.append(h_form)
                
        elif detected_maker == "ベンツ":
            num_base = re.sub(r'[^0-9]', '', base)
            if len(num_base) == 10:
                # 10桁の数字単体
                all_variants.append(num_base)
                all_variants.append(f"{num_base[:3]}-{num_base[3:6]}-{num_base[6:8]}-{num_base[8:10]}")
                all_variants.append("A" + num_base)
                all_variants.append(f"A-{num_base[:3]}-{num_base[3:6]}-{num_base[6:8]}-{num_base[8:10]}")

        elif detected_maker in ["アウディ", "VW"]:
            if len(base) >= 9:
                h_form = f"{base[:3]}-{base[3:6]}-{base[6:9]}"
                if len(base) > 9:
                    h_form += f"-{base[9:]}"
                all_variants.append(h_form)

        elif detected_maker == "ポルシェ":
            if len(base) == 11:
                h_form = f"{base[:3]}-{base[3:6]}-{base[6:9]}-{base[9:11]}"
                all_variants.append(h_form)

        # --- 2. 国産車ルール ---
        elif detected_maker in ["トヨタ", "日産", "スズキ", "レクサス"]:
            if len(base) == 10:
                h_form = f"{base[:5]}-{base[5:10]}"
                all_variants.append(h_form)
                
        elif detected_maker == "ホンダ":
            if len(base) == 11:
                h_form = f"{base[:5]}-{base[5:8]}-{base[8:11]}"
                all_variants.append(h_form)
                
        elif detected_maker == "マツダ":
            if len(base) == 9:
                h_form = f"{base[:4]}-{base[4:6]}-{base[6:9]}"
                all_variants.append(h_form)
                
        elif detected_maker in ["ダイハツ", "スバル"]:
            if len(base) == 9:
                h_form = f"{base[:5]}-{base[5:9]}"
                all_variants.append(h_form)
            elif len(base) == 10:
                h_form = f"{base[:5]}-{base[5:10]}"
                all_variants.append(h_form)

        # --- 3. 汎用ルール (その他・メーカー不明) ---
        else:
            if len(base) == 8:
                h_form = f"{base[:4]}-{base[4:8]}"
                all_variants.append(h_form)
            elif len(base) == 10:
                h_form = f"{base[:5]}-{base[5:10]}"
                all_variants.append(h_form)
            
    unique_keywords = list(dict.fromkeys(all_variants))
    
    # 最後にマイナス検索キーワードを付与
    if minus_kw:
        unique_keywords = [f"{kw}{minus_kw}" for kw in unique_keywords]
        
    return unique_keywords

def parse_product_item(item, excluded_sellers=None):
    """ヤフオクの商品要素(li.Product)をパースし、価格とURLを返す。除外条件に合致する場合は None。"""
    # 出品者除外
    seller_el = item.select_one(".Product__bonus")
    if seller_el and excluded_sellers:
        seller_id = seller_el.get("data-auction-auc-seller-id", "").strip().lower()
        if seller_id in excluded_sellers:
            return None
            
    # ステータス除外
    status_el = item.select_one(".Product__icon")
    if status_el:
        status_text = status_el.get_text()
        if "新品" not in status_text and "未使用" not in status_text:
            return None

    # 即決価格属性の存在確認
    if not seller_el:
        return None
    buynow_raw = seller_el.get("data-auction-buynowprice")
    # 即決価格が設定されていない（オークションのみ、buynowpriceが 0 または空）商品は除外する
    if not buynow_raw or buynow_raw == "0":
        return None

    # タイトルによるセット品・複数個商品の除外フィルタリング
    link_el = item.select_one("a.Product__titleLink")
    if not link_el:
        return None
    title = link_el.get_text().strip()
    title_clean = unicodedata.normalize('NFKC', title)

    is_multiple = False
    if any(w in title_clean for w in ["セット", "まとめ", "複数"]):
        is_multiple = True
    else:
        if "個" in title_clean or "枚" in title_clean:
            matches = re.findall(r'(\d+)\s*(個|枚)', title_clean)
            if matches:
                for num_str, _ in matches:
                    if int(num_str) > 1:
                        is_multiple = True
                        break
            else:
                if not ("1個" in title_clean or "1枚" in title_clean):
                    is_multiple = True

    if is_multiple:
        return None
            
    # HTML上の価格表示要素から税込即決価格を取得
    price_els = item.select(".Product__priceValue")
    if not price_els:
        return None
    
    # オークションかつ即決ありの場合は price_els が 2つあり、2つ目が即決価格。
    # 定額（フリマ）の場合は price_els が 1つで、それが即決価格。
    if len(price_els) >= 2:
        price_el = price_els[1]
    else:
        price_el = price_els[0]

    try:
        price = int(re.sub(r"[^\d]", "", price_el.get_text()))
    except ValueError:
        return None
    
    postage = 0
    postage_el = item.select_one(".Product__postage")
    if postage_el:
        ptxt = postage_el.get_text()
        if not any(w in ptxt for w in ["送料無料", "出品者負担"]):
            nums = re.findall(r"\d+", ptxt.replace(",", ""))
            if nums: postage = int(nums[0])
            
    item_url = link_el.get("href") if link_el else "-"
    return {"total": price + postage, "url": item_url}

def get_yahoo_auction_prices(part_numbers, manufacturer="", excluded_sellers=None, individual_cache=None):
    import random
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0"
    ]

    # 各個別品番について検索を実行するためのヘルパー関数
    def fetch_items_for_query(query_keyword, sort_order):
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "ja-JP,ja;q=0.9"
        }
        params = {
            "p": query_keyword,
            "auccat": "26318",
            "istatus": "1",
            "fixed": "1",
            "ei": "UTF-8",
            "is_postage_mode": "1",
            "s1": "tbidorbuy",
            "o1": sort_order,
            "n": "100"
        }
        search_url = "https://auctions.yahoo.co.jp/search/search"
        soup = None
        
        # 2. 連続アクセス制限・ロボット判定対策のリトライ
        for attempt in range(4):
            try:
                res = requests.get(search_url, headers=headers, params=params, timeout=15)
                res.raise_for_status()
                soup = BeautifulSoup(res.text, "html.parser")
                items = soup.select("li.Product")
                if not items:
                    if "ロボット" in res.text or "captcha" in res.text.lower():
                        wait_time = random.uniform(3.0, 7.0) + (attempt * 2)
                        time.sleep(wait_time)
                        headers["User-Agent"] = random.choice(USER_AGENTS)
                        continue
                    return [], False
                
                # 次のページがあるかの判定
                has_next = False
                if soup.select_one(".Pager__list--next, .Pager__btn--next, .Pager__list--next a") or soup.select("a[href*='b=101']") or len(items) >= 100:
                    has_next = True
                
                parsed = []
                for item in items:
                    parsed_item = parse_product_item(item, excluded_sellers)
                    if parsed_item:
                        parsed.append(parsed_item)
                return parsed, has_next
            except Exception:
                wait_time = random.uniform(2.0, 5.0) + (attempt * 2)
                time.sleep(wait_time)
            finally:
                if soup:
                    soup.decompose()
        return [], False

    all_parsed = []
    
    # 複数の品番がスペース（半角/全角）区切りで渡されている場合、個別に配列に分割する
    split_parts = []
    for p in part_numbers:
        if isinstance(p, str):
            parts = re.split(r'[\s　]+', p)
            split_parts.extend([pt.strip() for pt in parts if pt.strip()])
        else:
            split_parts.append(p)
    unique_parts = list(dict.fromkeys(split_parts))
    
    # 検索ごとにウェイトを入れるためのフラグ
    need_sleep = False
    
    for part in unique_parts:
        query_keywords = generate_search_keywords([part], manufacturer)
        if not query_keywords:
            continue
            
        for query_keyword in query_keywords:
            if not query_keyword.strip():
                continue
                
            cache_key = (query_keyword, tuple(excluded_sellers) if excluded_sellers else ())
            
            # 個別キャッシュのチェック
            if individual_cache is not None and cache_key in individual_cache:
                items_for_this_part = individual_cache[cache_key]
                all_parsed.extend(items_for_this_part)
                continue
                
            # キャッシュにない実検索が発生した場合のみウェイト
            if need_sleep:
                time.sleep(2.5)
                
            items_asc, _ = fetch_items_for_query(query_keyword, "a")
            
            items_for_this_part = []
            items_for_this_part.extend(items_asc)
            
            # 安い順の結果の有無や件数にかかわらず、必ず高い順（降順）の検索を実行して合算する
            time.sleep(1.5)
            items_desc, _ = fetch_items_for_query(query_keyword, "d")
            items_for_this_part.extend(items_desc)
                
            all_parsed.extend(items_for_this_part)
            
            # 個別キャッシュへ格納
            if individual_cache is not None:
                individual_cache[cache_key] = items_for_this_part
                
            need_sleep = True

    if not all_parsed:
        return 0, "-", 0, 0
        
    # URLによる重複排除（同じオークションの重複を消す）
    unique_items = {}
    for item in all_parsed:
        url = item["url"]
        if url not in unique_items:
            unique_items[url] = item
            
    unique_list = list(unique_items.values())
    unique_list.sort(key=lambda x: x["total"])
    
    min_p = unique_list[0]["total"]
    min_url = unique_list[0]["url"]
    runner_up = unique_list[1]["total"] if len(unique_list) >= 2 else min_p
    max_p = unique_list[-1]["total"]
    
    return min_p, min_url, runner_up, max_p

# ==========================================
# ロジック関数：価格算出と利益判定
# ==========================================
def calculate_recommended_price(row, min_total, runner_up_total, max_total, is_strong_mode=False):
    if min_total == 0:
        return 0, 0.0, "❌ 取得不可", "市場データ取得不可のため手動確認"

    base_target = runner_up_total if is_strong_mode else min_total
    brand_type = str(row.get('ブランド区分', ''))
    item_name = str(row.get('商品名', ''))
    is_own_brand = (
        "自社ブランド" in brand_type or
        "CAPSOL" in item_name or
        "HAPAD" in item_name or
        "閃-SEN-" in item_name
    )
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
        target_total = (base_target + max_total) / 2
    else:
        target_total = base_target
        
    rec_price_incl_tax = target_total - own_shipping
    if rec_price_incl_tax <= 0:
        return 0, 0.0, "❌ 設定不可", "自社送料が市場の総額を上回るため設定不可"
        
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
def get_part_numbers_list(text):
    import re
    if not isinstance(text, str):
        return []

    # 「純正品番」という文字から、最初の日本語（ひらがな・カタカナ・漢字）が出現するまでの文字列を取得
    match = re.search(r'純正品番([^ぁ-んァ-ン一-龥]+)', text)
    if not match:
        return []

    raw_text = match.group(1)

    # 取得した文字列からHTMLタグ（BRやBなど）をスペースに置換して消去
    raw_text = re.sub(r'<[^>]+>', ' ', raw_text)

    # 残った文字列から英数字とハイフンの塊を抽出
    nums = re.findall(r'[A-Za-z0-9\-]+', raw_text)
    return [n.strip('-') for n in nums if len(n.strip('-')) >= 4]


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

def preprocess_masters(file_list, file_smile, file_ys, file_cost):
    """ベースリストに存在する品番だけをマスタから抽出して結合し、メモリを極小化する"""

    # 1. ベースリストの読み込み
    df_raw = load_df(file_list)
    list_part_col = df_raw.columns[0]
    list_manuf_col = find_col(df_raw, 'メーカー') or find_col(df_raw, 'ブランド')
    
    if list_manuf_col:
        df_base = df_raw[[list_part_col, list_manuf_col]].rename(columns={list_part_col: '管理品番', list_manuf_col: 'メーカー'}).copy()
    else:
        df_base = df_raw[[list_part_col]].rename(columns={list_part_col: '管理品番'}).copy()
        df_base['メーカー'] = ""
    
    df_base['管理品番'] = df_base['管理品番'].apply(clean_id)
    # 無効なキーを排除
    df_base = df_base[~df_base['管理品番'].astype(str).str.lower().isin(['nan', 'none', ''])].copy()
    
    # ゼロ落ち吸収用の結合キーを生成
    df_base['結合キー'] = df_base['管理品番'].apply(lambda x: x.lstrip('0') or '0')
    # ターゲット品番の抽出（これを使ってマスタを絞り込む）
    target_ids = set(df_base['結合キー'].unique())
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
    df_smile_agg['結合キー'] = df_smile_agg[smile_part_col].apply(lambda x: clean_id(x).lstrip('0') or '0')
    # 【最重要】ターゲット以外を即座に破棄
    df_smile_agg = df_smile_agg[df_smile_agg['結合キー'].isin(target_ids)].copy()
    
    df_smile_agg[smile_price_col] = pd.to_numeric(df_smile_agg[smile_price_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    df_smile_agg = df_smile_agg.groupby('結合キー').agg({smile_price_col: 'median'}).reset_index()
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
    df_cost_agg['結合キー'] = df_cost_agg[cost_part_col].apply(lambda x: clean_id(x).lstrip('0') or '0')
    # 【最重要】ターゲット以外を即座に破棄
    df_cost_agg = df_cost_agg[df_cost_agg['結合キー'].isin(target_ids)].copy()
    df_cost_agg[cost_price_col] = pd.to_numeric(df_cost_agg[cost_price_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    df_cost_agg = df_cost_agg.groupby('結合キー').agg({cost_price_col: 'median'}).reset_index()
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
    df_ys_clean['結合キー'] = df_ys_clean[ys_code_col].apply(lambda x: clean_id(x).lstrip('0') or '0')
    # 【最重要】ターゲット以外を即座に破棄
    df_ys_clean = df_ys_clean[df_ys_clean['結合キー'].isin(target_ids)].copy()
    
    # 枝番違いの additional1 を集約するため、まず各行で品番リスト化
    df_ys_clean['temp_parts'] = df_ys_clean[ys_add1_col].apply(get_part_numbers_list)
    
    # 価格の数値化（agg前に実施）
    if ys_price_col:
        df_ys_clean[ys_price_col] = pd.to_numeric(df_ys_clean[ys_price_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        
    # groupby 集約ルールの構築
    agg_dict = {
        ys_code_col: 'first',
        ys_name_col: 'first',
        ys_path_col: 'first',
        ys_price_col: 'first',
        # 各行の品番リストを足し合わせて重複排除
        'temp_parts': lambda x: list(dict.fromkeys([item for sublist in x for item in sublist if sublist]))
    }
    if ys_weight_col:
        agg_dict[ys_weight_col] = 'first'
        
    agg_dict = {k: v for k, v in agg_dict.items() if k in df_ys_clean.columns}
    
    df_ys_clean = df_ys_clean.groupby('結合キー').agg(agg_dict).reset_index()
    del df_raw
    gc.collect()

    # 5. 結合と後処理
    df_merged = pd.merge(df_base, df_smile_agg, on='結合キー', how='left')
    del df_smile_agg
    df_merged = pd.merge(df_merged, df_cost_agg, on='結合キー', how='left')
    del df_cost_agg
    df_merged = pd.merge(df_merged, df_ys_clean, on='結合キー', how='left')
    del df_ys_clean
    gc.collect()

    # YSマスタ側に正しい管理品番（ゼロ落ちなし）が存在する場合、ベースリスト側のゼロ落ち品番を上書き
    if ys_code_col and ys_code_col in df_merged.columns:
        df_merged['管理品番'] = df_merged.apply(
            lambda r: str(r[ys_code_col]).strip() if pd.notna(r[ys_code_col]) and str(r[ys_code_col]).strip().lower() not in ["", "nan"] else r['管理品番'],
            axis=1
        )

    df_merged['priority_shipping'] = df_merged[ys_name_col].astype(str).str.contains('送料185円').fillna(False)
    df_merged = df_merged.sort_values(by=['管理品番', 'priority_shipping'], ascending=[True, False])
    df_merged = df_merged.drop_duplicates(subset=['管理品番'], keep='first')

    # ユーザー指定 Step 3: 品番リストの抽出
    df_merged['品番リスト'] = df_merged['temp_parts'].apply(lambda x: x if isinstance(x, list) else [])
    # 空リストの場合のみ管理品番を返す（BMW・ベンツの強制上書きは廃止）
    df_merged['品番リスト'] = df_merged.apply(lambda row: row['品番リスト'] if row['品番リスト'] else [str(row['管理品番'])], axis=1)

    processed_data = []
    oem_list = ["BREMI", "FAE", "BOSCH", "Febi Bilstein", "NGK", "HELLA", "VEMO", "PAGID", "ERLING", "大野ゴム", "RAICAM", "DEPO", "MAHLE", "三ツ星ベルト", "Miyako", "Kashimura", "ALIC", "ミヤコ"]
    own_brand_keywords = ["HAPAD", "CAPSOL", "純正品", "閃-SEN-"]

    for _, row in df_merged.iterrows():
        management_id = str(row['管理品番'])
        search_id = row['品番リスト'] # List
        raw_name = row.get(ys_name_col, '')
        name = "" if pd.isna(raw_name) or str(raw_name).strip().lower() == "nan" else str(raw_name)
        
        raw_path = row.get(ys_path_col, '')
        path_val = "" if pd.isna(raw_path) or str(raw_path).strip().lower() == "nan" else str(raw_path)
        
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
                    # 各品番の後ろにメーカー名を追加してリストを更新
                    search_id = [f"{sid} {oem}" for sid in search_id]
                    break

        processed_data.append({
            '管理品番': management_id, '検索用品番': search_id, '販売価格': price,
            '送料': shipping, '下代': cost, 'ブランド区分': brand_type, 'カテゴリパス': path_val,
            'メーカー': str(row.get('メーカー', '')).replace('nan', ''),
            '商品名': name
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
        file_perf = st.file_uploader("5. 期間実績マスタ", type=["csv", "xlsx"])
        st.divider()
        st.header("⚙️ 設定")
        excluded_sellers_input = st.text_input(
            "除外する出品者ID（カンマ区切りで複数可）",
            value="665Hwri8MexoyraBB75HRiUiP2mJy,FJ5NeS99SC8HDJ1PhPKRoqDWW62UG,Bd1WtybfMbo2TWMwnogLzQqZH7Wam,7kRN5AJLxPj99m5XD1bnffyoWzGpN,CYWC2j57DBqjcNEscUthj1EQCzRbG,dB8iQzJmtyaCDc2KFcm7BTqBCzTX,6NXmtHnWrDxuqFQiPWsoPUoxSsZcz",
            help="自社や系列店のYahoo! JAPAN IDを入力してください"
        )
        excluded_sellers = [s.strip().lower() for s in excluded_sellers_input.split(',') if s.strip()]
        st.info("待機時間を設けてヤフオクのサーバー負荷を抑えながら実行します（1件あたり2.5秒）。")

    if file_list and file_smile and file_ys and file_cost and file_perf:
        # ファイル変更検知とキャッシュクリア
        current_files = [
            (file_list.name, file_list.size),
            (file_smile.name, file_smile.size),
            (file_ys.name, file_ys.size),
            (file_cost.name, file_cost.size),
            (file_perf.name, file_perf.size),
        ]
        if 'last_files' not in st.session_state or st.session_state['last_files'] != current_files:
            if 'integrated_data' in st.session_state:
                del st.session_state['integrated_data']
            if 'analysis_results' in st.session_state:
                del st.session_state['analysis_results']
            if 'rpa_verified_df' in st.session_state:
                del st.session_state['rpa_verified_df']
            st.session_state['last_files'] = current_files

        try:
            st.success("✅ 5つのマスタファイルを認識しました。分析を開始します。")
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
            df_preview = df_input.drop(columns=['カテゴリパス']).copy()
            # リスト型や混在型によるArrowTypeErrorを防止するため、型を明示的にキャスト
            if '検索用品番' in df_preview.columns:
                df_preview['検索用品番'] = df_preview['検索用品番'].apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x))
            
            preview_schema = {
                '管理品番': str,
                '検索用品番': str,
                '販売価格': int,
                '送料': int,
                '下代': int,
                'ブランド区分': str,
                'メーカー': str,
                '商品名': str
            }
            for col, dtype in preview_schema.items():
                if col in df_preview.columns:
                    try:
                        if dtype == int:
                            df_preview[col] = pd.to_numeric(df_preview[col], errors='coerce').fillna(0).astype(int)
                        else:
                            df_preview[col] = df_preview[col].astype(str)
                    except:
                        df_preview[col] = df_preview[col].astype(str)
            
            st.dataframe(df_preview, width='stretch', hide_index=True)

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
                    # 期間実績マスタの結合（分析開始時に実行）
                    with st.spinner("期間実績マスタを結合中..."):
                        try:
                            if file_perf.name.endswith('.csv'):
                                df_perf_raw = robust_read_csv(file_perf, header=4)
                            else:
                                df_perf_raw = pd.read_excel(file_perf, header=4, dtype=str)
                            
                            perf_code_col = find_col(df_perf_raw, '商品コード') or find_col(df_perf_raw, '商品ｺｰﾄﾞ')
                            perf_qty_col = find_col(df_perf_raw, '累計売上数量') or find_col(df_perf_raw, '売上数量')
                            
                            if perf_code_col and perf_qty_col:
                                df_perf = df_perf_raw[[perf_code_col, perf_qty_col]].copy()
                                df_perf['管理品番'] = df_perf[perf_code_col].apply(clean_id)
                                df_perf[perf_qty_col] = pd.to_numeric(df_perf[perf_qty_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                                df_perf_agg = df_perf.groupby('管理品番').agg({perf_qty_col: 'sum'}).reset_index()
                                df_to_analyze = pd.merge(df_input, df_perf_agg, on='管理品番', how='left').fillna({'累計売上数量': 0})
                                sales_col = perf_qty_col
                            else:
                                df_to_analyze = df_input.copy()
                                df_to_analyze['累計売上数量'] = 0
                                sales_col = '累計売上数量'
                        except Exception:
                            df_to_analyze = df_input.copy()
                            df_to_analyze['累計売上数量'] = 0
                            sales_col = '累計売上数量'

                    results = []
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    total_rows = len(df_to_analyze)
                    
                    # 検索クエリ用の実行時キャッシュ
                    query_cache = {}
                    individual_cache = {}
                    
                    for i, row in df_to_analyze.iterrows():
                        management_id = str(row['管理品番'])
                        search_ids = row['検索用品番'] # これはリスト
                        manuf_context = f"{row.get('メーカー', '')} {row.get('カテゴリパス', '')} {row.get('商品名', '')}"
                        status_text.markdown(f"<div class='status-box'>🔎 調査中 ({i+1}/{total_rows}): <b>{management_id}</b></div>", unsafe_allow_html=True)
                        
                        # 強気モード判定（元の販売価格と累計売上数量に基づく）
                        orig_price_val = row['販売価格']
                        sales_qty = row.get(sales_col, 0)
                        is_strong = False
                        if orig_price_val < 3000 and sales_qty >= 30: is_strong = True
                        elif 3000 <= orig_price_val < 10000 and sales_qty >= 15: is_strong = True
                        elif 10000 <= orig_price_val < 30000 and sales_qty >= 5: is_strong = True
                        elif orig_price_val >= 30000 and sales_qty >= 2: is_strong = True
                        
                        mode_str = "強気モード" if is_strong else "通常モード"
                        
                        query_keywords = generate_search_keywords(search_ids, manuf_context)
                        query_keyword_str = ", ".join(query_keywords)
                        
                        # キャッシュ用のキー（検索キーワードリストと除外出品者の組み合わせ）
                        cache_key = (tuple(query_keywords), tuple(excluded_sellers) if excluded_sellers else ())
                        
                        is_cached = False
                        if cache_key in query_cache:
                            min_p, min_url, runner_up, max_p = query_cache[cache_key]
                            is_cached = True
                        else:
                            min_p, min_url, runner_up, max_p = get_yahoo_auction_prices(search_ids, manuf_context, excluded_sellers, individual_cache=individual_cache)
                            query_cache[cache_key] = (min_p, min_url, runner_up, max_p)
                        
                        final_rec, final_m, status, reason = calculate_recommended_price(row, min_p, runner_up, max_p, is_strong)
                        
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
                            "管理品番": management_id, "抽出した純正品番": ", ".join(search_ids), "元販売価格(込)": orig_price_display,
                            "ブランド区分": row['ブランド区分'], "ヤフオク最安値": min_p if min_p > 0 else "取得不可",
                            "最安値商品URL": min_url,
                            "ヤフオク次点": runner_up if runner_up > 0 else "取得不可",
                            "ヤフオク最高値": max_p if max_p > 0 else "取得不可", "判定モード": mode_str,
                            "推奨価格(込)": final_rec if final_rec > 0 else "-",
                            "粗利率(税抜)": f"{final_m*100:.1f}%" if final_rec > 0 else "-", "ステータス": status, 
                            "備考（調整理由）": reason, "実際のヤフオク検索キーワード": query_keyword_str
                        })
                        if i % 10 == 0: gc.collect()
                        progress_bar.progress((i + 1) / total_rows)
                        
                        # 新規にスクレイピングした場合のみウェイトを入れる（キャッシュヒット時はスキップして高速化）
                        if not is_cached:
                            time.sleep(2.5)
                    
                    status_text.success(f"✅ 全 {total_rows} 件の分析が完了しました。")
                    df_result = pd.DataFrame(results)
                    
                    # 画面表示およびシリアライズ時のArrowTypeError対策として、全列の型を明示的にキャスト
                    result_schema = {
                        "管理品番": str,
                        "抽出した純正品番": str,
                        "元販売価格(込)": str,
                        "ブランド区分": str,
                        "ヤフオク最安値": str,
                        "最安値商品URL": str,
                        "ヤフオク次点": str,
                        "ヤフオク最高値": str,
                        "判定モード": str,
                        "推奨価格(込)": str,
                        "粗利率(税抜)": str,
                        "ステータス": str,
                        "備考（調整理由）": str,
                        "実際のヤフオク検索キーワード": str
                    }
                    if df_result.empty:
                        df_result = pd.DataFrame(columns=result_schema.keys()).astype(result_schema)
                    else:
                        for col, dtype in result_schema.items():
                            if col in df_result.columns:
                                df_result[col] = df_result[col].astype(dtype)
                    
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
                        
                        # キントーン用：A〜G列のみを出力
                        df_kintone_export = pd.DataFrame({
                            0: ["*"] * len(df_export), 1: df_export["管理品番"].values,
                            2: [today_str] * len(df_export), 3: df_export["元販売価格(込)"].values,
                            4: df_export["推奨価格(込)"].values, 5: ("価格調整 " + df_export["粗利率(税抜)"].astype(str)).values,
                            6: [""] * len(df_export)
                        })
                        header_row = ["レコードの開始行", "管理品番", "変更日付", "変更前", "変更後", "備考", "担当者"]
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
                df_to_show = res['df_result'].copy()
                
                # 古いキャッシュデータや環境差異によるArrowTypeErrorを完全に防止するため、描画直前にも文字列にキャスト
                result_schema = {
                    "管理品番": str,
                    "抽出した純正品番": str,
                    "元販売価格(込)": str,
                    "ブランド区分": str,
                    "ヤフオク最安値": str,
                    "最安値商品URL": str,
                    "ヤフオク次点": str,
                    "ヤフオク最高値": str,
                    "判定モード": str,
                    "推奨価格(込)": str,
                    "粗利率(税抜)": str,
                    "ステータス": str,
                    "備考（調整理由）": str,
                    "実際のヤフオク検索キーワード": str
                }
                for col, dtype in result_schema.items():
                    if col in df_to_show.columns:
                        df_to_show[col] = df_to_show[col].astype(dtype)
                
                st.dataframe(df_to_show, width='stretch', hide_index=True)

                if not res['df_export'].empty:
                    st.divider()
                    col1, col2, col3, col4 = st.columns(4)
                    with col1: st.download_button(label="📥 調整済みリスト", data=res['output_result'], file_name="market_detective_result.xlsx")
                    with col2: st.download_button(label="📥 SMILE用", data=res['output_smile'], file_name="smile_import.xlsx")
                    with col3: st.download_button(label="📥 キントーン用", data=res['output_kintone'], file_name="kintone_import.xlsx")
                    with col4:
                        if res['output_ex']:
                            st.download_button(label="📥 除外リスト", data=res['output_ex'], file_name="market_detective_excluded.xlsx")
                    st.info(r"💡 基幹システムへのインポート専用フォルダ: \\\\192.168.1.77\【新】共有\【アシロボ】作業フォルダ\販促\価格更新")
                else:
                    st.warning("価格変動があった商品が見つかりませんでした。")
                
                # 【検証用機能】RPA検証UIの呼び出し
                render_rpa_validation_ui(excluded_sellers)
                
        except Exception as e:
            st.error(f"エラーが発生しました: {e}")
    else:
        st.info("サイドバーから 5 つのマスタファイルをアップロードしてください。")

# ==========================================
# 【検証用機能】RPA自動検証（デプロイ前一時テスト用）
# ==========================================
def get_rpa_items_for_query(driver, query_keyword, sort_order, excluded_sellers):
    import unicodedata
    import re
    from bs4 import BeautifulSoup
    import time
    import urllib.parse
    
    try:
        params = {
            "p": query_keyword,
            "auccat": "26318",
            "istatus": "1",
            "fixed": "1",
            "ei": "UTF-8",
            "is_postage_mode": "1",
            "s1": "tbidorbuy",
            "o1": sort_order,
            "n": "100"
        }
        url = "https://auctions.yahoo.co.jp/search/search?" + urllib.parse.urlencode(params)
        driver.get(url)
        time.sleep(3.0)

    except Exception as e:
        print(f"Error during RPA flow for query '{query_keyword}': {e}")
        return [], False
        
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select("li.Product")
    
    if not items:
        return [], False
        
    parsed = []
    for item in items:
        parsed_item = parse_product_item(item, excluded_sellers)
        if parsed_item:
            parsed.append(parsed_item)
            
    # 次のページがあるかの判定
    has_next = False
    if soup.select_one(".Pager__list--next, .Pager__btn--next, .Pager__list--next a") or soup.select("a[href*='b=101']") or len(items) >= 100:
        has_next = True
        
    return parsed, has_next

def run_rpa_validation(df_result, excluded_sellers, rpa_show_gui, progress_bar, status_text):
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    import time
    
    # ドライバーの設定
    chrome_options = Options()
    if not rpa_show_gui:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = None
    rpa_results = []
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
    except Exception as e:
        status_text.error(f"Selenium WebDriverの起動に失敗しました。Chromeがインストールされているか確認してください。エラー: {e}")
        return None
        
    total_rows = len(df_result)
    
    try:
        for idx, row in df_result.iterrows():
            management_id = row["管理品番"]
            query_keyword_str = row["実際のヤフオク検索キーワード"]
            query_keywords = [k.strip() for k in str(query_keyword_str).split(",")] if query_keyword_str else []
            
            status_text.markdown(f"<div class='status-box'>🤖 RPA検証中 ({idx+1}/{total_rows}): <b>{management_id}</b></div>", unsafe_allow_html=True)
            
            if not query_keywords or all(not k for k in query_keywords):
                rpa_results.append({
                    "RPA確認：最安値": "取得不可",
                    "RPA確認：最高値": "取得不可"
                })
                progress_bar.progress((idx + 1) / total_rows)
                continue
                
            has_error = False
            all_rpa_parsed = []
            
            for query_keyword in query_keywords:
                if not query_keyword: continue
                try:
                    # 安い順で検索して全商品を取得
                    items_asc, _ = get_rpa_items_for_query(driver, query_keyword, "a", excluded_sellers)
                    all_rpa_parsed.extend(items_asc)
                    
                    time.sleep(2.0)
                    
                    # 高い順（降順）の検索も実行して合算する
                    items_desc, _ = get_rpa_items_for_query(driver, query_keyword, "d", excluded_sellers)
                    all_rpa_parsed.extend(items_desc)
                    time.sleep(2.0)
                        
                except Exception as item_err:
                    print(f"Error validating {management_id} with keyword {query_keyword}: {item_err}")
                    has_error = True
            
            if has_error and not all_rpa_parsed:
                rpa_min = "エラー"
                rpa_max = "エラー"
            elif not all_rpa_parsed:
                rpa_min = "取得不可"
                rpa_max = "取得不可"
            else:
                # URLによる重複排除
                unique_rpa_items = {}
                for item in all_rpa_parsed:
                    url = item["url"]
                    if url not in unique_rpa_items:
                        unique_rpa_items[url] = item
                        
                unique_rpa_list = list(unique_rpa_items.values())
                unique_rpa_list.sort(key=lambda x: x["total"])
                
                rpa_min = unique_rpa_list[0]["total"]
                rpa_max = unique_rpa_list[-1]["total"]
                
            rpa_results.append({
                "RPA確認：最安値": str(rpa_min),
                "RPA確認：最高値": str(rpa_max)
            })
            
            progress_bar.progress((idx + 1) / total_rows)
            time.sleep(2.5) # 全体的な負荷軽減ウェイト
            
    finally:
        if driver:
            driver.quit()
            
    return rpa_results

def render_rpa_validation_ui(excluded_sellers):
    import io
    
    if 'analysis_results' not in st.session_state:
        return
        
    res = st.session_state['analysis_results']
    df_result = res['df_result']
    
    st.divider()
    st.subheader("🤖 スクレイピング精度 自動検証 (RPA)")
    st.markdown("通常の requests によるスクレイピング結果と、Selenium による実ブラウザ操作での結果を自動で突き合わせ、取得漏れやロジズレを検証します。")
    
    rpa_show_gui = st.checkbox("RPA実行時にブラウザの動きを表示する（デモ用）", value=False, key="cb_rpa_show_gui")
    rpa_done = 'rpa_verified_df' in st.session_state
    
    if st.button("🤖 RPA自動検証スタート", key="btn_rpa_verify"):
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        with st.spinner("Seleniumを起動して自動検証を実行中..."):
            rpa_data = run_rpa_validation(df_result, excluded_sellers, rpa_show_gui, progress_bar, status_text)
            
        if rpa_data is not None:
            status_text.success("✅ RPA自動検証が完了しました。")
            
            df_verified = df_result.copy()
            
            rpa_min_list = [item["RPA確認：最安値"] for item in rpa_data]
            rpa_max_list = [item["RPA確認：最高値"] for item in rpa_data]
            
            df_verified["RPA確認：最安値"] = rpa_min_list
            df_verified["RPA確認：最高値"] = rpa_max_list
            
            judge_list = []
            for _, row in df_verified.iterrows():
                app_min_str = str(row["ヤフオク最安値"]).strip()
                app_max_str = str(row["ヤフオク最高値"]).strip()
                rpa_min_str = str(row["RPA確認：最安値"]).strip()
                rpa_max_str = str(row["RPA確認：最高値"]).strip()
                
                if app_min_str == rpa_min_str and app_max_str == rpa_max_str:
                    if app_min_str == "取得不可" and app_max_str == "取得不可":
                        judge_list.append("✅ 一致 (取得不可)")
                    else:
                        judge_list.append("✅ 一致")
                else:
                    judge_list.append("⚠️ ズレあり")
                    
            df_verified["RPA判定"] = judge_list
            
            st.session_state['rpa_verified_df'] = df_verified
            rpa_done = True
            
    if rpa_done:
        df_show = st.session_state['rpa_verified_df'].copy()
        
        for col in df_show.columns:
            df_show[col] = df_show[col].astype(str)
            
        st.subheader("📊 RPA検証結果プレビュー")
        
        mismatch_count = len(df_show[df_show["RPA判定"] == "⚠️ ズレあり"])
        if mismatch_count > 0:
            st.warning(f"⚠️ 検証結果: {mismatch_count} 件の商品で requests と RPA(Selenium) の取得価格にズレが検出されました。")
        else:
            st.success("✅ 検証結果: すべての商品で requests と RPA(Selenium) の取得結果が一致しました。")
            
        st.dataframe(df_show, width='stretch', hide_index=True)
        
        output_rpa = io.BytesIO()
        with pd.ExcelWriter(output_rpa, engine='openpyxl') as writer:
            df_show.to_excel(writer, index=False, sheet_name='RPA検証結果')
            
        st.download_button(
            label="📥 RPA検証結果を含むExcelをダウンロード",
            data=output_rpa.getvalue(),
            file_name="market_detective_rpa_verified.xlsx",
            key="btn_rpa_download"
        )

if __name__ == "__main__":
    main()
