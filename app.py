import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import io
import zipfile
import easyocr
from thefuzz import process
from PIL import Image
import gspread
from google.oauth2.service_account import Credentials
import matplotlib.font_manager as fm

# ==========================================
# 1. GLOBAL SETUP & CONFIG
# ==========================================
st.set_page_config(page_title="MeAndBro Guild Manager", layout="wide")

# ตั้งค่า Font กราฟ (รองรับภาษาไทย)
# ตั้งค่า Font กราฟ (รองรับภาษาไทย)
font_path = './NotoSansThai-Regular.ttf'
fe = fm.FontEntry(
    fname=font_path,
    name='NotoSansThai'
)
fm.fontManager.ttflist.insert(0, fe)
plt.rcParams['font.family'] = fe.name

# บรรทัดนี้ให้ลบออกหรือใส่ # ไว้ครับ เพื่อป้องกันความสับสนของ Library
plt.rcParams['font.sans-serif'] = ['Tahoma', 'Sarabun', 'Arial Unicode MS', 'DejaVu Sans', 'sans-serif']

# รายชื่อบอส (หัวตาราง)
days_cols = ["ลูดี้", "ไอลีน", "ราเชล", "เดลโลน", "เจฟ", "สไปร์ค", "คริส"]

# ==========================================
# 2. GOOGLE SHEETS CONNECTION FUNCTIONS
# ==========================================
@st.cache_resource
def get_gsheet_client():
    if "gcp_service_account" not in st.secrets:
        st.error("ไม่พบข้อมูล 'gcp_service_account' ใน secrets.toml")
        return None
    
    scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scope)
    client = gspread.authorize(creds)
    return client

def load_data_from_gsheet(sheet_url, worksheet_name):
    try:
        client = get_gsheet_client()
        if not client: return None
        
        sh = client.open_by_url(sheet_url)
        
        try:
            worksheet = sh.worksheet(worksheet_name)
        except:
            worksheet = sh.add_worksheet(title=worksheet_name, rows="100", cols="20")
            headers = ["ชื่อสมาชิก"] + days_cols
            worksheet.append_row(headers)
            return pd.DataFrame(columns=headers)

        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        
        if "ชื่อสมาชิก" not in df.columns:
            df["ชื่อสมาชิก"] = ""
        for col in days_cols:
            if col not in df.columns:
                df[col] = 0.0
                
        df["ชื่อสมาชิก"] = df["ชื่อสมาชิก"].astype(str)
        for col in days_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
        return df
    except Exception as e:
        st.error(f"❌ Error Loading Sheet: {e}")
        return None

def save_data_to_gsheet(sheet_url, worksheet_name, df):
    try:
        client = get_gsheet_client()
        if not client: return False

        sh = client.open_by_url(sheet_url)
        try:
            worksheet = sh.worksheet(worksheet_name)
        except:
            worksheet = sh.add_worksheet(title=worksheet_name, rows="100", cols="20")

        export_cols = ["ชื่อสมาชิก"] + days_cols
        for c in export_cols:
            if c not in df.columns: df[c] = 0

        worksheet.clear()
        worksheet.update([export_cols])
        
        val_list = df[export_cols].values.tolist()
        clean_list = []
        for row in val_list:
            clean_row = []
            for item in row:
                if isinstance(item, (int, float, np.integer, np.floating)):
                    clean_row.append(float(item))
                else:
                    clean_row.append(str(item))
            clean_list.append(clean_row)
            
        if clean_list:
            worksheet.update("A2", clean_list)
            
        # ใช้ Emoji แทน text เพื่อแก้ error icon="cloud"
        st.toast("✅ บันทึกข้อมูลลง Google Sheet เรียบร้อย!", icon="☁️")
        return True
    except Exception as e:
        st.error(f"❌ Error Saving: {e}")
        return False

# ==========================================
# 3. EASYOCR SETUP
# ==========================================
@st.cache_resource
def load_reader():
    return easyocr.Reader(['th', 'en'])

reader = load_reader()

# ==========================================
# 4. SESSION STATE INITIALIZATION
# ==========================================
if 'main_df' not in st.session_state:
    st.session_state.main_df = pd.DataFrame(columns=["ชื่อสมาชิก"] + days_cols)
if 'prev_df' not in st.session_state:
    st.session_state.prev_df = None
if 'guild_name' not in st.session_state:
    st.session_state.guild_name = "MeAndBro"
if 'pending_new_members' not in st.session_state: 
    st.session_state.pending_new_members = []
if 'scan_target_boss' not in st.session_state: 
    st.session_state.scan_target_boss = None

if 'sheet_url' not in st.session_state: 
    if "sheet_config" in st.secrets and "spreadsheet_url" in st.secrets["sheet_config"]:
        st.session_state.sheet_url = st.secrets["sheet_config"]["spreadsheet_url"]
    else:
        st.session_state.sheet_url = "" 
        
if 'current_sheet_id' not in st.session_state: 
    st.session_state.current_sheet_id = "1"

# ==========================================
# 5. TAB 1: DASHBOARD
# ==========================================
def render_dashboard_tab():
    st.title("📊 Guild Damage Manager (Google Sheets Edition)")

    with st.expander("⚙️ ตรวจสอบการเชื่อมต่อ", expanded=not st.session_state.sheet_url):
        st.session_state.sheet_url = st.text_input("🔗 Google Sheet URL:", st.session_state.sheet_url)
        if not st.session_state.sheet_url:
            st.warning("กรุณาใส่ Google Sheet URL หรือตั้งค่าใน secrets.toml")
            return

    col_c1, col_c2, col_c3 = st.columns([1, 1, 2])
    with col_c1:
        target_sheet_name = st.text_input("📄 ชื่อหน้า (Worksheet Name)", value=st.session_state.current_sheet_id)
        st.session_state.current_sheet_id = target_sheet_name
    
    with col_c2:
        st.write("") 
        st.write("") 
        if st.button("📥 โหลดข้อมูล (Load)", type="primary", use_container_width=True):
            df = load_data_from_gsheet(st.session_state.sheet_url, target_sheet_name)
            if df is not None:
                st.session_state.main_df = df
                st.success(f"โหลดข้อมูลจากหน้า '{target_sheet_name}' สำเร็จ")

    with col_c3:
        st.session_state.guild_name = st.text_input("ชื่อกิลด์ (หัวกราฟ):", st.session_state.guild_name)

    st.divider()

    st.subheader(f"📝 แก้ไขข้อมูล (หน้า: {st.session_state.current_sheet_id})")
    
    c_f1, c_f2 = st.columns([3, 1])
    with c_f1: search_term = st.text_input("🔍 ค้นหาชื่อ:", placeholder="พิมพ์ชื่อ...")
    with c_f2: filter_zero = st.checkbox("⚠️ แสดงเฉพาะ 0")

    display_df = st.session_state.main_df.copy()
    if search_term:
        display_df = display_df[display_df['ชื่อสมาชิก'].astype(str).str.contains(search_term, case=False, na=False)]
    if filter_zero:
        display_df = display_df[display_df[days_cols].eq(0).any(axis=1)]

    column_config = {"ชื่อสมาชิก": st.column_config.TextColumn("ชื่อสมาชิก", width="medium")}
    for col in days_cols: column_config[col] = st.column_config.NumberColumn(col, format="%d")

    edited_df = st.data_editor(
        display_df,
        column_config=column_config,
        num_rows="dynamic",
        use_container_width=True,
        key="main_editor",
        height=400
    )

    if not search_term and not filter_zero:
        st.session_state.main_df = edited_df
    else:
        st.session_state.main_df.update(edited_df)

    st.write("")
    if st.button("💾 บันทึกการแก้ไขลง Google Sheet (Save & Sync)", type="primary"):
        save_data_to_gsheet(st.session_state.sheet_url, st.session_state.current_sheet_id, st.session_state.main_df)

    st.divider()
    st.subheader("📈 เปรียบเทียบพัฒนาการ (Compare)")
    col_g1, col_g2 = st.columns([2, 1])
    with col_g1:
        prev_sheet_name = st.text_input("ชื่อหน้าของสัปดาห์ก่อน (เช่น 1):", placeholder="ใส่ชื่อหน้า...")
    with col_g2:
        st.write("")
        st.write("")
        if st.button("ดึงข้อมูลเก่ามาเทียบ"):
            if prev_sheet_name:
                df_prev = load_data_from_gsheet(st.session_state.sheet_url, prev_sheet_name)
                if df_prev is not None:
                    st.session_state.prev_df = df_prev
                    st.success("โหลดข้อมูลเก่าเรียบร้อย")

    df_growth_export = None
    if st.session_state.prev_df is not None:
        curr = st.session_state.main_df.copy()
        prev = st.session_state.prev_df.copy()
        curr["Total"] = curr[days_cols].sum(axis=1)
        prev["Total"] = prev[days_cols].sum(axis=1)
        
        growth = pd.merge(curr[["ชื่อสมาชิก", "Total"]], prev[["ชื่อสมาชิก", "Total"]], on="ชื่อสมาชิก", how="left", suffixes=("_Cur", "_Prev"))
        growth["Total_Prev"] = growth["Total_Prev"].fillna(0)
        growth["Diff"] = growth["Total_Cur"] - growth["Total_Prev"]
        
        # แก้ไข format เพื่อไม่ให้ error กับ string column (ValueError fixed)
        st.dataframe(
            growth.sort_values("Diff", ascending=False).style.format({
                "Total_Cur": "{:,.0f}", 
                "Total_Prev": "{:,.0f}", 
                "Diff": "{:+,.0f}"
            }), 
            use_container_width=True, 
            hide_index=True
        )
        df_growth_export = growth

    st.divider()
    if st.button("🖼️ สร้างกราฟและดาวน์โหลด (ZIP Images Only)"):
        generate_and_download_images(df_growth_export)

def generate_and_download_images(growth_df=None):
    df = st.session_state.main_df.copy()
    # กรองแถวว่างออกก่อนสร้างกราฟ
    df = df[df["ชื่อสมาชิก"].str.strip() != ""]
    
    if df.empty:
        st.error("ไม่มีข้อมูลสำหรับสร้างกราฟ")
        return

    df["Total Damage"] = df[days_cols].sum(axis=1)
    
    # --- Graph 1: Ranking (Total) ---
    df_sorted = df.sort_values("Total Damage", ascending=True)
    fig1, ax1 = plt.subplots(figsize=(10, max(5, len(df) * 0.45)))
    bars = ax1.barh(df_sorted["ชื่อสมาชิก"].astype(str), df_sorted["Total Damage"], color='#2196F3', zorder=3)
    ax1.set_title(f"Ranking: {st.session_state.guild_name}", fontsize=14, fontweight='bold')
    ax1.set_xticks([]) # Remove x-axis numbers as requested
    # ax1.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{x/1e6:.1f}M'))
    # ax1.xaxis.set_major_locator(ticker.MultipleLocator(1000000)) # 1M Interval
    # ax1.grid(axis='x', linestyle='--', alpha=0.6, zorder=0) # Removed grid lines as requested
    
    for bar in bars:
        width = bar.get_width()
        ax1.text(width * 1.01, bar.get_y() + bar.get_height()/2, f' {width:,.0f}', va='center', fontsize=9)
    plt.tight_layout()

    # --- Graph 2: Breakdown (GROUPED BAR - แบบเก่า) ---
    # เปลี่ยน Logic กลับมาเป็นแบบแยกแท่ง (Side-by-side)
    fig2, ax2 = plt.subplots(figsize=(16, 8))
    df_desc = df.sort_values("Total Damage", ascending=False)
    
    x = np.arange(len(df_desc))
    width = 0.1  # ความกว้างของแท่งแต่ละบอส
    colors = ['#FF5733', '#FFC300', '#DAF7A6', '#33FF57', '#3380FF', '#FF33A8', '#8D33FF']
    
    # วนลูปสร้างกราฟแท่งทีละบอส โดยขยับแกน x ไปทีละนิด
    for i, day in enumerate(days_cols):
        ax2.bar(x + (i * width), df_desc[day], width, label=day, color=colors[i], zorder=3)

    # เพิ่มเส้นคั่นระหว่างคน (Separator Lines)
    for i in range(len(df_desc) - 1):
        ax2.axvline(x=i + 0.85, color='gray', linestyle=':', alpha=0.5, zorder=1)
    
    # จัดตำแหน่งชื่อสมาชิกให้อยู่กึ่งกลางกลุ่มแท่งกราฟ     # (มี 7 บอส กึ่งกลางคือช่องที่ 3.5 -> width * 3)
    ax2.set_xticks(x + (width * 3))
    ax2.set_xticklabels(df_desc["ชื่อสมาชิก"].astype(str), rotation=45, ha='right', fontsize=10)

    # เพิ่มเส้นเกณฑ์ (Threshold Lines) แบบเก่า
    ax2.axhline(y=2500000, color='red', linestyle='--', linewidth=1.5, label="2.5M กายภาพ", zorder=4)
    ax2.axhline(y=1500000, color='#00BFFF', linestyle='--', linewidth=1.5, label="1.5M เวทย์", zorder=4)

    ax2.set_title(f"Damage Breakdown: {st.session_state.guild_name}", fontsize=14, fontweight='bold')
    ax2.legend()
    ax2.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{x/1e6:.1f}M'))
    ax2.yaxis.set_major_locator(ticker.MultipleLocator(1000000)) # 1M Interval
    ax2.grid(axis='y', linestyle='--', alpha=0.6, zorder=0)
    plt.tight_layout()

    # --- ZIP Creation ---
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "a", zipfile.ZIP_DEFLATED, False) as z:
        img1 = io.BytesIO(); fig1.savefig(img1, format='png'); z.writestr("Rank_Graph.png", img1.getvalue())
        img2 = io.BytesIO(); fig2.savefig(img2, format='png'); z.writestr("Weekly_Graph.png", img2.getvalue())
    
    st.write("### Preview:")
    st.pyplot(fig1)
    st.pyplot(fig2)
    st.download_button("💾 Download ZIP", data=zip_buf.getvalue(), file_name="Guild_Graphs.zip", mime="application/zip")

# ==========================================
# 6. TAB 2: OCR AUTO-FILLER
# ==========================================
def render_ocr_tab():
    st.header("🤖 OCR Auto-Filler")
    st.caption("ระบบจะอ่านค่าจากรูปภาพและอัปเดตลงตารางชั่วคราว (เมื่อเสร็จแล้ว อย่าลืมกด Save ลง Google Sheet ที่หน้าแรก)")
    
    st.info(f"📁 สมาชิกในระบบปัจจุบัน: {len(st.session_state.main_df)} คน (ข้อมูลนี้รอการบันทึก)")

    col1, col2 = st.columns([1, 2])
    with col1:
        selected_boss = st.selectbox("1. เลือกบอสที่จะสแกน", days_cols)
    with col2:
        uploaded_images = st.file_uploader("2. อัปโหลดรูป (รองรับหลายไฟล์)", type=['png', 'jpg', 'jpeg'], accept_multiple_files=True)

    if uploaded_images:
        st.divider()
        ignore_words = ["rank", "score", "damage", "total", "guild", "boss", "level", "lv", "name", "point"]
        if "guild_name" in st.session_state:
            ignore_words.append(st.session_state.guild_name.lower())

        if st.button("🚀 เริ่มอ่าน (Scan Images)", type="primary"):
            st.session_state.scan_target_boss = selected_boss
            target_boss = st.session_state.scan_target_boss
            
            all_match_log = []
            new_candidates = []
            
            # แปลงเป็น list string ให้หมด เพื่อความชัวร์ และกรองค่าว่าง (Fix NoneType error)
            existing_names = [str(x) for x in st.session_state.main_df["ชื่อสมาชิก"].tolist() if str(x).strip() != ""]
            
            progress_bar = st.progress(0)
            status_text = st.empty()

            for idx, img_file in enumerate(uploaded_images):
                status_text.text(f"Processing image {idx+1}/{len(uploaded_images)}...")
                try:
                    image = Image.open(img_file)
                    img_np = np.array(image)
                    results = reader.readtext(img_np, adjust_contrast=0.7, text_threshold=0.5, low_text=0.35)
                    
                    text_blocks = []
                    for (bbox, text, prob) in results:
                        (tl, tr, br, bl) = bbox
                        center_y = int((tl[1] + bl[1]) / 2)
                        text_blocks.append({"text": text.strip(), "y": center_y, "x": int(tl[0])})

                    text_blocks.sort(key=lambda k: k['y'])
                    used_blocks = []
                    
                    for i, block_num in enumerate(text_blocks):
                        num_text = block_num['text']
                        clean_num = num_text.replace(',', '').replace('.', '')
                        
                        if clean_num.isdigit() and len(clean_num) >= 4:
                            best_name = None
                            found_name_index = -1
                            
                            for j, block_name in enumerate(text_blocks):
                                if i == j or j in used_blocks: continue
                                if abs(block_num['y'] - block_name['y']) < 30: 
                                    if block_name['x'] < block_num['x']:
                                        raw_name = block_name['text']
                                        if len(raw_name) < 2: continue
                                        if raw_name.lower() in ignore_words: continue
                                        if raw_name.replace(',','').isdigit(): continue
                                        
                                        best_name = raw_name
                                        found_name_index = j
                                        break
                            
                            if best_name:
                                damage_val = int(clean_num)
                                used_blocks.append(i)
                                used_blocks.append(found_name_index)
                                
                                # ป้องกัน Error ถ้า list ว่างเปล่า หรือหาไม่เจอ
                                match_result = None
                                if existing_names:
                                    match_result = process.extractOne(best_name, existing_names)
                                
                                if match_result:
                                    match_name, score = match_result
                                    if score >= 70: 
                                        all_match_log.append({"name": match_name, "damage": damage_val})
                                    else:
                                        if best_name.lower() != st.session_state.guild_name.lower():
                                            new_candidates.append({"name": best_name, "damage": damage_val})
                                else:
                                    # ถ้าไม่มีรายชื่อในระบบเลย ก็ถือเป็นคนใหม่ทันที
                                    if best_name.lower() != st.session_state.guild_name.lower():
                                        new_candidates.append({"name": best_name, "damage": damage_val})

                except Exception as e:
                    st.error(f"Image {idx+1} Error: {e}")
                
                progress_bar.progress((idx + 1) / len(uploaded_images))

            status_text.text("✅ Finished!")
            progress_bar.empty()

            count_update = 0
            if all_match_log:
                update_dict = {item['name']: item['damage'] for item in all_match_log}
                for r_idx, row in st.session_state.main_df.iterrows():
                    name_key = str(row["ชื่อสมาชิก"])
                    if name_key in update_dict:
                        st.session_state.main_df.at[r_idx, target_boss] = update_dict[name_key]
                        count_update += 1
            
            unique_candidates = {}
            for item in new_candidates:
                unique_candidates[item['name']] = item['damage']
            
            st.session_state.pending_new_members = []
            for name, dmg in unique_candidates.items():
                st.session_state.pending_new_members.append({
                    "ชื่อที่อ่านได้": name,
                    "ดาเมจ": dmg,
                    "จัดการ": "++ สร้างสมาชิกใหม่ ++"
                })

            st.success(f"🎉 อัปเดตสมาชิกเดิม {count_update} คน (ลงในช่อง: {target_boss})")
            if len(st.session_state.pending_new_members) > 0:
                st.warning(f"⚠️ พบรายชื่อใหม่ {len(st.session_state.pending_new_members)} คน กรุณาตรวจสอบด้านล่าง")

    if len(st.session_state.pending_new_members) > 0:
        st.divider()
        st.subheader(f"👤 ตรวจสอบรายชื่อใหม่ (บอส: {st.session_state.scan_target_boss})")
        
        existing_options = sorted([str(x) for x in st.session_state.main_df["ชื่อสมาชิก"].tolist() if str(x).strip() != ""])
        dropdown_options = ["++ สร้างสมาชิกใหม่ ++"] + existing_options
        df_pending = pd.DataFrame(st.session_state.pending_new_members)
        
        edited_pending = st.data_editor(
            df_pending,
            column_config={
                "ชื่อที่อ่านได้": st.column_config.TextColumn("ชื่ออ่านได้", disabled=True),
                "ดาเมจ": st.column_config.NumberColumn("ดาเมจ", format="%d"),
                "จัดการ": st.column_config.SelectboxColumn("Action", options=dropdown_options, width="large", required=True)
            },
            use_container_width=True,
            num_rows="dynamic",
            key="pending_editor_full"
        )

        c1, c2 = st.columns([1, 4])
        with c1:
            if st.button("✅ ยืนยันข้อมูล", type="primary"):
                confirmed_boss = st.session_state.scan_target_boss 
                count_added = 0
                count_mapped = 0
                new_rows_to_add = []
                
                for _, row in edited_pending.iterrows():
                    action = row["จัดการ"]
                    dmg_val = row["ดาเมจ"]
                    read_name = row["ชื่อที่อ่านได้"]

                    if action == "++ สร้างสมาชิกใหม่ ++":
                        new_row = {"ชื่อสมาชิก": read_name}
                        for d in days_cols:
                            new_row[d] = dmg_val if d == confirmed_boss else 0
                        new_rows_to_add.append(new_row)
                        count_added += 1
                    else:
                        target_name = action
                        idx_list = st.session_state.main_df.index[st.session_state.main_df['ชื่อสมาชิก'].astype(str) == str(target_name)].tolist()
                        if idx_list:
                            st.session_state.main_df.at[idx_list[0], confirmed_boss] = dmg_val
                            count_mapped += 1

                if new_rows_to_add:
                    st.session_state.main_df = pd.concat([st.session_state.main_df, pd.DataFrame(new_rows_to_add)], ignore_index=True)

                st.session_state.pending_new_members = []
                st.toast(f"จัดการเรียบร้อย (New: {count_added}, Mapped: {count_mapped})", icon="✅")
                st.success("✅ อัปเดตตารางแล้ว! อย่าลืมกลับไปหน้า Dashboard เพื่อกด Save ลง Google Sheet")
                st.rerun()
                
        with c2:
            if st.button("🗑️ ทิ้งรายการนี้"):
                st.session_state.pending_new_members = []
                st.rerun()

    st.divider()
    st.subheader("📝 Preview ข้อมูล (รอ Save)")
    st.dataframe(st.session_state.main_df, use_container_width=True, height=200)

# ==========================================
# 7. MAIN EXECUTION
# ==========================================
tab1, tab2 = st.tabs(["📊 Dashboard & Google Sheet", "🤖 OCR Auto-Filler"])

with tab1:
    render_dashboard_tab()

with tab2:
    render_ocr_tab()
