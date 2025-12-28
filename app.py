import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import json
import io
import zipfile
import easyocr
from thefuzz import process
from PIL import Image
from datetime import datetime
import matplotlib.font_manager as fm
import os

# ==========================================
# 1. GLOBAL SETUP & CONFIG
# ==========================================
st.set_page_config(page_title="MeAndBro Guild All-in-One", layout="wide")

# ตั้งค่า Font กราฟ
# ==========================================
# [แก้ใหม่] โหลดฟอนต์ภาษาไทยจากไฟล์
# ==========================================
font_file_name = "NotoSansThai-Regular.ttf" 

if os.path.exists(font_file_name):
    # เพิ่มฟอนต์เข้าไปในระบบของ Matplotlib
    fm.fontManager.addfont(font_file_name)
    # ดึงชื่อฟอนต์จริงๆ ออกมา
    prop = fm.FontProperties(fname=font_file_name)
    plt.rcParams['font.family'] = prop.get_name()
else:
    # ถ้าหาไฟล์ไม่เจอ ให้ใช้ฟอนต์สำรอง
    plt.rcParams['font.family'] = 'sans-serif'
    st.warning(f"⚠️ ไม่พบไฟล์ฟอนต์ {font_file_name} ภาษาไทยอาจแสดงผลไม่ถูกต้อง")

# โหลด EasyOCR
@st.cache_resource
def load_reader():
    return easyocr.Reader(['th', 'en'])

reader = load_reader()

# ==========================================
# 2. SHARED SESSION STATE
# ==========================================
if 'main_df' not in st.session_state:
    st.session_state.main_df = pd.DataFrame(
        [{"ชื่อสมาชิก": "ตัวอย่างสมาชิก", "ลูดี้": 0.0, "ไอลีน": 0.0, "ราเชล": 0.0, "เดลโลน": 0.0, "เจฟ": 0.0, "สไปร์ค": 0.0, "คริส": 0.0}],
    )
if 'prev_df' not in st.session_state:
    st.session_state.prev_df = None
if 'guild_name' not in st.session_state:
    st.session_state.guild_name = "MeAndBro"

if 'pending_new_members' not in st.session_state: st.session_state.pending_new_members = []
if 'scan_target_boss' not in st.session_state: st.session_state.scan_target_boss = None
if 'table_refresh_key' not in st.session_state: st.session_state.table_refresh_key = 0
if 'json_data_ocr' not in st.session_state: st.session_state.json_data_ocr = None

days_cols = ["ลูดี้", "ไอลีน", "ราเชล", "เดลโลน", "เจฟ", "สไปร์ค", "คริส"]

# ==========================================
# 3. HELPER FUNCTIONS
# ==========================================
def load_json_data(uploaded_file):
    try:
        data = json.load(uploaded_file)
        if "members" in data:
            new_rows = []
            for m in data["members"]:
                name = m.get("name", "Unknown") or "Unknown"
                row = {"ชื่อสมาชิก": str(name)}
                dmgs = m.get("damages", [0]*7)
                for i, label in enumerate(days_cols):
                    val = dmgs[i] if i < len(dmgs) else 0
                    try: row[label] = float(str(val).replace(',', ''))
                    except: row[label] = 0.0
                new_rows.append(row)
            return pd.DataFrame(new_rows), data.get("guild_name", "MeAndBro")
    except Exception as e:
        st.error(f"Error reading JSON: {e}")
        return None, None

# ==========================================
# 4. TAB 1: DASHBOARD & MANAGER
# ==========================================
def render_dashboard_tab():
    st.header("📊 Guild Damage Manager & Analytics")

    # --- Upload Logic ---
    def process_current_upload():
        if st.session_state.json_upload is not None:
            df, g_name = load_json_data(st.session_state.json_upload)
            if df is not None:
                st.session_state.main_df = df
                st.session_state.guild_name = g_name
                st.toast("✅ โหลดข้อมูลสำเร็จ!", icon="📂")

    def process_prev_upload():
        if st.session_state.prev_upload is not None:
            df, _ = load_json_data(st.session_state.prev_upload)
            if df is not None:
                st.session_state.prev_df = df

    col_h1, col_h2 = st.columns([2, 1])
    with col_h1:
        st.session_state.guild_name = st.text_input("ชื่อกิลด์:", st.session_state.guild_name)
    with col_h2:
        st.file_uploader("📂 Import ข้อมูลปัจจุบัน (JSON)", type=["json"], key="json_upload", on_change=process_current_upload)

    # --- Growth Comparison ---
    with st.expander("📈 เปิดโหมดเปรียบเทียบพัฒนาการ (Import ข้อมูลสัปดาห์ก่อน)"):
        st.file_uploader("📂 Import ข้อมูลสัปดาห์ที่แล้ว (JSON)", type=["json"], key="prev_upload", on_change=process_prev_upload)
        if st.session_state.prev_df is not None:
            st.info("✅ โหลดข้อมูลสัปดาห์ก่อนเรียบร้อย!")

    st.divider()

    # --- Data Checking ---
    df_check = st.session_state.main_df.copy()
    df_check["ชื่อสมาชิก"] = df_check["ชื่อสมาชิก"].fillna("ไม่ระบุชื่อ").astype(str)
    for col in days_cols: df_check[col] = pd.to_numeric(df_check[col], errors='coerce').fillna(0)

    zero_count = (df_check[days_cols] == 0).sum().sum()
    if zero_count > 0:
        st.warning(f"⚠️ พบช่อง 0 Damage: {zero_count} จุด")
    else:
        st.success("✅ ลงดาเมจครบถ้วน!")

    # --- Filtering & Editing ---
    col_t1, col_t2 = st.columns([3, 1])
    with col_t1:
        search_term = st.text_input("🔍 ค้นหาชื่อสมาชิก:", placeholder="พิมพ์ชื่อ...")
    with col_t2:
        st.write("") 
        st.write("") 
        filter_zero = st.checkbox("⚠️ กรองคนมี 0")

    display_df = st.session_state.main_df.copy()
    for col in days_cols: display_df[col] = pd.to_numeric(display_df[col], errors='coerce').fillna(0.0)

    if search_term:
        display_df = display_df[display_df['ชื่อสมาชิก'].astype(str).str.contains(search_term, case=False, na=False)]
    if filter_zero:
        display_df = display_df[display_df[days_cols].eq(0).any(axis=1)]

    st.subheader("จัดการดาเมจสมาชิก")
    column_config = {"ชื่อสมาชิก": st.column_config.TextColumn("ชื่อสมาชิก", width="medium")}
    for col in days_cols: column_config[col] = st.column_config.NumberColumn(col, min_value=0, format="%d")

    edited_df = st.data_editor(
        display_df,
        column_config=column_config,
        num_rows="dynamic",
        use_container_width=True,
        key="editor_main",
        height=(len(display_df) + 2) * 35 + 40
    )

    if search_term or filter_zero:
        st.session_state.main_df.loc[edited_df.index] = edited_df
    else:
        st.session_state.main_df = edited_df

    # --- Analytics & Export ---
    df_growth_export = None
    if st.session_state.prev_df is not None:
        st.divider()
        st.subheader("🚀 วิเคราะห์พัฒนาการ (Growth Comparison)")
        current_calc = st.session_state.main_df.copy()
        prev_calc = st.session_state.prev_df.copy()
        for d in days_cols:
            current_calc[d] = pd.to_numeric(current_calc[d], errors='coerce').fillna(0)
            prev_calc[d] = pd.to_numeric(prev_calc[d], errors='coerce').fillna(0)
        
        current_calc["Total"] = current_calc[days_cols].sum(axis=1)
        prev_calc["Total"] = prev_calc[days_cols].sum(axis=1)
        
        growth_df = pd.merge(current_calc[["ชื่อสมาชิก", "Total"]], prev_calc[["ชื่อสมาชิก", "Total"]], on="ชื่อสมาชิก", how="left", suffixes=("_Current", "_Prev"))
        growth_df["Total_Prev"] = growth_df["Total_Prev"].fillna(0)
        growth_df["Diff"] = growth_df["Total_Current"] - growth_df["Total_Prev"]
        
        st.dataframe(
            growth_df.sort_values("Diff", ascending=False).style.format({"Total_Current": "{:,.0f}", "Total_Prev": "{:,.0f}", "Diff": "{:+,.0f}"}),
            use_container_width=True,
            column_config={"Diff": st.column_config.NumberColumn("เปลี่ยนแปลง", format="%d")},
            hide_index=True
        )
        df_growth_export = growth_df

    st.divider()
    if st.button("📊 สรุปผลและสร้างรายงาน (Export ZIP)"):
        df_exp = st.session_state.main_df.copy()
        df_exp = df_exp[df_exp["ชื่อสมาชิก"].notna() & (df_exp["ชื่อสมาชิก"] != "")]
        
        if df_exp.empty:
            st.error("ไม่มีข้อมูล")
        else:
            for d in days_cols: df_exp[d] = pd.to_numeric(df_exp[d], errors='coerce').fillna(0)
            df_exp["Total Damage"] = df_exp[days_cols].sum(axis=1)
            
            if df_growth_export is not None:
                 df_exp = pd.merge(df_exp, df_growth_export[["ชื่อสมาชิก", "Total_Prev", "Diff"]], on="ชื่อสมาชิก", how="left")
                 df_exp.rename(columns={"Total_Prev": "Previous Week", "Diff": "Growth"}, inplace=True)

            # ================= GRAPH 1: TOTAL DAMAGE RANKING =================
            df_sorted = df_exp.sort_values("Total Damage", ascending=True)
            fig1, ax1 = plt.subplots(figsize=(10, max(5, len(df_exp) * 0.45)))
            bars = ax1.barh(df_sorted["ชื่อสมาชิก"].astype(str), df_sorted["Total Damage"], color='#2196F3', zorder=3)
            
            ax1.set_title(f"Total Damage Ranking: {st.session_state.guild_name}", fontsize=14, fontweight='bold')
            ax1.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{x/1e6:.1f}M'))
            
            # เส้นปะแบ่งดาเมจ (แนวตั้ง)
            ax1.grid(axis='x', linestyle='--', alpha=0.6, color='gray', zorder=0)
            # เส้นปะแบ่งคน (แนวนอน)
            for y in range(len(df_sorted) + 1):
                ax1.axhline(y=y - 0.5, color='lightgray', linestyle='--', linewidth=0.8, zorder=0)

            # ใส่ตัวเลขดาเมจต่อท้ายแท่ง
            for bar in bars:
                width = bar.get_width()
                ax1.text(width * 1.01, bar.get_y() + bar.get_height()/2, 
                         f' {width:,.0f}',
                         va='center', ha='left', fontsize=10, fontweight='bold', color='#333333')
            
            xmax = df_sorted["Total Damage"].max()
            ax1.set_xlim(0, xmax * 1.15) 
            plt.tight_layout()

            # ================= GRAPH 2: WEEKLY BREAKDOWN =================
            fig2, ax2 = plt.subplots(figsize=(16, 8))
            x = np.arange(len(df_exp))
            width = 0.1
            colors = ['#FF5733', '#FFC300', '#DAF7A6', '#33FF57', '#3380FF', '#FF33A8', '#8D33FF']
            df_desc = df_exp.sort_values("Total Damage", ascending=False)
            
            for i, day in enumerate(days_cols):
                ax2.bar(x + (width * i), df_desc[day], width, label=day, color=colors[i], zorder=3)
            
            # เส้นปะแบ่งดาเมจ (แนวนอน)
            ax2.grid(axis='y', linestyle='--', alpha=0.6, color='gray', zorder=0)
            
            # เส้นปะแบ่งคน (แนวตั้ง)
            for i in range(len(df_exp) - 1): 
                ax2.axvline(x=i + 0.85, color='gray', linestyle='--', linewidth=0.8, alpha=0.5, zorder=2)

            # ++++++++++++++++++ เพิ่มเส้นเกณฑ์ 1M และ 0.5M ++++++++++++++++++
            ax2.axhline(y=1000000, color='red', linestyle='--', linewidth=1.5, label="1M กายภาพ", zorder=4)
            ax2.axhline(y=500000, color='#00BFFF', linestyle='--', linewidth=1.5, label="0.5M เวทย์", zorder=4) # สีฟ้า (Deep Sky Blue)
                
            ax2.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{x/1e6:.1f}M'))
            ax2.set_xticks(x + width * 3)
            ax2.set_xticklabels(df_desc["ชื่อสมาชิก"].astype(str), rotation=45, ha='right')
            ax2.legend()
            plt.tight_layout()

            # ZIP Creation
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, "a", zipfile.ZIP_DEFLATED, False) as z:
                excel_buf = io.BytesIO()
                df_exp.to_excel(excel_buf, index=False)
                z.writestr("Guild_Data.xlsx", excel_buf.getvalue())
                img1 = io.BytesIO(); fig1.savefig(img1, format='png'); z.writestr("Rank_Graph.png", img1.getvalue())
                img2 = io.BytesIO(); fig2.savefig(img2, format='png'); z.writestr("Weekly_Graph.png", img2.getvalue())
                
                json_out = {"guild_name": st.session_state.guild_name, "members": [{"name": str(r["ชื่อสมาชิก"]), "damages": [r[d] for d in days_cols]} for _, r in df_exp.iterrows()]}
                z.writestr("backup.json", json.dumps(json_out, ensure_ascii=False, indent=4))
            
            st.write("### 🖼️ ตัวอย่างกราฟ (เวอร์ชันอัปเดต):")
            st.pyplot(fig1)
            st.pyplot(fig2)
            st.success("สร้างรายงานเรียบร้อย!")
            st.download_button("💾 Download ZIP", data=zip_buf.getvalue(), file_name="Report.zip", mime="application/zip")

# ==========================================
# 5. TAB 2: OCR AUTO-FILLER
# ==========================================
def render_ocr_tab():
    st.header("🤖 OCR Auto-Filler")
    st.caption("อัปโหลดรูป -> สแกน -> ข้อมูลจะถูกบันทึกลงตารางหลักในแท็บ Dashboard โดยอัตโนมัติ")

    def on_upload_ocr_change():
        uploaded = st.session_state.uploaded_file_key_ocr
        if uploaded is not None:
            df, g_name = load_json_data(uploaded)
            if df is not None:
                st.session_state.main_df = df
                st.session_state.guild_name = g_name
                st.session_state.table_refresh_key += 1
                st.toast("✅ โหลดไฟล์เรียบร้อย (อัปเดตตารางหลักแล้ว)", icon="📂")

    col1, col2 = st.columns([1, 2])
    with col1:
        st.file_uploader(
            "1. เลือกไฟล์ JSON (ถ้ายังไม่ได้โหลดในหน้าแรก)", 
            type=['json'], 
            key="uploaded_file_key_ocr",
            on_change=on_upload_ocr_change
        )
        st.info(f"📁 สมาชิกในระบบ: {len(st.session_state.main_df)} คน")

    # --- Image Scanning ---
    selected_boss = "ลูดี้"
    with col2:
        selected_boss = st.selectbox("2. เลือกบอสที่จะสแกน", days_cols)
        uploaded_images = st.file_uploader("3. อัปโหลดรูป (รองรับหลายไฟล์)", type=['png', 'jpg', 'jpeg'], accept_multiple_files=True)

    if uploaded_images:
        st.divider()
        ignore_words = ["rank", "score", "damage", "total", "guild", "boss", "level", "lv", "name", "point", "reward", "exp"]
        ignore_words.append(st.session_state.guild_name.lower())

        if st.button("🚀 เริ่มอ่าน (Scan Images)", type="primary"):
            st.session_state.scan_target_boss = selected_boss 
            target_boss = st.session_state.scan_target_boss
            
            all_match_log = [] 
            new_candidates = [] 
            existing_names = st.session_state.main_df["ชื่อสมาชิก"].tolist()
            
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
                                
                                match_name, score = process.extractOne(best_name, existing_names)
                                if score >= 70: 
                                    all_match_log.append({"name": match_name, "damage": damage_val})
                                else:
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
                    if row["ชื่อสมาชิก"] in update_dict:
                        st.session_state.main_df.at[r_idx, target_boss] = update_dict[row["ชื่อสมาชิก"]]
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

            st.session_state.table_refresh_key += 1
            st.success(f"🎉 อัปเดตสมาชิกเดิมทันที {count_update} คน (ลงในช่อง: {target_boss})")
            if len(st.session_state.pending_new_members) > 0:
                st.warning(f"⚠️ พบรายชื่อใหม่/ไม่ตรงกัน {len(st.session_state.pending_new_members)} คน")

    if len(st.session_state.pending_new_members) > 0:
        st.divider()
        st.subheader(f"👤 ตรวจสอบรายชื่อ (บอส: {st.session_state.scan_target_boss})")
        existing_options = sorted(st.session_state.main_df["ชื่อสมาชิก"].tolist())
        dropdown_options = ["++ สร้างสมาชิกใหม่ ++"] + existing_options
        df_pending = pd.DataFrame(st.session_state.pending_new_members)
        
        edited_pending = st.data_editor(
            df_pending,
            column_config={
                "ชื่อที่อ่านได้": st.column_config.TextColumn("ชื่ออ่านได้", disabled=False),
                "ดาเมจ": st.column_config.NumberColumn("ดาเมจ", format="%d"),
                "จัดการ": st.column_config.SelectboxColumn("Action", options=dropdown_options, width="large", required=True)
            },
            use_container_width=True,
            num_rows="dynamic",
            key="pending_editor_map_stable"
        )

        col_conf1, col_conf2 = st.columns([1, 4])
        with col_conf1:
            if st.button("✅ ยืนยันการเปลี่ยนแปลง", type="primary"):
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
                        idx_list = st.session_state.main_df.index[st.session_state.main_df['ชื่อสมาชิก'] == target_name].tolist()
                        if idx_list:
                            st.session_state.main_df.at[idx_list[0], confirmed_boss] = dmg_val
                            count_mapped += 1

                if new_rows_to_add:
                    st.session_state.main_df = pd.concat([st.session_state.main_df, pd.DataFrame(new_rows_to_add)], ignore_index=True)

                st.session_state.table_refresh_key += 1 
                st.session_state.pending_new_members = []
                st.success(f"บันทึกสำเร็จ! (New: {count_added} | Mapped: {count_mapped})")
                st.rerun()
        
        with col_conf2:
            if st.button("🗑️ ทิ้งรายการนี้"):
                st.session_state.pending_new_members = []
                st.rerun()

    st.divider()
    st.subheader("📝 Preview ข้อมูลปัจจุบัน")
    st.dataframe(st.session_state.main_df, use_container_width=True, height=300)

# ==========================================
# 6. MAIN APP EXECUTION
# ==========================================

tab1, tab2 = st.tabs(["📊 Dashboard & Manual", "🤖 OCR Auto-Filler"])

with tab1:
    render_dashboard_tab()

with tab2:
    render_ocr_tab()