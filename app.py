"""
MOHOLE - Dashboard Controllo Ore Docenti
Versione 2.0 - Google Sheets Integration
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, time
from io import BytesIO
import gspread
from google.oauth2.service_account import Credentials

# ========== CONFIGURAZIONE ==========
st.set_page_config(
    page_title="Mohole - Controllo Ore Docenti",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========== GOOGLE SHEETS CONNECTION ==========

@st.cache_resource
def get_google_client():
    """Connessione a Google Sheets con credenziali da Streamlit Secrets"""
    scope = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly"
    ]
    
    # Credenziali da st.secrets (configurate su Streamlit Cloud)
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
    return gspread.authorize(creds)

@st.cache_data(ttl=300)  # Cache per 5 minuti
def load_google_sheet(sheet_url, worksheet_name="Foglio1"):
    """Carica dati da Google Sheets"""
    try:
        client = get_google_client()
        sheet = client.open_by_url(sheet_url)
        worksheet = sheet.worksheet(worksheet_name)
        data = worksheet.get_all_records()
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"❌ Errore connessione Google Sheets: {str(e)}")
        return None

# ========== FUNZIONI DI UTILITÀ (stesse di prima) ==========

def parse_time(x):
    """Converte vari formati orari in time object"""
    if pd.isna(x):
        return None
    s = str(x).strip().replace('.', ':')
    if s == '':
        return None
    
    for fmt in ("%H:%M", "%H.%M", "%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).time()
        except:
            pass
    
    try:
        f = float(s)
        if 0 <= f < 2:
            minutes = f * 24 * 60
            h = int(minutes // 60) % 24
            m = int(round(minutes % 60))
            return time(h, m)
    except:
        pass
    
    return None

def normalize_dataframe(df):
    """Normalizza il DataFrame per l'analisi"""
    df.columns = [str(c).strip() for c in df.columns]
    
    required_cols = ['DATA LEZIONE', 'TOTALE_ORE', 'ORA_INIZIO', 'ORA_FINE', 'SEDE', 'Codice Fiscale']
    missing = [c for c in required_cols if c not in df.columns]
    
    if missing:
        st.error(f"⚠️ Colonne mancanti: {', '.join(missing)}")
        return None
    
    df['_date'] = pd.to_datetime(df['DATA LEZIONE'], dayfirst=True, errors='coerce').dt.normalize()
    df['_start_time'] = df['ORA_INIZIO'].apply(parse_time)
    df['_end_time'] = df['ORA_FINE'].apply(parse_time)
    
    df['_start_dt'] = pd.Series(pd.NaT, index=df.index, dtype='datetime64[ns]')
    df['_end_dt'] = pd.Series(pd.NaT, index=df.index, dtype='datetime64[ns]')
    
    for i in df.index:
        if isinstance(df.loc[i, '_start_time'], time) and isinstance(df.loc[i, '_end_time'], time) and pd.notna(df.loc[i, '_date']):
            df.loc[i, '_start_dt'] = pd.Timestamp.combine(df.loc[i, '_date'].date(), df.loc[i, '_start_time'])
            df.loc[i, '_end_dt'] = pd.Timestamp.combine(df.loc[i, '_date'].date(), df.loc[i, '_end_time'])
    
    overnight = df['_end_dt'].notna() & df['_start_dt'].notna() & (df['_end_dt'] < df['_start_dt'])
    df.loc[overnight, '_end_dt'] = df.loc[overnight, '_end_dt'] + pd.Timedelta(days=1)
    
    df['_cf_norm'] = df['Codice Fiscale'].astype(str).str.strip().str.upper()
    df['_computed_hours'] = ((df['_end_dt'] - df['_start_dt']) / pd.Timedelta(hours=1)).round(2)
    df['_declared_hours'] = pd.to_numeric(df['TOTALE_ORE'], errors='coerce').round(2)
    
    return df

# ========== CONTROLLI (stessi di prima) ==========

def check_hours(df, tolerance=0.02):
    """Controlla coerenza TOTALE_ORE vs differenza oraria"""
    mismatch_mask = (
        df['_computed_hours'].notna() & 
        df['_declared_hours'].notna() & 
        (df['_computed_hours'] - df['_declared_hours']).abs() > tolerance
    )
    
    errors = df[mismatch_mask].copy()
    errors['Diff (ore)'] = (errors['_computed_hours'] - errors['_declared_hours']).round(2)
    
    return errors[['DATA LEZIONE', 'ORA_INIZIO', 'ORA_FINE', 'TOTALE_ORE', '_computed_hours', 'Diff (ore)', 'Codice Fiscale']]

def check_duplicates(df):
    """Trova record identici su A-F"""
    df_work = df.copy()
    df_work['_start_str'] = df_work['_start_time'].apply(lambda x: x.strftime('%H:%M') if isinstance(x, time) else '')
    df_work['_end_str'] = df_work['_end_time'].apply(lambda x: x.strftime('%H:%M') if isinstance(x, time) else '')
    
    key_cols_str = ['_date', '_start_str', '_end_str', 'SEDE', '_cf_norm']
    df_work['_key'] = df_work[key_cols_str].astype(str).agg('|'.join, axis=1)
    duplicates_mask = df_work.duplicated(subset='_key', keep=False)
    
    dups = df_work[duplicates_mask].sort_values(key_cols_str)
    
    if dups.empty:
        return pd.DataFrame()
    
    return dups[['DATA LEZIONE', 'ORA_INIZIO', 'ORA_FINE', 'TOTALE_ORE', 'SEDE', 'Codice Fiscale', 'Materia']]

def check_overlaps(df):
    """Trova sovrapposizioni orarie per (data, CF)"""
    work = df[df['_start_dt'].notna() & df['_end_dt'].notna() & (df['_cf_norm'] != '')].copy()
    work['_date_str'] = work['_date'].dt.strftime('%Y-%m-%d')
    work['_row'] = range(len(work))
    
    overlaps = []
    
    for (date_key, cf), g in work.groupby(['_date_str', '_cf_norm']):
        g = g.sort_values('_start_dt')
        active = []
        
        for _, r in g.iterrows():
            active = [a for a in active if a[0] > r['_start_dt']]
            
            for end_time, prev_idx, start_time in active:
                overlaps.append({
                    'DATA LEZIONE': date_key,
                    'Codice Fiscale': cf,
                    'Ora inizio 1': start_time.strftime('%H:%M'),
                    'Ora fine 1': end_time.strftime('%H:%M'),
                    'Ora inizio 2': r['_start_dt'].strftime('%H:%M'),
                    'Ora fine 2': r['_end_dt'].strftime('%H:%M')
                })
            
            active.append((r['_end_dt'], r['_row'], r['_start_dt']))
    
    return pd.DataFrame(overlaps)

def to_excel(errors_df, duplicates_df, overlaps_df):
    """Genera file Excel con 3 fogli"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        errors_df.to_excel(writer, sheet_name='Errori Ore', index=False)
        duplicates_df.to_excel(writer, sheet_name='Duplicati', index=False)
        overlaps_df.to_excel(writer, sheet_name='Sovrapposizioni', index=False)
    
    output.seek(0)
    return output

# ========== INTERFACCIA STREAMLIT ==========

def main():
    st.title("📊 Mohole - Dashboard Controllo Ore Docenti")
    st.markdown("**Connessione Live a Google Sheets** - Aggiornamento automatico")
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Configurazione")
        
        # URL Google Sheets
        default_url = st.secrets.get("sheet_url", "")
        sheet_url = st.text_input(
            "🔗 URL Google Sheets",
            value=default_url,
            help="Incolla l'URL del tuo Google Sheets"
        )
        
        worksheet_name = st.text_input(
            "📄 Nome Foglio",
            value="Foglio1",
            help="Nome del foglio da analizzare"
        )
        
        tolerance = st.slider(
            "Tolleranza errori ore (minuti)",
            min_value=0,
            max_value=5,
            value=1
        ) / 60.0
        
        # Pulsante refresh manuale
        if st.button("🔄 Aggiorna Dati", type="primary"):
            st.cache_data.clear()
            st.rerun()
        
        st.markdown("---")
        st.markdown("### ℹ️ Info")
        st.info("I dati vengono aggiornati automaticamente ogni 5 minuti o tramite pulsante Aggiorna")
    
    if not sheet_url:
        st.warning("👈 Inserisci l'URL del Google Sheets nella sidebar")
        st.stop()
    
    # Carica dati
    with st.spinner("📡 Connessione a Google Sheets..."):
        df_raw = load_google_sheet(sheet_url, worksheet_name)
    
    if df_raw is None or df_raw.empty:
        st.error("❌ Impossibile caricare dati o foglio vuoto")
        st.stop()
    
    df = normalize_dataframe(df_raw)
    
    if df is None:
        st.stop()
    
    # Mostra ultimo aggiornamento
    last_update = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    st.success(f"✅ Dati caricati: {len(df)} righe | Ultimo aggiornamento: {last_update}")
    
    # Run checks
    with st.spinner("🔍 Esecuzione controlli..."):
        errors_df = check_hours(df, tolerance)
        duplicates_df = check_duplicates(df)
        overlaps_df = check_overlaps(df)
    
    # KPI Summary
    st.markdown("---")
    st.header("📈 Riepilogo Controlli")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📋 Record Totali", len(df))
    
    with col2:
        delta_color = "off" if len(errors_df) == 0 else "inverse"
        st.metric("❌ Errori Ore", len(errors_df), delta_color=delta_color)
    
    with col3:
        delta_color = "off" if len(duplicates_df) == 0 else "inverse"
        st.metric("🔄 Duplicati", len(duplicates_df), delta_color=delta_color)
    
    with col4:
        delta_color = "off" if len(overlaps_df) == 0 else "inverse"
        st.metric("⚠️ Sovrapposizioni", len(overlaps_df), delta_color=delta_color)
    
    # Tabs for details
    st.markdown("---")
    tab1, tab2, tab3 = st.tabs(["❌ Errori Ore", "🔄 Duplicati", "⚠️ Sovrapposizioni"])
    
    with tab1:
        st.subheader("Errori TOTALE_ORE vs Differenza Oraria")
        if errors_df.empty:
            st.success("✅ Nessun errore trovato!")
        else:
            st.warning(f"⚠️ Trovati {len(errors_df)} errori")
            st.dataframe(errors_df, use_container_width=True)
    
    with tab2:
        st.subheader("Record Duplicati")
        if duplicates_df.empty:
            st.success("✅ Nessun duplicato trovato!")
        else:
            st.warning(f"⚠️ Trovati {len(duplicates_df)} record duplicati")
            st.dataframe(duplicates_df, use_container_width=True)
    
    with tab3:
        st.subheader("Sovrapposizioni Orarie")
        if overlaps_df.empty:
            st.success("✅ Nessuna sovrapposizione trovata!")
        else:
            st.warning(f"⚠️ Trovate {len(overlaps_df)} sovrapposizioni")
            st.dataframe(overlaps_df, use_container_width=True)
    
    # Export button
    if not errors_df.empty or not duplicates_df.empty or not overlaps_df.empty:
        st.markdown("---")
        excel_file = to_excel(errors_df, duplicates_df, overlaps_df)
        
        st.download_button(
            label="📥 Scarica Report Completo (Excel)",
            data=excel_file,
            file_name=f"report_controllo_ore_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

if __name__ == "__main__":
    main()
