"""
MOHOLE - Dashboard Controllo Ore Docenti
Versione 2.2 - Report Migliorati
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, time
from io import BytesIO
from itertools import combinations

# ========== CONFIGURAZIONE ==========
st.set_page_config(
    page_title="Mohole - Controllo Ore Docenti",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========== FUNZIONI DI UTILITÀ ==========

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
    df['_original_row'] = df.index + 2  # Numero riga Excel (header + offset)
    
    return df

def check_hours(df, tolerance=0.02):
    """Controlla coerenza TOTALE_ORE vs differenza oraria"""
    mismatch_mask = (
        df['_computed_hours'].notna() & 
        df['_declared_hours'].notna() & 
        (df['_computed_hours'] - df['_declared_hours']).abs() > tolerance
    )
    
    errors = df[mismatch_mask].copy()
    errors['Diff (ore)'] = (errors['_computed_hours'] - errors['_declared_hours']).round(2)
    
    return errors[['_original_row', 'DATA LEZIONE', 'ORA_INIZIO', 'ORA_FINE', 'TOTALE_ORE', '_computed_hours', 'Diff (ore)', 'Codice Fiscale']].rename(columns={'_original_row': 'Riga Excel'})

def check_duplicates(df):
    """Trova record identici su A-F e genera report coppie"""
    df_work = df.copy()
    df_work['_start_str'] = df_work['_start_time'].apply(lambda x: x.strftime('%H:%M') if isinstance(x, time) else '')
    df_work['_end_str'] = df_work['_end_time'].apply(lambda x: x.strftime('%H:%M') if isinstance(x, time) else '')
    
    key_cols_str = ['_date', '_start_str', '_end_str', 'SEDE', '_cf_norm']
    df_work['_key'] = df_work[key_cols_str].astype(str).agg('|'.join, axis=1)
    
    # Trova tutti i duplicati
    duplicates_mask = df_work.duplicated(subset='_key', keep=False)
    dups = df_work[duplicates_mask].sort_values(key_cols_str)
    
    if dups.empty:
        return pd.DataFrame()
    
    # Genera coppie di duplicati
    duplicate_pairs = []
    
    for key_val, group in dups.groupby('_key'):
        if len(group) < 2:
            continue
        
        # Genera tutte le coppie
        rows = group.sort_values('_original_row')
        for (idx1, row1), (idx2, row2) in combinations(rows.iterrows(), 2):
            duplicate_pairs.append({
                'Riga X': int(row1['_original_row']),
                'Riga Y': int(row2['_original_row']),
                'Data Lezione': row1['_date'].strftime('%Y-%m-%d') if pd.notna(row1['_date']) else '',
                'Codice Fiscale': row1['_cf_norm'],
                'Ora Inizio': row1['_start_str'],
                'Ora Fine': row1['_end_str'],
                'Sede': row1['SEDE']
            })
    
    return pd.DataFrame(duplicate_pairs)

def check_overlaps(df):
    """Trova sovrapposizioni orarie per (data, CF) con info righe"""
    work = df[df['_start_dt'].notna() & df['_end_dt'].notna() & (df['_cf_norm'] != '')].copy()
    work['_date_str'] = work['_date'].dt.strftime('%Y-%m-%d')
    
    overlaps = []
    
    for (date_key, cf), g in work.groupby(['_date_str', '_cf_norm']):
        g = g.sort_values('_start_dt').reset_index(drop=True)
        
        # Confronta ogni coppia di lezioni
        for i in range(len(g)):
            for j in range(i + 1, len(g)):
                row_i = g.iloc[i]
                row_j = g.iloc[j]
                
                # Controlla sovrapposizione: inizio_j < fine_i
                if row_j['_start_dt'] < row_i['_end_dt']:
                    overlaps.append({
                        'Data Lezione': date_key,
                        'Codice Fiscale': cf,
                        'Riga X': int(row_i['_original_row']),
                        'Riga Y': int(row_j['_original_row']),
                        'Ora Inizio X': row_i['_start_dt'].strftime('%H:%M'),
                        'Ora Fine X': row_i['_end_dt'].strftime('%H:%M'),
                        'Ora Inizio Y': row_j['_start_dt'].strftime('%H:%M'),
                        'Ora Fine Y': row_j['_end_dt'].strftime('%H:%M')
                    })
    
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
    st.markdown("Sistema automatico di verifica qualità dati lezioni")
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Impostazioni")
        
        tolerance = st.slider(
            "Tolleranza errori ore (minuti)",
            min_value=0,
            max_value=5,
            value=1,
            help="Tolleranza accettabile per differenze TOTALE_ORE"
        ) / 60.0
        
        st.markdown("---")
        st.markdown("### 📖 Istruzioni")
        st.markdown("""
        1. Carica file Excel con dati lezioni
        2. Verifica i controlli automatici
        3. Scarica report errori se necessario
        
        **Colonne richieste:**
        - DATA LEZIONE
        - TOTALE_ORE
        - ORA_INIZIO
        - ORA_FINE
        - SEDE
        - Codice Fiscale
        """)
    
    # Upload file
    uploaded_file = st.file_uploader(
        "📁 Carica file Excel",
        type=['xlsx', 'xls'],
        help="Formato supportato: Excel (.xlsx, .xls)"
    )
    
    if not uploaded_file:
        st.info("👆 Carica un file Excel per iniziare l'analisi")
        st.stop()
    
    # Read file
    try:
        with st.spinner("📖 Lettura file..."):
            df_raw = pd.read_excel(uploaded_file)
            df = normalize_dataframe(df_raw)
        
        if df is None:
            st.stop()
        
        st.success(f"✅ File caricato: {len(df)} righe")
    
    except Exception as e:
        st.error(f"❌ Errore lettura file: {str(e)}")
        st.stop()
    
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
        st.metric("🔄 Coppie Duplicate", len(duplicates_df), delta_color=delta_color)
    
    with col4:
        delta_color = "off" if len(overlaps_df) == 0 else "inverse"
        st.metric("⚠️ Sovrapposizioni", len(overlaps_df), delta_color=delta_color)
    
    # Tabs for details
    st.markdown("---")
    tab1, tab2, tab3 = st.tabs(["❌ Errori Ore", "🔄 Duplicati", "⚠️ Sovrapposizioni"])
    
    with tab1:
        st.subheader("Errori TOTALE_ORE vs Differenza Oraria")
        if errors_df.empty:
            st.success("✅ Nessun errore trovato! Tutti i valori TOTALE_ORE sono coerenti.")
        else:
            st.warning(f"⚠️ Trovati {len(errors_df)} errori")
            st.dataframe(errors_df, use_container_width=True)
    
    with tab2:
        st.subheader("Coppie di Record Duplicati")
        if duplicates_df.empty:
            st.success("✅ Nessun duplicato trovato!")
        else:
            st.warning(f"⚠️ Trovate {len(duplicates_df)} coppie di duplicati")
            st.info("💡 Ogni coppia mostra due righe Excel identiche su Data, Orari, Sede e Codice Fiscale")
            st.dataframe(duplicates_df, use_container_width=True)
    
    with tab3:
        st.subheader("Sovrapposizioni Orarie (stessa data e CF)")
        if overlaps_df.empty:
            st.success("✅ Nessuna sovrapposizione trovata!")
        else:
            st.warning(f"⚠️ Trovate {len(overlaps_df)} sovrapposizioni")
            st.info("💡 Riga X e Riga Y indicano le righe Excel con orari sovrapposti per lo stesso docente nella stessa data")
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
