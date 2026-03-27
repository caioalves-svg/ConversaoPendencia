import pandas as pd
import streamlit as st

def normalizar_nf(valor):
    """Padroniza o formato da Nota Fiscal para string limpa."""
    if pd.isna(valor): return ""
    s = str(valor).strip()
    if s.lower() == 'nan': return ""
    if s.endswith('.0'): s = s.replace('.0', '')
    if ',' in s: s = s.split(',')[0]
    return s

def carregar_arquivo(uploaded_file):
    """Carrega arquivos CSV ou Excel lidando com diferentes encodings."""
    if uploaded_file.name.endswith('.csv'):
        try:
            return pd.read_csv(uploaded_file, encoding='utf-8')
        except:
            uploaded_file.seek(0)
            try:
                return pd.read_csv(uploaded_file, sep=';', encoding='latin1')
            except:
                uploaded_file.seek(0)
                return pd.read_csv(uploaded_file, sep=',', encoding='latin1')
    else:
        return pd.read_excel(uploaded_file)

def encontrar_coluna(df, palavras_chave):
    """Busca inteligente de colunas baseada em palavras-chave."""
    colunas_reais = df.columns
    # Busca exata
    for chave in palavras_chave:
        if chave in colunas_reais:
            return chave
    # Busca case-insensitive e stripped
    for chave in palavras_chave:
        for col_real in colunas_reais:
            if chave.upper() == col_real.upper().strip():
                return col_real
    return None
