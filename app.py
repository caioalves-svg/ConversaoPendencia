bimport streamlit as st
import pandas as pd
from datetime import datetime
import io
import traceback

# ==============================================================================
# CONFIGURAÇÃO VISUAL
# ==============================================================================
st.set_page_config(
    page_title="Gestão de Tratativas Logísticas",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #e0e0e0;
    }
</style>
""", unsafe_allow_html=True)

st.title("🚚 Painel de Automação Logística")
st.markdown("### Processamento e Cruzamento de Dados (Intelipost x Sysemp)")
st.markdown("---")

# ==============================================================================
# DICIONÁRIOS
# ==============================================================================
DICIONARIO_MARKETPLACE = {
    "ALIEXPRESS": "ALIEXPRESS", "AMAZON - EXTREMA": "AMAZON - EXTREMA",
    "AMAZON | ENGAGE LOG": "AMAZON | ENGAGE LOG", "AMERICANAS - EXTREMA": "AMERICANAS - EXTREMA",
    "B2W": "B2W", "CARREFOUR": "CARREFOUR", "CNOVA": "CNOVA", "CNOVA - EXTREMA": "CNOVA - EXTREMA",
    "FAST SHOP": "FAST SHOP", "MADEIRA MADEIRA": "MADEIRA MADEIRA", "MAGALU - EXTREMA": "MAGALU - EXTREMA",
    "MAGALU ELETRO": "MAGALU ELETRO", "MAGALU INFO": "MAGALU INFO", "MARTINS": "MARTINS",
    "MELI OUTLET": "MELI OUTLET", "MERCADO LIVRE": "MERCADO LIVRE",
    "MERCADO LIVRE - EXTREMA": "MERCADO LIVRE - EXTREMA", "shopee": "SHOPEE",
    "WEBCONTINENTAL": "WEBCONTINENTAL", "WAPSTORE - ENGAGE": "WAPSTORE - ENGAGE",
    "LEROY - EXTREMA": "LEROY - EXTREMA", "BRADESCO SHOP": "BRADESCO SHOP",
    "TIKTOK": "TIKTOK", "AMAZON DBA": "AMAZON DBA", "ZEMA": "ZEMA"
}

DICIONARIO_TRANSPORTADORA = {
    "Atual Cargas": "ATUAL", "Brasil Web Standard": "BRASIL WEB", "Favorita Transportes": "FAVORITA",
    "FrontLog": "FRONTLOG", "Generoso": "GENEROSO", "JadLog": "JADLOG", "Logan Express": "LOGAN",
    "MMA Cargas Expressas": "MMA", "Patrus": "PATRUS", "Reboucas ": "REBOUÇAS", "Rede Sul": "REDE SUL",
    "Rio Express Cargas": "RIO EXPRESS", "TJB": "TJB", "Total": "TOTAL", "Trilog Express": "TRILOG", "Via Pajucara": "PAJUÇARA"
}

DICIONARIO_OCORRENCIA = {
    "AGUARDANDO DADOS": "VERIFICAR", "(TOTAL) FALTA DE ARQUIVO": "VERIFICAR",
    "AGUARDANDO INSTRUÇÃO": "VERIFICAR", "ÁREA DE RISCO": "ÁREA DE RISCO",
    "ÁREA NÃO ATENDIDA": "ÁREA NÃO ATENDIDA", "AVERIGUAR FALHA NA ENTREGA": "VERIFICAR",
    "ARREPENDIMENTO": "BLOQUEADO PELO REMETENTE", "AUSENTE": "AUSENTE", "BUSCA": "EXTRAVIO",
    "CARGA DESCARTADA": "VERIFICAR", "AVARIA": "AVARIA", "CARGA ERRADA": "VERIFICAR",
    "CARGA ROUBADA": "ROUBO", "CARGA RECUSADA PELO DESTINATARIO": "RECUSADO",
    "CARTA DE CORREÇÃO": "VERIFICAR", "CLIENTE ALEGA FALTA DE MERCADORIA": "VERIFICAR",
    "DESTINATÁRIO DESCONHECID0": "DESTINATÁRIO DESCONHECIDO", "DESTINATÁRIO AUSENTE": "AUSENTE",
    "DEVOLUÇÃO INDEVIDA": "VERIFICAR", "DEVOLUÇÃO POR ATRASO": "VERIFICAR",
    "DESTINATÁRIO MUDOU-SE": "ENDEREÇO NÃO LOCALIZADO", "DUPLICIDADE": "VERIFICAR",
    "DESTINATÁRIO NÃO LOCALIZADO": "ENDEREÇO NÃO LOCALIZADO", "DIFICIL ACESSO": "ÁREA DE RISCO",
    "ENTREGUE E CANCELADO": "VERIFICAR", "ENDEREÇO INSUFICIENTE": "ENDEREÇO NÃO LOCALIZADO",
    "ERRO DE EXPEDIÇÃO": "VERIFICAR", "ESTABELECIMENTO FECHADO": "AUSENTE",
    "FURTO / ROUBO": "ROUBO", "EXTRAVIO CONFIRMADO": "EXTRAVIO", "ITEM FALTANTE": "AVARIA PARCIAL",
    "FALHA NA ENTREGA": "VERIFICAR", "NÃO ENTROU NA UNIDADE": "VERIFICAR",
    "Mercadoria retida/liberada por Fiscalização": "NOTA RETIDA", "PARADO NA FISCALIZACAO": "NOTA RETIDA",
    "PROBLEMA OPERACIONAL": "VERIFICAR", "SEM RASTREIO": "VERIFICAR",
    "RESGATE DE MERCADORIA SOLICITADA PELO CLIENTE": "RETIRADA NA UNIDADE",
    "ANÁLISE FISCAL": "NOTA RETIDA", "SOLICITAÇÃO DE ACAREAÇÃO": "EM PROCESSO DE INVESTIGAÇÃO",
    "VIA INTERDITADA": "VERIFICAR", "CORRECAO INFORMACAO DE EVENTO": "VERIFICAR",
    "ZONA RURAL": "VERIFICAR", "CARGA INCOMPLETA": "AVARIA PARCIAL"
}

# ==============================================================================
# FUNÇÕES DE SUPORTE
# ==============================================================================

def normalizar_nf(valor):
    if pd.isna(valor): return ""
    s = str(valor).strip()
    if s.lower() == 'nan': return ""
    if s.endswith('.0'): s = s.replace('.0', '')
    if ',' in s: s = s.split(',')[0]
    return s

def carregar_arquivo(uploaded_file):
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
    colunas_reais = df.columns
    for chave in palavras_chave:
        if chave in colunas_reais:
            return chave
    for chave in palavras_chave:
        for col_real in colunas_reais:
            if chave.upper() == col_real.upper().strip():
                return col_real
    return None

def carregar_base_tratativas(file_base):
    if file_base is None: return set()
    try:
        df_base = carregar_arquivo(file_base)
        col_nf_base = encontrar_coluna(df_base, ['Nota Fiscal', 'NF', 'Numero NF'])
        if col_nf_base:
            return set(df_base[col_nf_base].apply(normalizar_nf))
        return set()
    except:
        return set()

def tratar_sysemp(df):
    st.info("Processando Sysemp...", icon="⚙️")
    
    # === BUSCA INTELIGENTE DA COLUNA DE EMPRESA ===
    # Procura TODAS as colunas que parecem "Empresa"
    candidatas = [c for c in df.columns if 'EMPRESA' in c.upper()]
    
    coluna_id_final = None
    
    # Testa uma por uma: qual delas tem os números 16, 18, 19 ou 21?
    for col in candidatas:
        # Converte para número (se for texto, vira NaN)
        temp_series = pd.to_numeric(df[col], errors='coerce')
        # Verifica se existe algum dos nossos IDs alvo nesta coluna
        matches = temp_series.isin([16, 18, 19, 21]).sum()
        
        if matches > 0:
            coluna_id_final = col
            # st.success(f"Coluna de ID identificada automaticamente: {col}") # Debug visual
            break
            
    if not coluna_id_final:
        st.error("❌ ERRO CRÍTICO: Não encontrei nenhuma coluna com os códigos das empresas (16, 18, 19, 21).")
        st.write("Colunas analisadas:", candidatas)
        return pd.DataFrame()

    # Filtro usando a coluna vencedora
    df['temp_id'] = pd.to_numeric(df[coluna_id_final], errors='coerce')
    df_filtrado = df[df['temp_id'].isin([16, 18, 19, 21])].copy()
    
    if df_filtrado.empty:
        st.error("❌ O filtro retornou vazio mesmo após identificar a coluna. Verifique o arquivo.")
        st.stop()
    else:
        df = df_filtrado

    # 2. Busca Nota Fiscal
    col_nf = encontrar_coluna(df, ['Nota Fiscal', 'NF', 'Numero NF'])
    if not col_nf:
        st.error("❌ ERRO NO SYSEMP: Coluna 'Nota Fiscal' não encontrada.")
        return pd.DataFrame()
    
    df['Nota Fiscal'] = df[col_nf].apply(normalizar_nf)

    # 3. Busca Chave e Pedido
    col_chave = encontrar_coluna(df, ['Chave NFe', 'Chave NF', 'Chave'])
    
    col_pedido_final = None
    if 'Pedido Marketplace' in df.columns:
        col_pedido_final = 'Pedido Marketplace'
    else:
        for col in df.columns:
            if "PEDIDO" in col.upper() and "MARKETPLACE" in col.upper():
                col_pedido_final = col
                break
        if not col_pedido_final:
             col_pedido_final = encontrar_coluna(df, ['Pedido'])

    df_novo = pd.DataFrame()
    df_novo['Nota Fiscal'] = df['Nota Fiscal']

    # Preenche Chave
    if col_chave:
        df_novo['Chave NF_sys'] = df[col_chave].astype(str).str.replace('.0', '', regex=False).str.replace('nan', '', case=False).str.strip()
    else:
        df_novo['Chave NF_sys'] = "N/A"

    # Preenche Pedido
    if col_pedido_final:
        df_novo['Pedido_sys'] = df[col_pedido_final].astype(str).str.replace('.0', '', regex=False).str.replace('nan', '', case=False).str.strip()
    else:
        df_novo['Pedido_sys'] = "N/A"

    return df_novo

def tratar_intelipost(df):
    st.info("Processando Intelipost...", icon="⚙️")
    
    col_mkt = encontrar_coluna(df, ['Canal de Vendas', 'Marketplace'])
    col_micro = encontrar_coluna(df, ['MicroStatus', 'Ocorrência de Entrega', 'Status'])
    col_nf = encontrar_coluna(df, ['Nota Fiscal', 'NF', 'Pedido do Cliente']) 

    if col_mkt: df = df.rename(columns={col_mkt: 'Marketplace'})
    if col_micro: df = df.rename(columns={col_micro: 'Ocorrência de Entrega'})
    
    if col_nf and col_nf != 'Nota Fiscal':
        df = df.rename(columns={col_nf: 'Nota Fiscal'})
    
    if 'Nota Fiscal' not in df.columns:
        st.error("Erro Intelipost: Coluna Nota Fiscal não encontrada.")
        return pd.DataFrame()

    df['Nota Fiscal'] = df['Nota Fiscal'].apply(normalizar_nf)
    
    # Filtro de Status
    if 'Ocorrência de Entrega' in df.columns:
        df['Ocorrência de Entrega'] = df['Ocorrência de Entrega'].astype(str).str.upper()
        df = df[~df['Ocorrência de Entrega'].str.contains("ATRASO|INFORMATIVO", na=False)]
    
    return df

# ==============================================================================
# LÓGICA PRINCIPAL
# ==============================================================================

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 1. Intelipost")
    file_intelipost = st.file_uploader("Transações", type=["xlsx", "csv"], key="inteli")

with col2:
    st.markdown("### 2. Sysemp")
    file_sysemp = st.file_uploader("Manutenção NF", type=["xlsx", "csv"], key="sys")

with col3:
    st.markdown("### 3. Histórico")
    file_base = st.file_uploader("Opcional: Exclusão", type=["xlsx", "csv"], key="base")

if file_intelipost and file_sysemp:
    st.markdown("---")
    if st.button("🚀 INICIAR PROCESSAMENTO", type="primary", use_container_width=True):
        try:
            # 1. Carregamento
            df_inteli_raw = carregar_arquivo(file_intelipost)
            df_sysemp_raw = carregar_arquivo(file_sysemp)
            
            nfs_bloqueadas = set()
            if file_base:
                nfs_bloqueadas = carregar_base_tratativas(file_base)

            # 2. Tratamento
            df_inteli = tratar_intelipost(df_inteli_raw)
            df_sysemp = tratar_sysemp(df_sysemp_raw)

            if df_inteli.empty:
                st.warning("Intelipost vazio após filtros.")
                st.stop()

            # 3. Merge
            df_merged = pd.merge(df_inteli, df_sysemp, on='Nota Fiscal', how='left')

            # 4. Regras
            if 'Pedido_sys' in df_merged.columns:
                df_merged['Pedido'] = df_merged['Pedido_sys'].fillna("N/A")
            elif 'Pedido' not in df_merged.columns:
                df_merged['Pedido'] = "N/A"
            
            if 'Chave NF_sys' in df_merged.columns:
                df_merged['Chave NF'] = df_merged['Chave NF_sys'].fillna("N/A")
            elif 'Chave NF' not in df_merged.columns:
                 df_merged['Chave NF'] = "N/A"

            dict_mkt_norm = {k.upper(): v for k, v in DICIONARIO_MARKETPLACE.items()}
            def corrigir_mkt(val):
                if pd.isna(val): return "VERIFICAR"
                s = str(val).strip().upper()
                return dict_mkt_norm.get(s, str(val))
            
            col_mkt_final = 'Marketplace' if 'Marketplace' in df_merged.columns else None
            if col_mkt_final:
                df_merged['Marketplace Final'] = df_merged[col_mkt_final].apply(corrigir_mkt)
            else:
                df_merged['Marketplace Final'] = "VERIFICAR"

            if 'Transportadora' in df_merged.columns:
                df_merged['Transportadora'] = df_merged['Transportadora'].map(DICIONARIO_TRANSPORTADORA).fillna(df_merged['Transportadora'])
            
            if 'Ocorrência de Entrega' in df_merged.columns:
                df_merged['Ocorrência de Entrega'] = df_merged['Ocorrência de Entrega'].map(DICIONARIO_OCORRENCIA).fillna(df_merged['Ocorrência de Entrega'])

            df_merged['Data Tratativa'] = datetime.now().strftime('%d/%m/%Y')

            # 5. Filtro de Histórico
            total_inicial = len(df_merged)
            mask_exclusao = df_merged['Nota Fiscal'].isin(nfs_bloqueadas)
            
            df_final_filtrado = df_merged[~mask_exclusao].copy()
            df_removidas = df_merged[mask_exclusao].copy()
            
            total_excluido = mask_exclusao.sum()
            total_final = len(df_final_filtrado)

            colunas_desejadas = [
                'Transportadora', 'Chave NF', 'Nota Fiscal', 'UF',
                'Data Tratativa', 'Marketplace Final', 'Pedido', 'Ocorrência de Entrega'
            ]
            
            for c in colunas_desejadas:
                if c not in df_final_filtrado.columns: df_final_filtrado[c] = ""
                if c not in df_removidas.columns: df_removidas[c] = ""
            
            df_export = df_final_filtrado[colunas_desejadas].rename(columns={'Marketplace Final': 'Marketplace'})
            df_export_removidas = df_removidas[colunas_desejadas].rename(columns={'Marketplace Final': 'Marketplace'})

            st.success("✅ Processamento Concluído!")
            
            m1, m2, m3 = st.columns(3)
            m1.metric("Pendências Totais", total_inicial)
            m2.metric("Removidas (Histórico)", int(total_excluido), delta=-int(total_excluido), delta_color="inverse")
            m3.metric("Novas para Tratar", total_final)

            st.subheader("Novas Pendências (Aba 1)")
            st.dataframe(df_export.head())

            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_export.to_excel(writer, index=False, sheet_name='Tratativas (Novas)')
                df_export_removidas.to_excel(writer, index=False, sheet_name='Removidas (No Histórico)')
            
            st.download_button(
                label="📥 BAIXAR PLANILHA COMPLETA",
                data=buffer.getvalue(),
                file_name=f"Tratativas_Full_{datetime.now().strftime('%d-%m')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary"
            )

        except Exception as e:
            st.error("🚨 ERRO CRÍTICO NO SISTEMA")
            st.error(f"Detalhe do erro: {e}")
            st.code(traceback.format_exc())
