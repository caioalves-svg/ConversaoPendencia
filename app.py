import streamlit as st
import pandas as pd
from datetime import datetime
import io
import traceback  # Para mostrar o erro detalhado se acontecer

# ==============================================================================
# CONFIGURAÇÃO VISUAL
# ==============================================================================
st.set_page_config(
    page_title="Gestão de Tratativas Logísticas",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# CSS para métricas
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
# DICIONÁRIOS (ESSENCIAIS PARA O MAPEAMENTO)
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
    "TIKTOK": "TIKTOK", "AMAZON DBA": "AMAZON DBA", "Via Pajucara": "PAJUÇARA"
}

DICIONARIO_TRANSPORTADORA = {
    "Atual Cargas": "ATUAL", "Brasil Web Standard": "BRASIL WEB", "Favorita Transportes": "FAVORITA",
    "FrontLog": "FRONTLOG", "Generoso": "GENEROSO", "JadLog": "JADLOG", "Logan Express": "LOGAN",
    "MMA Cargas Expressas": "MMA", "Patrus": "PATRUS", "Reboucas": "REBOUÇAS", "Rede Sul": "REDE SUL",
    "Rio Express Cargas": "RIO EXPRESS", "TJB": "TJB", "Total": "TOTAL", "Trilog Express": "TRILOG"
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
    """Garante que a NF seja texto puro (remove .0 e espaços)."""
    if pd.isna(valor): return ""
    s = str(valor).strip()
    if s.lower() == 'nan': return ""
    if s.endswith('.0'): s = s.replace('.0', '')
    if ',' in s: s = s.split(',')[0]
    return s

def carregar_arquivo(uploaded_file):
    """Carrega Excel ou CSV com robustez."""
    if uploaded_file.name.endswith('.csv'):
        try:
            return pd.read_csv(uploaded_file, encoding='utf-8')
        except:
            uploaded_file.seek(0)
            return pd.read_csv(uploaded_file, sep=';', encoding='latin1')
    else:
        return pd.read_excel(uploaded_file)

def carregar_base_tratativas(file_base):
    """Lê a base histórica e retorna conjunto de NFs para bloquear."""
    if file_base is None: return set()
    try:
        df_base = carregar_arquivo(file_base)
        col_nf_base = None
        for col in df_base.columns:
            if "NOTA" in col.upper() and "FISCAL" in col.upper():
                col_nf_base = col
                break
            if col.upper() == "NF":
                col_nf_base = col
                break
        if col_nf_base:
            return set(df_base[col_nf_base].apply(normalizar_nf))
        return set()
    except:
        return set()

def tratar_sysemp(df):
    """Limpa Sysemp, filtra empresas e normaliza tipos."""
    st.info("Processando Sysemp...", icon="⚙️")
    
    # Identifica ID da Empresa
    coluna_id = 'Empresa'
    if 'Empresa.1' in df.columns: coluna_id = 'Empresa.1'

    # Filtro de Empresa
    if coluna_id in df.columns:
        df['temp_id'] = pd.to_numeric(df[coluna_id], errors='coerce')
        df = df[df['temp_id'].isin([16, 18, 19, 21])].copy()
    
    # Normalização NF
    df['Nota Fiscal'] = df['Nota Fiscal'].apply(normalizar_nf)

    # Seleção de Colunas (Evita duplicidade no merge)
    colunas_map = {
        'Nota Fiscal': 'Nota Fiscal',
        'Chave NFe': 'Chave NF',
        'Pedido Marketplace': 'Pedido' # Renomeia direto aqui
    }
    
    # Garante que só pegamos colunas que existem
    cols_existentes = {}
    for orig, dest in colunas_map.items():
        if orig in df.columns:
            cols_existentes[orig] = dest
            # Força texto para não perder chave/pedido
            df[orig] = df[orig].astype(str).str.replace('.0', '', regex=False).str.replace('nan', '', case=False)

    return df[list(cols_existentes.keys())].rename(columns=cols_existentes).copy()

def tratar_intelipost(df):
    """Limpa Intelipost e remove ocorrências irrelevantes."""
    st.info("Processando Intelipost...", icon="⚙️")
    
    df = df.rename(columns={
        'Canal de Vendas': 'Marketplace',
        'MicroStatus': 'Ocorrência de Entrega'
    })
    
    df['Nota Fiscal'] = df['Nota Fiscal'].apply(normalizar_nf)
    
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

            # 2. Tratamento Individual
            df_inteli = tratar_intelipost(df_inteli_raw)
            df_sysemp = tratar_sysemp(df_sysemp_raw)

            if df_inteli.empty:
                st.warning("Intelipost vazio após filtros.")
                st.stop()

            # 3. Merge (Cruzamento)
            # suffixes evita erro se tiver colunas com mesmo nome (ex: Transportadora)
            df_merged = pd.merge(df_inteli, df_sysemp, on='Nota Fiscal', how='left', suffixes=('', '_sys'))

            # 4. Regras de Negócio e Preenchimento
            
            # Marketplace (Tenta pegar do Intelipost, normaliza)
            dict_mkt_norm = {k.upper(): v for k, v in DICIONARIO_MARKETPLACE.items()}
            def corrigir_mkt(val):
                if pd.isna(val): return "VERIFICAR"
                s = str(val).strip().upper()
                return dict_mkt_norm.get(s, str(val))
            
            if 'Marketplace' in df_merged.columns:
                df_merged['Marketplace Final'] = df_merged['Marketplace'].apply(corrigir_mkt)
            else:
                df_merged['Marketplace Final'] = "VERIFICAR"

            # Transportadora e Ocorrência
            if 'Transportadora' in df_merged.columns:
                df_merged['Transportadora'] = df_merged['Transportadora'].map(DICIONARIO_TRANSPORTADORA).fillna(df_merged['Transportadora'])
            
            if 'Ocorrência de Entrega' in df_merged.columns:
                df_merged['Ocorrência de Entrega'] = df_merged['Ocorrência de Entrega'].map(DICIONARIO_OCORRENCIA).fillna(df_merged['Ocorrência de Entrega'])

            # Verifica Chave e Pedido (Podem ter vindo vazios se não deu match)
            for col in ['Chave NF', 'Pedido']:
                if col not in df_merged.columns:
                    df_merged[col] = 'N/A'
                df_merged[col] = df_merged[col].fillna('N/A')
            
            df_merged['Data Tratativa'] = datetime.now().strftime('%d/%m/%Y')

            # 5. Filtro de Exclusão (Histórico)
            total_inicial = len(df_merged)
            mask_exclusao = df_merged['Nota Fiscal'].isin(nfs_bloqueadas)
            
            # AQUI ESTAVA O ERRO LÓGICO: Precisamos criar um df filtrado
            df_final_filtrado = df_merged[~mask_exclusao].copy()
            
            total_excluido = mask_exclusao.sum()
            total_final = len(df_final_filtrado)

            # 6. Preparação para Excel
            colunas_desejadas = [
                'Transportadora', 'Chave NF', 'Nota Fiscal', 'UF',
                'Data Tratativa', 'Marketplace Final', 'Pedido', 'Ocorrência de Entrega'
            ]
            
            # Cria colunas vazias se faltar alguma
            for c in colunas_desejadas:
                if c not in df_final_filtrado.columns:
                    df_final_filtrado[c] = ""
            
            df_export = df_final_filtrado[colunas_desejadas].rename(columns={'Marketplace Final': 'Marketplace'})

            # 7. Dashboard e Resultados
            st.success("✅ Processamento Concluído!")
            
            m1, m2, m3 = st.columns(3)
            m1.metric("Pendências Totais", total_inicial)
            m2.metric("Já em Tratativa", int(total_excluido), delta=-int(total_excluido), delta_color="inverse")
            m3.metric("Novas para Tratar", total_final, delta=int(total_final))

            if total_final > 0:
                st.subheader("Visualização (Novas)")
                st.dataframe(df_export.head())

                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df_export.to_excel(writer, index=False, sheet_name='Tratativas')
                
                st.download_button(
                    label="📥 Baixar Planilha Final",
                    data=buffer.getvalue(),
                    file_name=f"Tratativas_{datetime.now().strftime('%d-%m')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary"
                )
            else:
                st.balloons()
                st.info("🎉 Maravilha! Todas as pendências já estão na base histórica.")

        except Exception as e:
            st.error("🚨 ERRO CRÍTICO NO SISTEMA")
            st.error(f"Detalhe do erro: {e}")
            st.code(traceback.format_exc()) # Mostra onde foi o erro no código