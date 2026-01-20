import streamlit as st
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
            return pd.read_csv(uploaded_file, sep=';', encoding='latin1')
    else:
        return pd.read_excel(uploaded_file)

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

def encontrar_coluna(df, palavras_chave):
    """
    Procura uma coluna no DataFrame baseada em palavras-chave (case insensitive).
    Retorna o nome real da coluna se encontrar, ou None.
    """
    colunas_reais = df.columns
    # Primeiro tenta match exato
    for chave in palavras_chave:
        if chave in colunas_reais:
            return chave
            
    # Depois tenta match insensível a maiúsculas/espaços
    for chave in palavras_chave:
        for col_real in colunas_reais:
            if chave.upper().replace(" ", "") == col_real.strip().upper().replace(" ", ""):
                return col_real
    return None

def tratar_sysemp(df):
    st.info("Processando Sysemp...", icon="⚙️")
    
    # 1. Identifica ID da Empresa
    coluna_id = encontrar_coluna(df, ['Empresa', 'Empresa.1', 'Cód. Empresa'])
    if not coluna_id:
        st.error("❌ ERRO NO SYSEMP: Não encontrei a coluna de 'Empresa'.")
        return pd.DataFrame()

    # Filtro de Empresa
    df['temp_id'] = pd.to_numeric(df[coluna_id], errors='coerce')
    df_filtrado = df[df['temp_id'].isin([16, 18, 19, 21])].copy()
    
    if df_filtrado.empty:
        st.error(f"❌ ERRO NO FILTRO: Nenhuma linha sobrou após filtrar empresas.")
        return pd.DataFrame()

    df = df_filtrado

    # 2. Busca Nota Fiscal
    col_nf = encontrar_coluna(df, ['Nota Fiscal', 'NF', 'Numero NF'])
    if not col_nf:
        st.error("❌ ERRO NO SYSEMP: Não encontrei a coluna 'Nota Fiscal'.")
        return pd.DataFrame()
    
    df['Nota Fiscal'] = df[col_nf].apply(normalizar_nf)

    # 3. Busca Chave e Pedido (LÓGICA BLINDADA)
    
    # Chave
    col_chave = encontrar_coluna(df, ['Chave NFe', 'Chave NF', 'Chave'])
    
    # --- NOVA LÓGICA DE PEDIDO (Igual à anterior, que funcionou para selecionar a coluna) ---
    col_pedido_final = None
    
    # Passo 1: Varre todas as colunas procurando "PEDIDO" E "MARKETPLACE" no nome
    for col in df.columns:
        nome_col = col.upper().strip()
        if "PEDIDO" in nome_col and "MARKETPLACE" in nome_col:
            col_pedido_final = col
            break 
            
    # Passo 2: Se não achou a 'Top', tenta achar 'PEDIDO MKT'
    if not col_pedido_final:
        col_pedido_final = encontrar_coluna(df, ['Pedido Mkt', 'Ped Marketplace'])
        
    # Passo 3: Se não achou NADA, só aí pega 'Pedido' comum
    if not col_pedido_final:
        col_pedido_final = encontrar_coluna(df, ['Pedido', 'Ped.'])

    # DEBUG
    with st.expander("🕵️‍♂️ DEBUG COLUNAS SYSEMP"):
        st.write(f"Coluna de Pedido Identificada no Sysemp: **{col_pedido_final}**")
        if col_pedido_final:
            st.dataframe(df[[col_pedido_final]].head(3))

    # Montagem do DF Limpo
    df_novo = pd.DataFrame()
    df_novo['Nota Fiscal'] = df['Nota Fiscal']

    if col_chave:
        df_novo['Chave NF'] = df[col_chave].astype(str).str.replace('.0', '', regex=False).str.replace('nan', '', case=False).str.strip()
    else:
        df_novo['Chave NF'] = "N/A"

    if col_pedido_final:
        df_novo['Pedido'] = df[col_pedido_final].astype(str).str.replace('.0', '', regex=False).str.replace('nan', '', case=False).str.strip()
    else:
        df_novo['Pedido'] = "N/A"

    return df_novo

def tratar_intelipost(df):
    st.info("Processando Intelipost...", icon="⚙️")
    
    col_mkt = encontrar_coluna(df, ['Canal de Vendas', 'Marketplace'])
    col_micro = encontrar_coluna(df, ['MicroStatus', 'Ocorrência de Entrega', 'Status'])
    col_nf = encontrar_coluna(df, ['Nota Fiscal', 'NF', 'Pedido do Cliente']) 

    if col_mkt: df = df.rename(columns={col_mkt: 'Marketplace'})
    if col_micro: df = df.rename(columns={col_micro: 'Ocorrência de Entrega'})
    
    if not col_nf and 'Nota Fiscal' not in df.columns:
        st.error("Erro Intelipost: Coluna Nota Fiscal não encontrada.")
        return pd.DataFrame()
    
    if col_nf and col_nf != 'Nota Fiscal':
        df = df.rename(columns={col_nf: 'Nota Fiscal'})

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

            if df_inteli.empty or df_sysemp.empty:
                st.warning("Processamento interrompido. Verifique erros acima.")
                st.stop()

            # 3. Merge (Cruzamento) - AQUI ESTAVA O ERRO DE SELEÇÃO
            # Usamos sufixos explicítos para saber quem é quem
            df_merged = pd.merge(df_inteli, df_sysemp, on='Nota Fiscal', how='left', suffixes=('_inteli', '_sys'))

            # 4. Regras de Negócio
            
            # --- CORREÇÃO DO PEDIDO ---
            # Se existir 'Pedido_sys' (vindo do Sysemp), ele é o rei. Sobrescreve tudo.
            if 'Pedido_sys' in df_merged.columns:
                df_merged['Pedido'] = df_merged['Pedido_sys'].fillna("N/A")
            elif 'Pedido' in df_merged.columns:
                # Se só tiver o da Intelipost, usa ele (mas provavelmente não é o que queremos)
                pass 
            else:
                df_merged['Pedido'] = "N/A"

            # --- CORREÇÃO DA CHAVE NF ---
            if 'Chave NF_sys' in df_merged.columns:
                df_merged['Chave NF'] = df_merged['Chave NF_sys'].fillna("N/A")
            elif 'Chave NF' not in df_merged.columns:
                 df_merged['Chave NF'] = "N/A"

            # Marketplace
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

            # 5. Filtro de Exclusão e Exportação
            total_inicial = len(df_merged)
            mask_exclusao = df_merged['Nota Fiscal'].isin(nfs_bloqueadas)
            df_final_filtrado = df_merged[~mask_exclusao].copy()
            total_excluido = mask_exclusao.sum()
            total_final = len(df_final_filtrado)

            colunas_desejadas = [
                'Transportadora', 'Chave NF', 'Nota Fiscal', 'UF',
                'Data Tratativa', 'Marketplace Final', 'Pedido', 'Ocorrência de Entrega'
            ]
            
            for c in colunas_desejadas:
                if c not in df_final_filtrado.columns: df_final_filtrado[c] = ""
            
            df_export = df_final_filtrado[colunas_desejadas].rename(columns={'Marketplace Final': 'Marketplace'})

            st.success("✅ Processamento Concluído!")
            
            m1, m2, m3 = st.columns(3)
            m1.metric("Pendências Totais", total_inicial)
            m2.metric("Já em Tratativa", int(total_excluido), delta=-int(total_excluido), delta_color="inverse")
            m3.metric("Novas para Tratar", total_final, delta=int(total_final))

            if total_final > 0:
                st.subheader("Visualização")
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
                st.info("Nada pendente!")

        except Exception as e:
            st.error("🚨 ERRO CRÍTICO NO SISTEMA")
            st.error(f"Detalhe do erro: {e}")
            st.code(traceback.format_exc())
