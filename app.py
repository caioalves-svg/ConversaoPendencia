import streamlit as st
import pandas as pd
from datetime import datetime
import io
import traceback

# ==============================================================================
# CONFIGURAÇÃO DA PÁGINA E ESTILOS CSS
# ==============================================================================
st.set_page_config(
    page_title="Gestão Logística Pro",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

def apply_custom_css():
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

        html, body, [class*="css"] {
            font-family: 'Inter', sans-serif;
        }

        /* Fundo da App */
        .stApp {
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        }

        /* Sidebar elegante */
        [data-testid="stSidebar"] {
            background-color: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-right: 1px solid rgba(0,0,0,0.05);
        }

        /* Cards de Métricas Premium */
        .metric-container {
            display: flex;
            justify-content: space-between;
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .metric-card {
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 10px 15px -3px rgba(0,0,0,0.1), 0 4px 6px -2px rgba(0,0,0,0.05);
            flex: 1;
            text-align: left;
            position: relative;
            overflow: hidden;
            border: 1px solid rgba(255,255,255,0.3);
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        }

        .metric-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 20px 25px -5px rgba(0,0,0,0.1), 0 10px 10px -5px rgba(0,0,0,0.04);
        }

        .metric-card::before {
            content: "";
            position: absolute;
            top: 0;
            left: 0;
            width: 5px;
            height: 100%;
        }

        .card-total::before { background: #4299e1; }
        .card-removidas::before { background: #f56565; }
        .card-novas::before { background: #48bb78; }

        .metric-value {
            font-size: 2.2rem;
            font-weight: 800;
            color: #1a202c;
            line-height: 1;
            margin-bottom: 8px;
        }
        
        .metric-label {
            font-size: 0.85rem;
            font-weight: 600;
            color: #718096;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        /* Estilização de Containers de Upload */
        .stFileUploader {
            background-color: white !important;
            padding: 20px !important;
            border-radius: 12px !important;
            border: 2px dashed #e2e8f0 !important;
            transition: border-color 0.3s !important;
        }
        .stFileUploader:hover {
            border-color: #4299e1 !important;
        }

        /* Botão Principal */
        .stButton>button {
            background: linear-gradient(90deg, #3182ce 0%, #2b6cb0 100%);
            color: white !important;
            border: none;
            padding: 12px 24px;
            font-size: 1rem;
            font-weight: 700;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(49, 130, 206, 0.3);
            width: 100%;
            transition: all 0.2s;
        }
        .stButton>button:hover {
            box-shadow: 0 7px 14px rgba(49, 130, 206, 0.4);
            transform: scale(1.01);
            color: white !important;
        }

        /* Tabelas */
        .stDataFrame {
            background: white;
            border-radius: 12px;
            padding: 10px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.02);
        }

        /* Títulos e Divisores */
        .main-title {
            font-size: 2.5rem;
            font-weight: 800;
            background: linear-gradient(90deg, #2d3748 0%, #4a5568 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 0.5rem;
        }
        
        .sub-title {
            color: #718096;
            font-size: 1.1rem;
            margin-bottom: 2rem;
        }

        /* Instruções */
        .instruction-box {
            background: rgba(255, 255, 255, 0.6);
            padding: 20px;
            border-radius: 12px;
            border-left: 5px solid #3182ce;
            margin-bottom: 25px;
        }
    </style>
    """, unsafe_allow_html=True)

# ==============================================================================
# DICIONÁRIOS E PADRONIZAÇÕES
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
# FUNÇÕES DE UTILIDADE E TRATAMENTO
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

def tratar_sysemp(df):
    candidatas = [c for c in df.columns if 'EMPRESA' in c.upper()]
    coluna_id_final = None
    
    for col in candidatas:
        temp_series = pd.to_numeric(df[col], errors='coerce')
        matches = temp_series.isin([16, 18, 19, 21]).sum()
        if matches > 0:
            coluna_id_final = col
            break
            
    if not coluna_id_final:
        return pd.DataFrame(), "Não encontrada coluna com IDs de empresa (16, 18, 19, 21) no Sysemp."

    df['temp_id'] = pd.to_numeric(df[coluna_id_final], errors='coerce')
    df_filtrado = df[df['temp_id'].isin([16, 18, 19, 21])].copy()
    
    if df_filtrado.empty:
        return pd.DataFrame(), "Filtro de empresas (16, 18, 19, 21) retornou vazio no Sysemp."

    col_nf = encontrar_coluna(df_filtrado, ['Nota Fiscal', 'NF', 'Numero NF'])
    if not col_nf:
        return pd.DataFrame(), "Coluna 'Nota Fiscal' não encontrada no Sysemp."
    
    df_filtrado['Nota Fiscal'] = df_filtrado[col_nf].apply(normalizar_nf)

    col_chave = encontrar_coluna(df_filtrado, ['Chave NFe', 'Chave NF', 'Chave'])
    col_pedido_final = None
    if 'Pedido Marketplace' in df_filtrado.columns:
        col_pedido_final = 'Pedido Marketplace'
    else:
        for col in df_filtrado.columns:
            if "PEDIDO" in col.upper() and "MARKETPLACE" in col.upper():
                col_pedido_final = col
                break
        if not col_pedido_final:
             col_pedido_final = encontrar_coluna(df_filtrado, ['Pedido'])

    df_novo = pd.DataFrame()
    df_novo['Nota Fiscal'] = df_filtrado['Nota Fiscal']
    df_novo['Chave NF_sys'] = df_filtrado[col_chave].astype(str).str.replace('.0', '', regex=False).str.replace('nan', '', case=False).str.strip() if col_chave else "N/A"
    df_novo['Pedido_sys'] = df_filtrado[col_pedido_final].astype(str).str.replace('.0', '', regex=False).str.replace('nan', '', case=False).str.strip() if col_pedido_final else "N/A"
    
    return df_novo, None

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

# ==============================================================================
# MOTORES DE PROCESSAMENTO
# ==============================================================================

def processar_base_comum(df_entrada, df_sysemp, nfs_historico):
    df_merged = pd.merge(df_entrada, df_sysemp, on='Nota Fiscal', how='left')

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
    
    col_mkt_ref = 'Marketplace' if 'Marketplace' in df_merged.columns else None
    df_merged['Marketplace Final'] = df_merged[col_mkt_ref].apply(corrigir_mkt) if col_mkt_ref else "VERIFICAR"

    if 'Transportadora' in df_merged.columns:
        df_merged['Transportadora'] = df_merged['Transportadora'].map(DICIONARIO_TRANSPORTADORA).fillna(df_merged['Transportadora'])
    
    if 'Ocorrência de Entrega' in df_merged.columns:
        df_merged['Ocorrência de Entrega'] = df_merged['Ocorrência de Entrega'].map(DICIONARIO_OCORRENCIA).fillna(df_merged['Ocorrência de Entrega'])

    df_merged['Data Tratativa'] = datetime.now().strftime('%d/%m/%Y')

    mask_exclusao = df_merged['Nota Fiscal'].isin(nfs_historico)
    df_final = df_merged[~mask_exclusao].copy()
    df_removidas = df_merged[mask_exclusao].copy()

    colunas_finais = [
        'Transportadora', 'Chave NF', 'Nota Fiscal', 'UF',
        'Data Tratativa', 'Marketplace Final', 'Pedido', 'Ocorrência de Entrega'
    ]
    
    for df in [df_final, df_removidas]:
        for c in colunas_finais:
            if c not in df.columns: df[c] = ""
    
    df_final = df_final[colunas_finais].rename(columns={'Marketplace Final': 'Marketplace'})
    df_removidas = df_removidas[colunas_finais].rename(columns={'Marketplace Final': 'Marketplace'})
    
    return df_final, df_removidas

def processar_intelipost(df_inteli, df_sysemp, nfs_historico):
    col_mkt = encontrar_coluna(df_inteli, ['Canal de Vendas', 'Marketplace'])
    col_micro = encontrar_coluna(df_inteli, ['MicroStatus', 'Ocorrência de Entrega', 'Status'])
    col_nf = encontrar_coluna(df_inteli, ['Nota Fiscal', 'NF', 'Pedido do Cliente']) 

    if col_mkt: df_inteli = df_inteli.rename(columns={col_mkt: 'Marketplace'})
    if col_micro: df_inteli = df_inteli.rename(columns={col_micro: 'Ocorrência de Entrega'})
    if col_nf and col_nf != 'Nota Fiscal': df_inteli = df_inteli.rename(columns={col_nf: 'Nota Fiscal'})
    
    if 'Nota Fiscal' not in df_inteli.columns:
        return None, None, "Coluna 'Nota Fiscal' não identificada no arquivo Intelipost."

    df_inteli['Nota Fiscal'] = df_inteli['Nota Fiscal'].apply(normalizar_nf)
    
    if 'Ocorrência de Entrega' in df_inteli.columns:
        df_inteli['Ocorrência de Entrega'] = df_inteli['Ocorrência de Entrega'].astype(str).str.upper()
        df_inteli = df_inteli[~df_inteli['Ocorrência de Entrega'].str.contains("ATRASO|INFORMATIVO", na=False)]
    
    return processar_base_comum(df_inteli, df_sysemp, nfs_historico)

def processar_email(df_email, df_sysemp, nfs_historico):
    col_nf = encontrar_coluna(df_email, ['NOTA FISCAL', 'NF', 'NÚMERO'])
    col_transp = encontrar_coluna(df_email, ['TRANSPORTADORA', 'TRANSP'])
    col_ocorr = encontrar_coluna(df_email, ['OCORRÊNCIA', 'OCORRENCIA', 'STATUS'])

    if not all([col_nf, col_transp, col_ocorr]):
        missing = []
        if not col_nf: missing.append("NOTA FISCAL")
        if not col_transp: missing.append("TRANSPORTADORA")
        if not col_ocorr: missing.append("OCORRÊNCIA")
        return None, None, f"Colunas obrigatórias não encontradas: {', '.join(missing)}"

    df_email = df_email.rename(columns={
        col_nf: 'Nota Fiscal',
        col_transp: 'Transportadora',
        col_ocorr: 'Ocorrência de Entrega'
    })

    df_email['Nota Fiscal'] = df_email['Nota Fiscal'].apply(normalizar_nf)
    
    return processar_base_comum(df_email, df_sysemp, nfs_historico)

# ==============================================================================
# INTERFACE DE RESULTADOS MELHORADA
# ==============================================================================

def exibir_resultados(df_final, df_removidas):
    total_novas = len(df_final)
    total_removidas = len(df_removidas)
    total_geral = total_novas + total_removidas

    st.markdown("---")
    
    st.markdown(f"""
    <div class="metric-container">
        <div class="metric-card card-total">
            <div class="metric-label">Total Processado</div>
            <div class="metric-value">{total_geral}</div>
        </div>
        <div class="metric-card card-removidas">
            <div class="metric-label">Removidas (Histórico)</div>
            <div class="metric-value" style="color: #c53030;">{total_removidas}</div>
        </div>
        <div class="metric-card card-novas">
            <div class="metric-label">Novas Tratativas</div>
            <div class="metric-value" style="color: #2f855a;">{total_novas}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["📋 Novas Pendências", "🗑️ Registros Removidos"])
    
    with tab1:
        if not df_final.empty:
            st.dataframe(df_final, use_container_width=True)
            
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_final.to_excel(writer, index=False, sheet_name='Tratativas (Novas)')
                df_removidas.to_excel(writer, index=False, sheet_name='Removidas (No Histórico)')
            
            st.download_button(
                label="📥 BAIXAR PLANILHA COMPLETA (.xlsx)",
                data=buffer.getvalue(),
                file_name=f"Tratativas_Full_{datetime.now().strftime('%d-%m')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=True
            )
        else:
            st.info("Nenhuma nova pendência para tratar.")

    with tab2:
        if not df_removidas.empty:
            st.dataframe(df_removidas, use_container_width=True)
        else:
            st.write("Nenhum registro foi removido pelo filtro de histórico.")

# ==============================================================================
# FLUXOS PRINCIPAIS MELHORADOS
# ==============================================================================

def fluxo_intelipost():
    st.markdown('<h1 class="main-title">🚚 Pendência - Intelipost</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-title">Automação de cruzamento de dados para exportações do portal Intelipost.</p>', unsafe_allow_html=True)
    
    with st.expander("📖 Como usar este fluxo", expanded=False):
        st.markdown("""
        1. **Arquivo Intelipost:** Exporte as transações do portal (CSV ou XLSX).
        2. **Arquivo Sysemp:** Gere o relatório de 'Manutenção de Notas Fiscais'.
        3. **Histórico:** (Opcional) Carregue a última planilha processada para evitar duplicidade.
        """)

    with st.container():
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("**1. Fonte de Dados**")
            file_inteli = st.file_uploader("Upload Intelipost", type=["xlsx", "csv"])
        with c2:
            st.markdown("**2. Base Sysemp**")
            file_sys = st.file_uploader("Upload Sysemp", type=["xlsx", "csv"])
        with c3:
            st.markdown("**3. Filtro de Histórico**")
            file_hist = st.file_uploader("Upload Histórico (Opcional)", type=["xlsx", "csv"])

    if st.button("🚀 INICIAR PROCESSAMENTO INTELIGENTE"):
        if not file_inteli or not file_sys:
            st.warning("⚠️ Arquivos obrigatórios ausentes. Por favor, verifique os uploads.")
            return

        with st.status("Processando dados...", expanded=True) as status:
            try:
                st.write("Lendo arquivos...")
                df_inteli_raw = carregar_arquivo(file_inteli)
                df_sys_raw = carregar_arquivo(file_sys)
                nfs_hist = carregar_base_tratativas(file_hist)

                st.write("Tratando base Sysemp...")
                df_sys_clean, err = tratar_sysemp(df_sys_raw)
                if err:
                    st.error(f"Erro no Sysemp: {err}")
                    return

                st.write("Cruzando com Intelipost...")
                df_f, df_r, err_p = processar_intelipost(df_inteli_raw, df_sys_clean, nfs_hist)
                if err_p:
                    st.error(err_p)
                    return

                status.update(label="Processamento concluído!", state="complete", expanded=False)
                exibir_resultados(df_f, df_r)
            except Exception as e:
                st.error(f"Erro inesperado: {str(e)}")
                st.code(traceback.format_exc())

def fluxo_email():
    st.markdown('<h1 class="main-title">📧 Pendência - E-mail</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-title">Processamento ágil para dados recebidos manualmente.</p>', unsafe_allow_html=True)
    
    st.markdown("""
    <div class="instruction-box">
        <strong>💡 Requisito de Colunas:</strong><br>
        O arquivo de e-mail deve conter obrigatoriamente as colunas: 
        <u>Nota Fiscal</u>, <u>Transportadora</u> e <u>Ocorrência</u>.
    </div>
    """, unsafe_allow_html=True)

    with st.container():
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("**1. Dados de E-mail**")
            file_email = st.file_uploader("Upload Planilha E-mail", type=["xlsx", "csv"])
        with c2:
            st.markdown("**2. Base Sysemp**")
            file_sys = st.file_uploader("Upload Sysemp", type=["xlsx", "csv"])
        with c3:
            st.markdown("**3. Histórico**")
            file_hist = st.file_uploader("Upload Histórico (Opcional)", type=["xlsx", "csv"])

    if st.button("🚀 PROCESSAR DADOS DE E-MAIL"):
        if not file_email or not file_sys:
            st.warning("⚠️ Arquivos obrigatórios ausentes.")
            return

        with st.status("Processando fluxo de e-mail...", expanded=True) as status:
            try:
                st.write("Carregando bases...")
                df_email_raw = carregar_arquivo(file_email)
                df_sys_raw = carregar_arquivo(file_sys)
                nfs_hist = carregar_base_tratativas(file_hist)

                st.write("Normalizando Sysemp...")
                df_sys_clean, err = tratar_sysemp(df_sys_raw)
                if err:
                    st.error(f"Erro no Sysemp: {err}")
                    return

                st.write("Executando merge e padronização...")
                df_f, df_r, err_p = processar_email(df_email_raw, df_sys_clean, nfs_hist)
                if err_p:
                    st.error(err_p)
                    return

                status.update(label="Processamento concluído!", state="complete", expanded=False)
                exibir_resultados(df_f, df_r)
            except Exception as e:
                st.error(f"Erro inesperado: {str(e)}")
                st.code(traceback.format_exc())

# ==============================================================================
# MAIN
# ==============================================================================

def main():
    apply_custom_css()
    
    st.sidebar.image("https://www.intelipost.com.br/wp-content/uploads/2021/05/logo-intelipost.png", width=150)
    st.sidebar.title("Navegação")
    menu = st.sidebar.radio(
        "Selecione o Fluxo:",
        ["Pendência - Intelipost", "Pendência - E-mail"],
        index=0
    )
    
    st.sidebar.markdown("---")
    st.sidebar.info(f"**Versão:** 2.1.0\n**Data:** {datetime.now().strftime('%d/%m/%Y')}")

    if menu == "Pendência - Intelipost":
        fluxo_intelipost()
    else:
        fluxo_email()

if __name__ == "__main__":
    main()
