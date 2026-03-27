import streamlit as st
import traceback
from datetime import datetime

# Importações da Nova Estrutura
from ui.styles import apply_global_styles
from ui.components import render_header, render_metric_card, render_results_tabs, render_instructions
from core.processor import DataProcessor
from utils.helpers import carregar_arquivo

# Configuração Base
st.set_page_config(
    page_title="Gestão Logística Pro",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

def main():
    # 1. Aplica Design System
    apply_global_styles()
    
    # 2. Inicializa Processador
    processor = DataProcessor()

    # 3. Sidebar - Navegação Profissional
    st.sidebar.image("https://intelipost-assets.s3.amazonaws.com/images/logo/logo-intelipost.png", width=160)
    st.sidebar.markdown("<br>", unsafe_allow_html=True)
    
    menu = st.sidebar.radio(
        "MÓDULOS DO SISTEMA",
        ["📦 Pendência - Intelipost", "📧 Pendência - E-mail"],
        index=0
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"""
        <div style='color: #64748b; font-size: 0.8rem;'>
            <b>Versão:</b> 3.0.0 Enterprise<br>
            <b>Data:</b> {datetime.now().strftime('%d/%m/%Y')}
        </div>
    """, unsafe_allow_html=True)

    # 4. Seleção de Fluxo
    if "Intelipost" in menu:
        render_header("Pendência - Intelipost", "Automação avançada para cruzamento de transações logísticas.")
        render_instructions("intelipost")
        
        with st.container():
            c1, c2, c3 = st.columns(3)
            with c1:
                st.markdown("### 1. Intelipost")
                file_source = st.file_uploader("Upload Transações", type=["xlsx", "csv"], key="inteli")
            with c2:
                st.markdown("### 2. Sysemp")
                file_sys = st.file_uploader("Upload Manutenção NF", type=["xlsx", "csv"], key="sys")
            with c3:
                st.markdown("### 3. Histórico")
                file_hist = st.file_uploader("Filtro de Exclusão (Opcional)", type=["xlsx", "csv"], key="hist")

        if st.button("🚀 PROCESSAR INTELIPOST"):
            if file_source and file_sys:
                executar_processamento(processor, "intelipost", file_source, file_sys, file_hist)
            else:
                st.warning("⚠️ Selecione os arquivos de origem (Intelipost e Sysemp).")

    else:
        render_header("Pendência - E-mail", "Fluxo ágil para tratativas recebidas via comunicação direta.")
        render_instructions("email")
        
        st.info("💡 Certifique-se que o arquivo de e-mail contém: **Nota Fiscal, Transportadora e Ocorrência**.")
        
        with st.container():
            c1, c2, c3 = st.columns(3)
            with c1:
                st.markdown("### 1. Dados de E-mail")
                file_source = st.file_uploader("Upload Planilha E-mail", type=["xlsx", "csv"], key="email")
            with c2:
                st.markdown("### 2. Sysemp")
                file_sys = st.file_uploader("Upload Manutenção NF", type=["xlsx", "csv"], key="sys_email")
            with c3:
                st.markdown("### 3. Histórico")
                file_hist = st.file_uploader("Opcional: Exclusão", type=["xlsx", "csv"], key="hist_email")

        if st.button("🚀 PROCESSAR E-MAIL"):
            if file_source and file_sys:
                executar_processamento(processor, "email", file_source, file_sys, file_hist)
            else:
                st.warning("⚠️ Selecione os arquivos de origem (E-mail e Sysemp).")

def executar_processamento(processor, tipo, file_source, file_sys, file_hist):
    """Orquestra a execução do processamento e renderiza resultados."""
    with st.status("Executando motor de inteligência logistica...", expanded=True) as status:
        try:
            # Carregamento
            st.write("📖 Lendo arquivos de entrada...")
            df_source_raw = carregar_arquivo(file_source)
            df_sys_raw = carregar_arquivo(file_sys)
            nfs_hist = processor.carregar_base_historico(file_hist)

            # Tratamento Sysemp
            st.write("⚙️ Normalizando base Sysemp...")
            df_sys_clean, err = processor.tratar_sysemp(df_sys_raw)
            if err:
                st.error(err)
                return

            # Processamento Específico
            st.write("🔄 Cruzando dados e aplicando dicionários...")
            if tipo == "intelipost":
                (df_f, df_r), err_p = processor.processar_intelipost(df_source_raw, df_sys_clean, nfs_hist)
            else:
                (df_f, df_r), err_p = processor.processar_email(df_source_raw, df_sys_clean, nfs_hist)

            if err_p:
                st.error(err_p)
                return

            status.update(label="✅ Processamento Concluído!", state="complete", expanded=False)

            # Renderização de Métricas
            st.markdown("<br>", unsafe_allow_html=True)
            m1, m2, m3 = st.columns(3)
            with m1: render_metric_card("Total Processado", len(df_f) + len(df_r))
            with m2: render_metric_card("Removidas (Histórico)", len(df_r), color="#dc2626")
            with m3: render_metric_card("Novas para Tratar", len(df_f), color="#16a34a")

            # Resultados
            render_results_tabs(df_f, df_r)

        except Exception as e:
            st.error(f"🚨 ERRO CRÍTICO: {str(e)}")
            with st.expander("Ver Log Técnico"):
                st.code(traceback.format_exc())

if __name__ == "__main__":
    main()
