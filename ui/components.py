import streamlit as st
import pandas as pd
import io
from datetime import datetime

def render_header(title, subtitle):
    """Renderiza o cabeçalho da página."""
    st.markdown(f'<h1 class="main-title">{title}</h1>', unsafe_allow_html=True)
    st.markdown(f'<p class="sub-title">{subtitle}</p>', unsafe_allow_html=True)

def render_metric_card(label, value, delta=None, color="#1e293b"):
    """Renderiza um card de métrica customizado."""
    delta_html = f'<span style="color: {color}; font-size: 0.875rem;">{delta}</span>' if delta else ""
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value" style="color: {color};">{value}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)

def render_results_tabs(df_final, df_removidas):
    """Renderiza as tabs de resultados e exportação."""
    tab1, tab2 = st.tabs(["📋 Novas Tratativas", "🗑️ Removidas pelo Histórico"])
    
    with tab1:
        if not df_final.empty:
            st.dataframe(df_final, use_container_width=True)
            
            # Preparar buffer Excel
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
            st.write("Nenhum registro foi removido.")
