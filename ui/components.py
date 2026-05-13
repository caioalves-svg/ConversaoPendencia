import streamlit as st
import pandas as pd
import io
from datetime import datetime
from openpyxl.utils import get_column_letter

# Colunas que devem ser exportadas como TEXTO (preserva zeros à esquerda
# e impede o Excel de renderizar chaves longas em notação científica).
COLUNAS_FORCAR_TEXTO = {
    'Chave NF', 'CHAVE DA NF', 'Chave da NF', 'Chave NFe', 'Chave da Nota',
}

def _forcar_colunas_texto(worksheet, df, colunas_alvo=COLUNAS_FORCAR_TEXTO):
    """Aplica number_format='@' (texto) nas células das colunas alvo.

    Necessário porque o Excel, ao abrir, converte strings que parecem números
    (como chaves de NF de 44 dígitos) em notação científica quando o formato
    da célula é 'Geral'.
    """
    for idx, col_name in enumerate(df.columns, start=1):
        if col_name in colunas_alvo:
            letra = get_column_letter(idx)
            # Linha 1 é cabeçalho; aplica do dado em diante.
            for cell in worksheet[letra][1:]:
                cell.number_format = '@'

def render_header(title, subtitle):
    """Renderiza o cabeçalho da página."""
    st.markdown(f'<h1 class="main-title">{title}</h1>', unsafe_allow_html=True)
    st.markdown(f'<p class="sub-title">{subtitle}</p>', unsafe_allow_html=True)

def render_instructions(tipo="intelipost"):
    """Renderiza o guia passo a passo visual."""
    if tipo == "intelipost":
        steps = [
            {"icon": "📥", "title": "Extração Intelipost", "desc": "Exporte as transações do portal Intelipost (CSV ou XLSX)."},
            {"icon": "🖥️", "title": "Base Sysemp", "desc": "Gere o relatório de 'Manutenção de Notas Fiscais' no Sysemp."},
            {"icon": "🔍", "title": "Histórico (Opcional)", "desc": "Use uma planilha anterior para ignorar NFs já tratadas."},
            {"icon": "🚀", "title": "Processamento", "desc": "Clique no botão de processar e baixe o arquivo final."}
        ]
    elif tipo == "validacao":
        steps = [
            {"icon": "📥", "title": "Extração Intelipost", "desc": "Exporte as transações do portal Intelipost (CSV ou XLSX)."},
            {"icon": "🖥️", "title": "Base Sysemp", "desc": "Gere o relatório de 'Manutenção de Notas Fiscais' no Sysemp."},
            {"icon": "🛡️", "title": "NFs em Tratamento", "desc": "Suba o histórico de NFs em tratamento para descartar duplicidades."},
            {"icon": "✅", "title": "Validação & Export", "desc": "Confronta transportadoras e exporta planilha final auditada."}
        ]
    elif tipo == "prevencao":
        steps = [
            {"icon": "📥", "title": "Extração Intelipost", "desc": "Exporte as transações do portal Intelipost (CSV ou XLSX)."},
            {"icon": "🖥️", "title": "Base Sysemp", "desc": "Gere o relatório de 'Manutenção de Notas Fiscais' no Sysemp."},
            {"icon": "🛡️", "title": "NFs em Tratamento", "desc": "Suba o histórico de NFs em tratamento para descartar duplicidades."},
            {"icon": "📅", "title": "Filtro Hoje + Amanhã", "desc": "Filtra DATA PREVISTA para HOJE e AMANHÃ (prevenção de atrasos)."}
        ]
    else:
        steps = [
            {"icon": "📧", "title": "Dados de E-mail", "desc": "Prepare a planilha com: Nota Fiscal, Transportadora e Ocorrência."},
            {"icon": "🖥️", "title": "Base Sysemp", "desc": "Gere o relatório de 'Manutenção de Notas Fiscais' no Sysemp."},
            {"icon": "🛡️", "title": "Evite Duplicidade", "desc": "Suba o histórico para filtrar registros repetidos."},
            {"icon": "📊", "title": "Resultado", "desc": "Processe e obtenha a planilha formatada para tratativa."}
        ]

    cols = st.columns(len(steps))
    for i, step in enumerate(steps):
        with cols[i]:
            st.markdown(f"""
            <div style="background: white; padding: 20px; border-radius: 15px; border: 1px solid #e2e8f0; text-align: center; height: 180px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05);">
                <div style="font-size: 2rem; margin-bottom: 10px;">{step['icon']}</div>
                <div style="font-weight: 700; color: #1e293b; margin-bottom: 5px; font-size: 0.9rem;">{i+1}. {step['title']}</div>
                <div style="font-size: 0.8rem; color: #64748b; line-height: 1.4;">{step['desc']}</div>
            </div>
            """, unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

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

def render_results_tabs(
    df_final,
    df_removidas,
    nome_arquivo="Tratativas_Full",
    sheet_principal="Tratativas (Novas)",
    sheet_removidas="Removidas (No Histórico)",
    label_principal="📋 Novas Tratativas",
    label_removidas="🗑️ Removidas pelo Histórico",
    msg_vazio_principal="Nenhuma nova pendência para tratar.",
    msg_vazio_removidas="Nenhum registro foi removido.",
):
    """Renderiza as tabs de resultados e exportação.

    Mantém retrocompatibilidade: chamar sem parâmetros adicionais reproduz
    exatamente o comportamento anterior dos fluxos Intelipost e E-mail.
    """
    tab1, tab2 = st.tabs([label_principal, label_removidas])

    with tab1:
        if not df_final.empty:
            st.dataframe(df_final, use_container_width=True)

            # Preparar buffer Excel
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_final.to_excel(writer, index=False, sheet_name=sheet_principal)
                df_removidas.to_excel(writer, index=False, sheet_name=sheet_removidas)
                _forcar_colunas_texto(writer.sheets[sheet_principal], df_final)
                _forcar_colunas_texto(writer.sheets[sheet_removidas], df_removidas)

            st.download_button(
                label="📥 BAIXAR PLANILHA COMPLETA (.xlsx)",
                data=buffer.getvalue(),
                file_name=f"{nome_arquivo}_{datetime.now().strftime('%d-%m')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=True
            )
        else:
            st.info(msg_vazio_principal)

    with tab2:
        if not df_removidas.empty:
            st.dataframe(df_removidas, use_container_width=True)
        else:
            st.write(msg_vazio_removidas)
