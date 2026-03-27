import streamlit as st

def apply_global_styles():
    """Injeta CSS avançado para um visual de software profissional."""
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

        :root {
            --primary: #2563eb;
            --primary-hover: #1d4ed8;
            --bg-gradient: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
            --card-bg: rgba(255, 255, 255, 0.9);
            --text-main: #0f172a;
            --text-muted: #64748b;
        }

        /* Reset e Tipografia */
        html, body, [class*="css"] {
            font-family: 'Inter', sans-serif;
            color: var(--text-main);
        }

        .stApp {
            background: var(--bg-gradient);
        }

        /* Sidebar Glassmorphism */
        [data-testid="stSidebar"] {
            background-color: rgba(255, 255, 255, 0.5);
            backdrop-filter: blur(15px);
            border-right: 1px solid rgba(0,0,0,0.05);
        }

        /* Estilo dos Títulos */
        .main-title {
            font-size: 2.75rem;
            font-weight: 800;
            letter-spacing: -0.025em;
            background: linear-gradient(90deg, #1e293b 0%, #334155 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 0.5rem;
        }

        .sub-title {
            color: var(--text-muted);
            font-size: 1.125rem;
            margin-bottom: 2.5rem;
        }

        /* Container de Upload */
        .stFileUploader {
            background: white !important;
            padding: 1.5rem !important;
            border-radius: 1rem !important;
            border: 2px dashed #cbd5e1 !important;
            box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1) !important;
        }

        /* Botão Customizado */
        .stButton > button {
            background: var(--primary);
            color: white !important;
            border-radius: 0.75rem;
            padding: 0.75rem 1.5rem;
            font-weight: 600;
            border: none;
            width: 100%;
            transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
            box-shadow: 0 10px 15px -3px rgba(37, 99, 235, 0.3);
        }

        .stButton > button:hover {
            background: var(--primary-hover);
            transform: translateY(-2px);
            box-shadow: 0 20px 25px -5px rgba(37, 99, 235, 0.4);
        }

        /* Cards de Métricas */
        .metric-card {
            background: white;
            padding: 1.5rem;
            border-radius: 1rem;
            border: 1px solid #f1f5f9;
            box-shadow: 0 10px 15px -3px rgb(0 0 0 / 0.05);
            transition: transform 0.2s;
        }
        
        .metric-card:hover {
            transform: scale(1.02);
        }

        .metric-label {
            font-size: 0.875rem;
            font-weight: 600;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .metric-value {
            font-size: 2.25rem;
            font-weight: 700;
            color: #1e293b;
        }

        /* Tabs customizadas */
        .stTabs [data-baseweb="tab-list"] {
            gap: 2rem;
            background-color: transparent;
        }

        .stTabs [data-baseweb="tab"] {
            height: 3rem;
            background-color: transparent;
            border-radius: 4px;
            color: var(--text-muted);
            font-weight: 600;
        }

        .stTabs [aria-selected="true"] {
            color: var(--primary) !important;
            border-bottom-color: var(--primary) !important;
        }

        /* Alertas */
        .stAlert {
            border-radius: 0.75rem !important;
            border: none !important;
        }
    </style>
    """, unsafe_allow_html=True)
