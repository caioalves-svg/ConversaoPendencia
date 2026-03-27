import pandas as pd
from datetime import datetime
from core.config import MARKETPLACES, CARRIERS, OCCURRENCES, FINAL_COLUMNS
from utils.helpers import normalizar_nf, encontrar_coluna, carregar_arquivo

class DataProcessor:
    def __init__(self):
        self.dict_mkt_norm = {k.upper(): v for k, v in MARKETPLACES.items()}

    def carregar_base_historico(self, file_base):
        """Carrega conjunto de NFs do histórico para exclusão."""
        if file_base is None: return set()
        try:
            df_base = carregar_arquivo(file_base)
            col_nf_base = encontrar_coluna(df_base, ['Nota Fiscal', 'NF', 'Numero NF'])
            if col_nf_base:
                return set(df_base[col_nf_base].apply(normalizar_nf))
            return set()
        except:
            return set()

    def tratar_sysemp(self, df):
        """Pipeline de limpeza da base Sysemp."""
        candidatas = [c for c in df.columns if 'EMPRESA' in c.upper()]
        coluna_id_final = None
        
        for col in candidatas:
            temp_series = pd.to_numeric(df[col], errors='coerce')
            matches = temp_series.isin([16, 18, 19, 21]).sum()
            if matches > 0:
                coluna_id_final = col
                break
                
        if not coluna_id_final:
            return pd.DataFrame(), "Coluna com IDs de empresa (16, 18, 19, 21) não encontrada no Sysemp."

        df['temp_id'] = pd.to_numeric(df[coluna_id_final], errors='coerce')
        df_filtrado = df[df['temp_id'].isin([16, 18, 19, 21])].copy()
        
        if df_filtrado.empty:
            return pd.DataFrame(), "Filtro de empresas (16, 18, 19, 21) resultou em base vazia."

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

    def _corrigir_mkt(self, val):
        if pd.isna(val): return "VERIFICAR"
        s = str(val).strip().upper()
        return self.dict_mkt_norm.get(s, str(val))

    def _aplicar_merge_e_filtros(self, df_entrada, df_sysemp, nfs_historico):
        """Lógica comum de merge e padronização final."""
        df_merged = pd.merge(df_entrada, df_sysemp, on='Nota Fiscal', how='left')

        # Normalização de Chave e Pedido
        df_merged['Pedido'] = df_merged['Pedido_sys'].fillna("N/A") if 'Pedido_sys' in df_merged.columns else "N/A"
        df_merged['Chave NF'] = df_merged['Chave NF_sys'].fillna("N/A") if 'Chave NF_sys' in df_merged.columns else "N/A"

        # Aplicar Dicionários
        col_mkt_ref = 'Marketplace' if 'Marketplace' in df_merged.columns else None
        df_merged['Marketplace Final'] = df_merged[col_mkt_ref].apply(self._corrigir_mkt) if col_mkt_ref else "VERIFICAR"

        if 'Transportadora' in df_merged.columns:
            df_merged['Transportadora'] = df_merged['Transportadora'].map(CARRIERS).fillna(df_merged['Transportadora'])
        
        if 'Ocorrência de Entrega' in df_merged.columns:
            df_merged['Ocorrência de Entrega'] = df_merged['Ocorrência de Entrega'].map(OCCURRENCES).fillna(df_merged['Ocorrência de Entrega'])

        df_merged['Data Tratativa'] = datetime.now().strftime('%d/%m/%Y')

        # Separação por Histórico
        mask_exclusao = df_merged['Nota Fiscal'].isin(nfs_historico)
        df_final = df_merged[~mask_exclusao].copy()
        df_removidas = df_merged[mask_exclusao].copy()

        # Ajuste Final de Colunas
        for df in [df_final, df_removidas]:
            for c in FINAL_COLUMNS:
                if c not in df.columns: df[c] = ""
            if 'Marketplace Final' in df.columns:
                df['Marketplace'] = df['Marketplace Final']

        return df_final[FINAL_COLUMNS], df_removidas[FINAL_COLUMNS]

    def processar_intelipost(self, df_inteli, df_sysemp, nfs_historico):
        """Motor específico para fluxo Intelipost."""
        col_mkt = encontrar_coluna(df_inteli, ['Canal de Vendas', 'Marketplace'])
        col_micro = encontrar_coluna(df_inteli, ['MicroStatus', 'Ocorrência de Entrega', 'Status'])
        col_nf = encontrar_coluna(df_inteli, ['Nota Fiscal', 'NF', 'Pedido do Cliente']) 

        if col_mkt: df_inteli = df_inteli.rename(columns={col_mkt: 'Marketplace'})
        if col_micro: df_inteli = df_inteli.rename(columns={col_micro: 'Ocorrência de Entrega'})
        if col_nf and col_nf != 'Nota Fiscal': df_inteli = df_inteli.rename(columns={col_nf: 'Nota Fiscal'})
        
        if 'Nota Fiscal' not in df_inteli.columns:
            return (None, None), "Coluna 'Nota Fiscal' não identificada no arquivo Intelipost."

        df_inteli['Nota Fiscal'] = df_inteli['Nota Fiscal'].apply(normalizar_nf)
        
        if 'Ocorrência de Entrega' in df_inteli.columns:
            df_inteli['Ocorrência de Entrega'] = df_inteli['Ocorrência de Entrega'].astype(str).str.upper()
            df_inteli = df_inteli[~df_inteli['Ocorrência de Entrega'].str.contains("ATRASO|INFORMATIVO", na=False)]
        
        return self._aplicar_merge_e_filtros(df_inteli, df_sysemp, nfs_historico), None

    def processar_email(self, df_email, df_sysemp, nfs_historico):
        """Motor específico para fluxo E-mail."""
        col_nf = encontrar_coluna(df_email, ['NOTA FISCAL', 'NF', 'NÚMERO'])
        col_transp = encontrar_coluna(df_email, ['TRANSPORTADORA', 'TRANSP'])
        col_ocorr = encontrar_coluna(df_email, ['OCORRÊNCIA', 'OCORRENCIA', 'STATUS'])

        if not all([col_nf, col_transp, col_ocorr]):
            return (None, None), "Colunas obrigatórias (NF, Transportadora, Ocorrência) não encontradas."

        df_email = df_email.rename(columns={
            col_nf: 'Nota Fiscal',
            col_transp: 'Transportadora',
            col_ocorr: 'Ocorrência de Entrega'
        })

        df_email['Nota Fiscal'] = df_email['Nota Fiscal'].apply(normalizar_nf)
        
        return self._aplicar_merge_e_filtros(df_email, df_sysemp, nfs_historico), None
