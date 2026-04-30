import pandas as pd
import numpy as np
from datetime import datetime
from core.config import (
    MARKETPLACES, CARRIERS, OCCURRENCES,
    FINAL_COLUMNS, FINAL_COLUMNS_VALIDACAO
)
from utils.helpers import normalizar_nf, encontrar_coluna, carregar_arquivo

class DataProcessor:
    def __init__(self):
        self.dict_mkt_norm = {k.upper(): v for k, v in MARKETPLACES.items()}
        self.dict_transp_norm = {k.upper(): v for k, v in CARRIERS.items()}
        self.dict_ocorr_norm = {k.upper(): v for k, v in OCCURRENCES.items()}

    # --------------------------------------------------------------------- #
    # Helpers internos
    # --------------------------------------------------------------------- #
    @staticmethod
    def _normalizar_pedido(valor):
        """Padroniza nº de pedido (marketplace/Intelipost) como string limpa."""
        if pd.isna(valor):
            return ""
        s = str(valor).strip()
        if s.lower() == 'nan':
            return ""
        if s.endswith('.0'):
            s = s[:-2]
        return s

    @staticmethod
    def _fmt_col(df, col, default=""):
        """Extrai uma coluna do df como string segura, mesmo se a coluna não existir."""
        if col is None or col not in df.columns:
            return pd.Series([default] * len(df), index=df.index)
        return (
            df[col]
            .astype(str)
            .replace({'nan': default, 'NaT': default, 'None': default})
            .fillna(default)
            .str.strip()
        )

    # --------------------------------------------------------------------- #
    # Carregamento e tratamento Sysemp (compartilhado entre módulos)
    # --------------------------------------------------------------------- #
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
        """Pipeline de limpeza da base Sysemp capturando UF, Marketplace e Transportadora."""
        candidatas_empresa = [c for c in df.columns if 'EMPRESA' in c.upper()]
        coluna_id_final = None

        for col in candidatas_empresa:
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

        # Busca colunas essenciais no Sysemp
        col_nf = encontrar_coluna(df_filtrado, ['Nota Fiscal', 'NF', 'Numero NF'])
        col_uf = encontrar_coluna(df_filtrado, ['UF', 'Estado', 'Destinatário UF'])
        col_mkt = encontrar_coluna(df_filtrado, ['Marketplace', 'Canal de Venda', 'Nome do Canal'])
        col_chave = encontrar_coluna(df_filtrado, ['Chave NFe', 'Chave NF', 'Chave'])
        col_transp = encontrar_coluna(df_filtrado, ['Transportadora', 'Transp', 'Nome Transportadora'])

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
        df_novo['Nota Fiscal'] = df_filtrado[col_nf].apply(normalizar_nf) if col_nf else []
        df_novo['Chave NF_sys'] = df_filtrado[col_chave].astype(str).str.replace('.0', '', regex=False).str.replace('nan', '', case=False).str.strip() if col_chave else "N/A"
        df_novo['Pedido_sys'] = df_filtrado[col_pedido_final].astype(str).str.replace('.0', '', regex=False).str.replace('nan', '', case=False).str.strip() if col_pedido_final else "N/A"
        df_novo['UF_sys'] = df_filtrado[col_uf].astype(str).str.upper().str.strip() if col_uf else "N/A"
        df_novo['Marketplace_sys'] = df_filtrado[col_mkt].astype(str).str.upper().str.strip() if col_mkt else "VERIFICAR"
        df_novo['Transportadora_sys'] = df_filtrado[col_transp].astype(str).str.strip() if col_transp else ""

        return df_novo, None

    # --------------------------------------------------------------------- #
    # Lógica compartilhada (Intelipost / E-mail)
    # --------------------------------------------------------------------- #
    def _corrigir_mkt(self, val):
        if pd.isna(val) or val == "N/A" or val == "NONE": return "VERIFICAR"
        s = str(val).strip().upper()
        return self.dict_mkt_norm.get(s, str(val))

    def _aplicar_merge_e_filtros(self, df_entrada, df_sysemp, nfs_historico, prioritario_sysemp=False, converter_ocorrencia=True):
        """Lógica comum de merge e padronização final."""
        df_merged = pd.merge(df_entrada, df_sysemp, on='Nota Fiscal', how='left')

        # Normalização de Chave e Pedido (Sempre do Sysemp)
        df_merged['Pedido'] = df_merged['Pedido_sys'].fillna("N/A") if 'Pedido_sys' in df_merged.columns else "N/A"
        df_merged['Chave NF'] = df_merged['Chave NF_sys'].fillna("N/A") if 'Chave NF_sys' in df_merged.columns else "N/A"

        # Lógica de UF e Marketplace (Diferenciada por fluxo)
        if prioritario_sysemp:
            df_merged['UF'] = df_merged['UF_sys'].fillna("N/A")
            df_merged['Marketplace Raw'] = df_merged['Marketplace_sys'].fillna("VERIFICAR")
        else:
            if 'UF' not in df_merged.columns:
                df_merged['UF'] = df_merged['UF_sys'].fillna("N/A")

            if 'Marketplace' in df_merged.columns:
                df_merged['Marketplace Raw'] = df_merged['Marketplace']
            else:
                df_merged['Marketplace Raw'] = df_merged['Marketplace_sys'].fillna("VERIFICAR")

        # Aplicar Dicionários no Marketplace
        df_merged['Marketplace Final'] = df_merged['Marketplace Raw'].apply(self._corrigir_mkt)

        # Padronização de Transportadora
        if 'Transportadora' in df_merged.columns:
            transp_upper = df_merged['Transportadora'].astype(str).str.upper().str.strip()
            df_merged['Transportadora'] = transp_upper.map(self.dict_transp_norm).fillna(transp_upper)

        # Padronização de Ocorrência (Opcional por fluxo)
        if 'Ocorrência de Entrega' in df_merged.columns:
            ocorr_upper = df_merged['Ocorrência de Entrega'].astype(str).str.upper().str.strip()
            if converter_ocorrencia:
                df_merged['Ocorrência de Entrega'] = ocorr_upper.map(self.dict_ocorr_norm).fillna(ocorr_upper)
            else:
                # No fluxo de e-mail, mantém o texto original mas garante que está em MAIÚSCULO
                df_merged['Ocorrência de Entrega'] = ocorr_upper.replace('NAN', 'VERIFICAR').fillna("VERIFICAR")

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
        """Motor específico para fluxo Intelipost (Prioriza dados do arquivo Intelipost)."""
        col_mkt = encontrar_coluna(df_inteli, ['Canal de Vendas', 'Marketplace'])
        col_micro = encontrar_coluna(df_inteli, ['MicroStatus', 'Ocorrência de Entrega', 'Status'])
        col_nf = encontrar_coluna(df_inteli, ['Nota Fiscal', 'NF', 'Pedido do Cliente'])
        col_uf = encontrar_coluna(df_inteli, ['UF', 'Estado'])

        if col_mkt: df_inteli = df_inteli.rename(columns={col_mkt: 'Marketplace'})
        if col_micro: df_inteli = df_inteli.rename(columns={col_micro: 'Ocorrência de Entrega'})
        if col_nf and col_nf != 'Nota Fiscal': df_inteli = df_inteli.rename(columns={col_nf: 'Nota Fiscal'})
        if col_uf and col_uf != 'UF': df_inteli = df_inteli.rename(columns={col_uf: 'UF'})

        if 'Nota Fiscal' not in df_inteli.columns:
            return (None, None), "Coluna 'Nota Fiscal' não identificada no arquivo Intelipost."

        df_inteli['Nota Fiscal'] = df_inteli['Nota Fiscal'].apply(normalizar_nf)

        if 'Ocorrência de Entrega' in df_inteli.columns:
            df_inteli['Ocorrência de Entrega'] = df_inteli['Ocorrência de Entrega'].astype(str).str.upper()
            df_inteli = df_inteli[~df_inteli['Ocorrência de Entrega'].str.contains("ATRASO|INFORMATIVO", na=False)]

        return self._aplicar_merge_e_filtros(df_inteli, df_sysemp, nfs_historico, prioritario_sysemp=False), None

    def processar_email(self, df_email, df_sysemp, nfs_historico):
        """Motor específico para fluxo E-mail (Puxa UF e Marketplace do Sysemp)."""
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

        # Seta a flag prioritario_sysemp=True para puxar UF e Mkt da base Sysemp
        # Seta converter_ocorrencia=False para manter o texto original da planilha de e-mail
        return self._aplicar_merge_e_filtros(df_email, df_sysemp, nfs_historico, prioritario_sysemp=True, converter_ocorrencia=False), None

    # --------------------------------------------------------------------- #
    # NOVO MÓDULO — Validação de Transportadora
    # --------------------------------------------------------------------- #
    def processar_validacao_transportadora(self, df_inteli, df_sysemp, nfs_historico):
        """
        Motor do fluxo "Validação de Transportadora".

        ETAPA 1 — Cruza Intelipost x Histórico/NFs em tratamento por 'Nota Fiscal'.
                   Linhas presentes no histórico são DESCARTADAS.
        ETAPA 2 — Cruza Intelipost x Sysemp pelo nº de pedido (coluna 'marketplace').
                   Compara transportadora Intelipost x transportadora Sysemp:
                       iguais   -> mantém Intelipost,  STATUS = 'Verdadeiro'
                       diferent -> usa Sysemp,         STATUS = 'Falso'
                       sem match-> mantém Intelipost,  STATUS = 'Não Localizado'
        ETAPA 3 — Monta planilha final na ordem fixa de FINAL_COLUMNS_VALIDACAO.

        Retorno: ((df_final, df_descartadas), erro_str_ou_None)
        """
        if df_inteli is None or df_inteli.empty:
            return (None, None), "Arquivo Intelipost vazio ou inválido."
        if df_sysemp is None or df_sysemp.empty:
            return (None, None), "Base Sysemp tratada está vazia. Verifique IDs de empresa (16, 18, 19, 21)."

        df = df_inteli.copy()

        # ----- Mapeamento de colunas Intelipost ---------------------------- #
        col_data_criacao = encontrar_coluna(df, ['Data Criação', 'Data Criacao', 'Data de Criação', 'Data de Criacao'])
        col_previsao     = encontrar_coluna(df, [
            'Previsão Entrega Cliente Original', 'Previsao Entrega Cliente Original',
            'Previsão Entrega', 'Previsao Entrega', 'Data Prevista'
        ])
        col_uf           = encontrar_coluna(df, ['UF', 'Estado'])
        col_transp       = encontrar_coluna(df, ['Transportadora', 'Transp'])
        col_pedido_inte  = encontrar_coluna(df, ['Pedido', 'Pedido Intelipost', 'Pedido ID'])
        col_chave_nf     = encontrar_coluna(df, ['Chave da Nota', 'Chave NF', 'Chave da NF', 'Chave NFe'])
        col_canal        = encontrar_coluna(df, ['Canal de Vendas', 'Canal de Venda'])
        col_num_pedido   = encontrar_coluna(df, ['marketplace', 'Marketplace', 'N° Pedido', 'Nº Pedido', 'Pedido Marketplace'])
        col_nf           = encontrar_coluna(df, ['Nota Fiscal', 'NF', 'Numero NF'])

        # ----- Validações obrigatórias ------------------------------------- #
        faltando = []
        if not col_nf:           faltando.append("Nota Fiscal")
        if not col_num_pedido:   faltando.append("marketplace (N° Pedido)")
        if not col_transp:       faltando.append("Transportadora")
        if faltando:
            return (None, None), (
                "Colunas obrigatórias não localizadas no Intelipost: "
                + ", ".join(faltando)
            )

        # ----- Normalizações ----------------------------------------------- #
        df['_NF_NORM']     = df[col_nf].apply(normalizar_nf)
        df['_PEDIDO_NORM'] = df[col_num_pedido].apply(self._normalizar_pedido)

        # ----- ETAPA 1 — Filtro pelo histórico ----------------------------- #
        if not isinstance(nfs_historico, set):
            nfs_historico = set(nfs_historico) if nfs_historico else set()

        mask_hist = df['_NF_NORM'].isin(nfs_historico) if nfs_historico else pd.Series(False, index=df.index)
        df_descartadas_raw = df[mask_hist].copy()
        df_validas         = df[~mask_hist].copy()

        # ----- ETAPA 2 — Cruzamento Sysemp por nº pedido + validação ------- #
        df_sysemp_lookup = (
            df_sysemp[['Pedido_sys', 'Transportadora_sys']]
            .copy()
            .assign(Pedido_sys=lambda x: x['Pedido_sys'].apply(self._normalizar_pedido))
        )
        df_sysemp_lookup = df_sysemp_lookup[df_sysemp_lookup['Pedido_sys'] != ""]
        df_sysemp_lookup = df_sysemp_lookup.drop_duplicates(subset='Pedido_sys', keep='first')

        df_merged = pd.merge(
            df_validas,
            df_sysemp_lookup,
            left_on='_PEDIDO_NORM',
            right_on='Pedido_sys',
            how='left'
        )

        transp_inteli = df_merged[col_transp].astype(str).str.upper().str.strip()
        transp_sys    = df_merged['Transportadora_sys'].astype(str).str.upper().str.strip()

        encontrado = (
            df_merged['Transportadora_sys'].notna()
            & (transp_sys != "")
            & (transp_sys != "NAN")
        )
        iguais = encontrado & (transp_inteli == transp_sys)
        diferentes = encontrado & (transp_inteli != transp_sys)

        # Status final
        status = np.where(
            ~encontrado, "Não Localizado",
            np.where(iguais, "Verdadeiro", "Falso")
        )

        # Transportadora final (preserva grafia original — Intelipost ou Sysemp)
        transp_final = np.where(
            diferentes,
            df_merged['Transportadora_sys'].astype(str).str.strip(),
            df_merged[col_transp].astype(str).str.strip(),
        )

        # ----- ETAPA 3 — Montagem do dataframe final ----------------------- #
        hoje = datetime.now().strftime('%d/%m/%Y')

        df_final = pd.DataFrame({
            'DIA DA TRATATIVA':         hoje,
            'DATA PEDIDO':              self._fmt_col(df_merged, col_data_criacao),
            'DATA PREVISTA':            self._fmt_col(df_merged, col_previsao),
            'UF':                       self._fmt_col(df_merged, col_uf).str.upper(),
            'TRANSPORTADORA':           pd.Series(transp_final, index=df_merged.index),
            'PEDIDO INTELIPOST':        self._fmt_col(df_merged, col_pedido_inte),
            'CHAVE DA NF':              self._fmt_col(df_merged, col_chave_nf),
            'MARKETPLACE':              self._fmt_col(df_merged, col_canal).str.upper(),
            'N° PEDIDO':                df_merged['_PEDIDO_NORM'].astype(str),
            'NOTA FISCAL':              df_merged['_NF_NORM'].astype(str),
            'STATUS DA TRANSPORTADORA': pd.Series(status, index=df_merged.index),
        })

        # Garante presença e ordem exata das colunas finais
        for c in FINAL_COLUMNS_VALIDACAO:
            if c not in df_final.columns:
                df_final[c] = ""
        df_final = df_final[FINAL_COLUMNS_VALIDACAO]

        # Linhas descartadas pelo histórico — mesmo schema, para auditoria
        df_descartadas = pd.DataFrame({
            'DIA DA TRATATIVA':         hoje,
            'DATA PEDIDO':              self._fmt_col(df_descartadas_raw, col_data_criacao),
            'DATA PREVISTA':            self._fmt_col(df_descartadas_raw, col_previsao),
            'UF':                       self._fmt_col(df_descartadas_raw, col_uf).str.upper(),
            'TRANSPORTADORA':           self._fmt_col(df_descartadas_raw, col_transp),
            'PEDIDO INTELIPOST':        self._fmt_col(df_descartadas_raw, col_pedido_inte),
            'CHAVE DA NF':              self._fmt_col(df_descartadas_raw, col_chave_nf),
            'MARKETPLACE':              self._fmt_col(df_descartadas_raw, col_canal).str.upper(),
            'N° PEDIDO':                df_descartadas_raw['_PEDIDO_NORM'].astype(str) if '_PEDIDO_NORM' in df_descartadas_raw.columns else "",
            'NOTA FISCAL':              df_descartadas_raw['_NF_NORM'].astype(str) if '_NF_NORM' in df_descartadas_raw.columns else "",
            'STATUS DA TRANSPORTADORA': "DESCARTADA - HISTÓRICO",
        })

        if not df_descartadas.empty:
            for c in FINAL_COLUMNS_VALIDACAO:
                if c not in df_descartadas.columns:
                    df_descartadas[c] = ""
            df_descartadas = df_descartadas[FINAL_COLUMNS_VALIDACAO]
        else:
            df_descartadas = pd.DataFrame(columns=FINAL_COLUMNS_VALIDACAO)

        return (df_final, df_descartadas), None
