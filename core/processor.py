import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from core.config import (
    MARKETPLACES, CARRIERS, OCCURRENCES,
    FINAL_COLUMNS, FINAL_COLUMNS_VALIDACAO
)
from utils.helpers import normalizar_nf, encontrar_coluna, carregar_arquivo

class DataProcessor:
    def __init__(self):
        self.dict_mkt_norm = {k.upper(): v for k, v in MARKETPLACES.items()}
        # Normaliza espacos multiplos -> espaco simples nas chaves do dict de
        # transportadoras. Garante que "JADLOG  SERRA 18" (2 espacos),
        # "TJB - TRANSPORTADORA  21" e similares casem com a versao canonica
        # do dict, mesmo quando o Sysemp/Intelipost trazem espacamento
        # inconsistente.
        self.dict_transp_norm = {
            ' '.join(k.upper().split()): v for k, v in CARRIERS.items()
        }
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

    @staticmethod
    def _fmt_data_br(df, col, default=""):
        """
        Formata coluna de data para padrão brasileiro.
        Entrada: '2026-03-29 12:51:46', '2026-03-29', datetime, NaT, etc.
        Saída:
            * Se houver hora não-zero -> 'DD/MM/YYYY HH:MM:SS'
            * Caso contrário          -> 'DD/MM/YYYY'
            * Vazio/inválido          -> default ("")
        """
        if col is None or col not in df.columns:
            return pd.Series([default] * len(df), index=df.index)

        # format='mixed' permite que cada linha seja parseada com o formato dela
        # (algumas com hora, outras só data). Sem isso, pandas infere pelo primeiro
        # elemento e torna NaT as linhas que não casam.
        try:
            dts = pd.to_datetime(df[col], errors='coerce', format='mixed')
        except (TypeError, ValueError):
            # Fallback para versões antigas do pandas (<2.0)
            dts = pd.to_datetime(df[col], errors='coerce')

        def _fmt(dt):
            if pd.isna(dt):
                # Tenta preservar o original quando não foi possível parsear como data
                return default
            if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
                return dt.strftime('%d/%m/%Y')
            return dt.strftime('%d/%m/%Y %H:%M:%S')

        formatted = dts.apply(_fmt)

        # Para valores não parseáveis, devolve string original (ex.: "VERIFICAR")
        original = df[col].astype(str).str.strip()
        mask_fallback = dts.isna() & original.replace({'nan': '', 'NaT': '', 'None': ''}).ne('')
        formatted = formatted.where(~mask_fallback, original)
        return formatted

    @staticmethod
    def _normalizar_transp(serie, dicionario):
        """Aplica o dicionário de transportadoras (já com chaves UPPER)."""
        upper = serie.astype(str).str.upper().str.strip()
        return upper.map(dicionario).fillna(upper)

    @staticmethod
    def _so_data(serie):
        """Mantém apenas a parte da data, descartando hora se houver (split no 1º espaço)."""
        return serie.astype(str).str.split(' ', n=1).str[0]

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
    def processar_validacao_transportadora(self, df_inteli, df_sysemp, nfs_historico, df_sys_raw=None):
        """
        Motor do fluxo "Validação de Transportadora".

        ETAPA 1 — Cruza Intelipost x Histórico/NFs em tratamento por 'Nota Fiscal'.
                   Linhas presentes no histórico são DESCARTADAS.
        ETAPA 2 — Cruza Intelipost x Sysemp pela NOTA FISCAL (chave mais
                   estável que o nº de pedido). N° PEDIDO de saída é puxado
                   do Sysemp ('Pedido Marketplace') quando disponível, com
                   fallback para o pedido normalizado da Intelipost.
                   Compara transportadora Intelipost x transportadora Sysemp
                   APÓS canonicalização pelo dicionário CARRIERS — assim
                   "JADLOG TRANSPORTES SERRA 18" e "JADLOG" são tratados
                   como a mesma transportadora:
                       match + iguais (após dict)     -> usa canonical, STATUS = 'Verdadeiro'
                       match + diferentes (após dict) -> usa Sysemp,    STATUS = 'Falso'
                       sem match                      -> mantém Intelipost, STATUS = 'Não Localizado'
        ETAPA 3 — Monta planilha final na ordem fixa de FINAL_COLUMNS_VALIDACAO.
                   DATA PEDIDO e DATA PREVISTA são exportadas SEM hora.
                   Detecção de SHOPEE é por substring (canal contém 'SHOPEE').
                   DATA PREVISTA usa coluna específica para SHOPEE
                   ('Previsão Entrega Transp. Original'); demais canais usam
                   'Previsão Entrega Cliente Original'.

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
        col_previsao_shopee = encontrar_coluna(df, [
            'Previsão Entrega Transp. Original', 'Previsao Entrega Transp. Original',
            'Previsão Entrega Transp Original', 'Previsao Entrega Transp Original',
        ])
        col_uf           = encontrar_coluna(df, ['UF', 'Estado'])
        col_transp       = encontrar_coluna(df, ['Transportadora', 'Transp'])
        col_pedido_inte  = encontrar_coluna(df, ['Pedido', 'Pedido Intelipost', 'Pedido ID'])
        col_chave_nf     = encontrar_coluna(df, ['Chave da Nota', 'Chave NF', 'Chave da NF', 'Chave NFe'])
        col_canal        = encontrar_coluna(df, ['Canal de Vendas', 'Canal de Venda'])
        col_num_pedido   = encontrar_coluna(df, ['marketplace', 'Marketplace', 'Pedido Marketplace', 'N° Pedido', 'Nº Pedido'])
        col_nf           = encontrar_coluna(df, ['Nota Fiscal', 'NF', 'Numero NF'])

        # ----- Validações obrigatórias ------------------------------------- #
        # col_num_pedido nao eh mais obrigatorio: o merge eh por NF.
        # Se ausente, N° PEDIDO de saida usa apenas o que vier do Sysemp.
        faltando = []
        if not col_nf:           faltando.append("Nota Fiscal")
        if not col_transp:       faltando.append("Transportadora")
        if faltando:
            return (None, None), (
                "Colunas obrigatórias não localizadas no Intelipost: "
                + ", ".join(faltando)
            )

        # ----- Normalizações ----------------------------------------------- #
        df['_NF_NORM']     = df[col_nf].apply(normalizar_nf)
        df['_PEDIDO_NORM'] = df[col_num_pedido].apply(self._normalizar_pedido) if col_num_pedido else ""

        # ----- ETAPA 1 — Filtro pelo histórico ----------------------------- #
        if not isinstance(nfs_historico, set):
            nfs_historico = set(nfs_historico) if nfs_historico else set()

        mask_hist = df['_NF_NORM'].isin(nfs_historico) if nfs_historico else pd.Series(False, index=df.index)
        df_descartadas_raw = df[mask_hist].copy()
        df_validas         = df[~mask_hist].copy()

        # ----- ETAPA 2 — Cruzamento Sysemp por NOTA FISCAL + validação ----- #
        # Renomeia 'Nota Fiscal' do Sysemp para '_NF_SYS_KEY' para evitar
        # colisao com a coluna do Intelipost no merge.
        df_sysemp_lookup = (
            df_sysemp[['Nota Fiscal', 'Pedido_sys', 'Transportadora_sys']]
            .copy()
            .rename(columns={'Nota Fiscal': '_NF_SYS_KEY'})
            .assign(Pedido_sys=lambda x: x['Pedido_sys'].apply(self._normalizar_pedido))
        )
        df_sysemp_lookup = df_sysemp_lookup[df_sysemp_lookup['_NF_SYS_KEY'] != ""]
        df_sysemp_lookup = df_sysemp_lookup.drop_duplicates(subset='_NF_SYS_KEY', keep='first')

        df_merged = pd.merge(
            df_validas,
            df_sysemp_lookup,
            left_on='_NF_NORM',
            right_on='_NF_SYS_KEY',
            how='left'
        )

        # Lookup adicional de N° PEDIDO contra o Sysemp BRUTO (sem o filtro
        # de empresa de tratar_sysemp). Necessario porque pedidos B2B/TIKTOK
        # direto podem estar em empresas fora do filtro [16,18,19,21] mas a
        # NF e o Pedido Marketplace existem no arquivo do Sysemp.
        # Esse lookup soh alimenta a coluna N° PEDIDO; status/transportadora
        # continuam usando o Sysemp filtrado.
        if df_sys_raw is not None and not df_sys_raw.empty:
            col_nf_full     = encontrar_coluna(df_sys_raw, ['Nota Fiscal', 'NF', 'Numero NF'])
            col_pedido_full = encontrar_coluna(df_sys_raw, ['Pedido Marketplace'])
            if col_nf_full and col_pedido_full:
                pedido_full_lookup = pd.DataFrame({
                    '_NF_FULL_KEY': df_sys_raw[col_nf_full].apply(normalizar_nf),
                    '_PEDIDO_FULL': df_sys_raw[col_pedido_full].apply(self._normalizar_pedido),
                })
                pedido_full_lookup = pedido_full_lookup[
                    (pedido_full_lookup['_NF_FULL_KEY'] != '')
                    & (pedido_full_lookup['_PEDIDO_FULL'] != '')
                ]
                pedido_full_lookup = pedido_full_lookup.drop_duplicates(
                    subset='_NF_FULL_KEY', keep='first'
                )
                df_merged = pd.merge(
                    df_merged,
                    pedido_full_lookup,
                    left_on='_NF_NORM',
                    right_on='_NF_FULL_KEY',
                    how='left',
                )

        # Comparacao usa o dicionario CARRIERS dos dois lados.
        # .map() retorna NaN quando a chave nao existe no dict — usamos isso
        # para detectar "transp nao esta no dicionario" (status Não Localizado).
        # Normaliza espacos multiplos -> espaco simples (consistente com as
        # chaves do dict_transp_norm criado em __init__) para que variantes
        # como "FRONTLOG  EXTREMA SDF 21" (2 espacos) casem corretamente.
        transp_inteli_norm = (
            df_merged[col_transp].astype(str).str.upper()
            .str.replace(r'\s+', ' ', regex=True).str.strip()
        )
        transp_sys_norm = (
            df_merged['Transportadora_sys'].astype(str).str.upper()
            .str.replace(r'\s+', ' ', regex=True).str.strip()
        )

        transp_inteli_dict = transp_inteli_norm.map(self.dict_transp_norm)
        transp_sys_dict    = transp_sys_norm.map(self.dict_transp_norm)

        inteli_in_dict = transp_inteli_dict.notna()
        sys_in_dict    = transp_sys_dict.notna()

        # Canonical para a comparacao (usa raw upper se nao tiver no dict).
        transp_inteli_canon = transp_inteli_dict.fillna(transp_inteli_norm)
        transp_sys_canon    = transp_sys_dict.fillna(transp_sys_norm)

        # Valores brutos (preservam capitalizacao original) — usados quando
        # a transportadora nao esta no dicionario e queremos manter como veio.
        transp_inteli_out = df_merged[col_transp].astype(str).str.strip()
        transp_sys_out    = df_merged['Transportadora_sys'].astype(str).str.strip()

        # 'encontrado' = NF foi localizada no Sysemp (Transportadora_sys valida).
        encontrado = (
            df_merged['Transportadora_sys'].notna()
            & (transp_sys_norm != '')
            & (transp_sys_norm != 'NAN')
        )

        # Ambas transportadoras (Intelipost E Sysemp) FORA do dicionario.
        ambos_fora_dict = encontrado & (~inteli_in_dict) & (~sys_in_dict)

        # Comparacao apos canonicalizacao (so significativa quando pelo menos
        # um lado esta no dicionario).
        iguais     = encontrado & ~ambos_fora_dict & (transp_inteli_canon == transp_sys_canon)
        diferentes = encontrado & ~ambos_fora_dict & (transp_inteli_canon != transp_sys_canon)

        # Status final:
        #   transportadora canonicas iguais (apos dict)     -> 'Verdadeiro'
        #   transportadora canonicas diferentes (apos dict) -> 'Falso'
        #   ambas fora do dicionario (ou NF nao casou)      -> 'Não Localizado'
        status = np.where(
            (~encontrado) | ambos_fora_dict, "Não Localizado",
            np.where(iguais, "Verdadeiro", "Falso")
        )

        # Transportadora final:
        #   ambos_fora_dict -> Sysemp RAW (mantem a transportadora do Sysemp)
        #   diferentes      -> Sysemp canonical (do dicionario)
        #   demais          -> Intelipost canonical (do dicionario)
        transp_final = np.select(
            [ambos_fora_dict, diferentes],
            [transp_sys_out,  transp_sys_canon],
            default=transp_inteli_canon,
        )

        # N° PEDIDO final — VLOOKUP por NF, com cadeia de fallback que
        # garante que NENHUMA linha fique sem informação:
        #   1. Sysemp BRUTO 'Pedido Marketplace' (sem filtro de empresa —
        #      cobre B2B/TIKTOK direto que ficam fora de [16,18,19,21])
        #   2. Sysemp FILTRADO 'Pedido_sys' (do merge principal)
        #   3. Intelipost 'marketplace' (_PEDIDO_NORM)
        #   4. 'NÃO INFORMADO' (trava anti-branco final)
        _NULOS = ['nan', 'NaN', 'None', '<NA>', '']
        if '_PEDIDO_FULL' in df_merged.columns:
            pedido_sys_full = (
                df_merged['_PEDIDO_FULL'].astype(str).str.strip().replace(_NULOS, pd.NA)
            )
        else:
            pedido_sys_full = pd.Series(pd.NA, index=df_merged.index)
        pedido_sys_filt = (
            df_merged['Pedido_sys'].astype(str).str.strip().replace(_NULOS, pd.NA)
        )
        pedido_int = (
            df_merged['_PEDIDO_NORM'].astype(str).str.strip().replace(_NULOS, pd.NA)
        )
        serie_pedido_final = (
            pedido_sys_full
            .fillna(pedido_sys_filt)
            .fillna(pedido_int)
            .fillna('')
        )

        # ----- ETAPA 3 — Montagem do dataframe final ----------------------- #
        hoje = datetime.now().strftime('%d/%m/%Y')

        # DATA PREVISTA por canal: SHOPEE usa 'Previsão Entrega Transp. Original';
        # demais canais usam 'Previsão Entrega Cliente Original'.
        # Detecção de Shopee é por substring (canal contém 'SHOPEE').
        canal_upper = self._fmt_col(df_merged, col_canal).str.upper()
        eh_shopee   = canal_upper.str.contains('SHOPEE', na=False, regex=False)
        previsao_geral  = self._fmt_data_br(df_merged, col_previsao)
        previsao_shopee = self._fmt_data_br(df_merged, col_previsao_shopee)
        data_prevista = previsao_geral.where(~eh_shopee, previsao_shopee)

        df_final = pd.DataFrame({
            'DIA DA TRATATIVA':         hoje,
            'DATA PEDIDO':              self._so_data(self._fmt_data_br(df_merged, col_data_criacao)),
            'DATA PREVISTA':            self._so_data(data_prevista),
            'UF':                       self._fmt_col(df_merged, col_uf).str.upper(),
            'TRANSPORTADORA':           pd.Series(transp_final, index=df_merged.index),
            'PEDIDO INTELIPOST':        self._fmt_col(df_merged, col_pedido_inte),
            'CHAVE DA NF':              self._fmt_col(df_merged, col_chave_nf),
            'MARKETPLACE':              self._fmt_col(df_merged, col_canal).str.upper(),
            'N° PEDIDO':                serie_pedido_final,
            'NOTA FISCAL':              df_merged['_NF_NORM'].astype(str),
            'STATUS DA TRANSPORTADORA': pd.Series(status, index=df_merged.index),
        })

        # Garante presença e ordem exata das colunas finais
        for c in FINAL_COLUMNS_VALIDACAO:
            if c not in df_final.columns:
                df_final[c] = ""
        df_final = df_final[FINAL_COLUMNS_VALIDACAO]

        # Linhas descartadas pelo histórico — mesmo schema, para auditoria.
        # Aplica também o dicionário de transportadora para padronizar a saída.
        transp_desc = self._normalizar_transp(
            self._fmt_col(df_descartadas_raw, col_transp),
            self.dict_transp_norm,
        )
        canal_desc = self._fmt_col(df_descartadas_raw, col_canal).str.upper()
        eh_shopee_desc = canal_desc.str.contains('SHOPEE', na=False, regex=False)
        previsao_desc_geral  = self._fmt_data_br(df_descartadas_raw, col_previsao)
        previsao_desc_shopee = self._fmt_data_br(df_descartadas_raw, col_previsao_shopee)
        data_prev_desc = previsao_desc_geral.where(~eh_shopee_desc, previsao_desc_shopee)

        df_descartadas = pd.DataFrame({
            'DIA DA TRATATIVA':         hoje,
            'DATA PEDIDO':              self._so_data(self._fmt_data_br(df_descartadas_raw, col_data_criacao)),
            'DATA PREVISTA':            self._so_data(data_prev_desc),
            'UF':                       self._fmt_col(df_descartadas_raw, col_uf).str.upper(),
            'TRANSPORTADORA':           transp_desc,
            'PEDIDO INTELIPOST':        self._fmt_col(df_descartadas_raw, col_pedido_inte),
            'CHAVE DA NF':              self._fmt_col(df_descartadas_raw, col_chave_nf),
            'MARKETPLACE':              self._fmt_col(df_descartadas_raw, col_canal).str.upper(),
            'N° PEDIDO':                (
                df_descartadas_raw['_PEDIDO_NORM'].astype(str).str.strip()
                .replace(['nan', 'NaN', 'None', '<NA>', ''], '')
                if '_PEDIDO_NORM' in df_descartadas_raw.columns else ''
            ),
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

        # ----- ETAPA 4 — Filtra DATA PREVISTA = ontem ---------------------- #
        # Mantem apenas as linhas cuja DATA PREVISTA eh igual a (hoje - 1 dia).
        # Se a coluna estiver com formato 'DD/MM/YYYY HH:MM:SS' (improvavel
        # apos _so_data), o split garante que so a parte da data eh comparada.
        ontem_str = (datetime.now() - timedelta(days=1)).strftime('%d/%m/%Y')
        df_final = df_final[
            df_final['DATA PREVISTA'].astype(str).str.split(' ', n=1).str[0] == ontem_str
        ].copy()
        df_descartadas = df_descartadas[
            df_descartadas['DATA PREVISTA'].astype(str).str.split(' ', n=1).str[0] == ontem_str
        ].copy()

        return (df_final, df_descartadas), None
