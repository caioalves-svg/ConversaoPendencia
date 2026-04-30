"""
Testes do módulo "Validação de Transportadora".

Como rodar (a partir da raiz do repositório, depois de aplicar o patch):
    pip install pytest
    pytest tests/test_validacao_transportadora.py -v
"""
import pandas as pd
import pytest

from core.processor import DataProcessor
from core.config import FINAL_COLUMNS_VALIDACAO


# --------------------------------------------------------------------------- #
# Helpers de fixture
# --------------------------------------------------------------------------- #
def _df_intelipost(linhas):
    """Cria um DataFrame Intelipost mínimo nos nomes esperados pelo pipeline."""
    cols = [
        "Data Criação", "Previsão Entrega Cliente Original", "UF",
        "Transportadora", "Pedido", "Chave da Nota",
        "Canal de Vendas", "marketplace", "Nota Fiscal",
    ]
    return pd.DataFrame(linhas, columns=cols)


def _df_sysemp_tratado(linhas):
    """Equivalente ao DataFrame retornado por DataProcessor.tratar_sysemp()."""
    cols = ["Nota Fiscal", "Chave NF_sys", "Pedido_sys", "UF_sys",
            "Marketplace_sys", "Transportadora_sys"]
    return pd.DataFrame(linhas, columns=cols)


@pytest.fixture
def processor():
    return DataProcessor()


# --------------------------------------------------------------------------- #
# Testes
# --------------------------------------------------------------------------- #
def test_caminho_feliz_status_verdadeiro_e_falso(processor):
    df_inteli = _df_intelipost([
        ["01/04/2026", "10/04/2026", "SP", "JADLOG", "PED-1", "CHV1", "MERCADO LIVRE", "ML-100", "12345"],
        ["02/04/2026", "11/04/2026", "RJ", "Total",  "PED-2", "CHV2", "SHOPEE",        "SH-200", "12346"],
    ])
    df_sys = _df_sysemp_tratado([
        ["12345", "CHV1", "ML-100", "SP", "MERCADO LIVRE", "JADLOG"],   # igual -> Verdadeiro
        ["12346", "CHV2", "SH-200", "RJ", "SHOPEE",        "PATRUS"],   # diferente -> Falso, troca p/ PATRUS
    ])
    (df_final, df_desc), err = processor.processar_validacao_transportadora(df_inteli, df_sys, set())

    assert err is None
    assert list(df_final.columns) == FINAL_COLUMNS_VALIDACAO
    assert len(df_final) == 2
    assert df_desc.empty

    row1 = df_final[df_final["NOTA FISCAL"] == "12345"].iloc[0]
    assert row1["STATUS DA TRANSPORTADORA"] == "Verdadeiro"
    assert row1["TRANSPORTADORA"] == "JADLOG"

    row2 = df_final[df_final["NOTA FISCAL"] == "12346"].iloc[0]
    assert row2["STATUS DA TRANSPORTADORA"] == "Falso"
    assert row2["TRANSPORTADORA"] == "PATRUS"


def test_descarta_nfs_presentes_no_historico(processor):
    df_inteli = _df_intelipost([
        ["01/04/2026", "10/04/2026", "SP", "JADLOG", "PED-1", "CHV1", "ML", "ML-100", "12345"],
        ["02/04/2026", "11/04/2026", "RJ", "TOTAL",  "PED-2", "CHV2", "SH", "SH-200", "12346"],
    ])
    df_sys = _df_sysemp_tratado([
        ["12346", "CHV2", "SH-200", "RJ", "SHOPEE", "TOTAL"],
    ])
    nfs_hist = {"12345"}

    (df_final, df_desc), err = processor.processar_validacao_transportadora(df_inteli, df_sys, nfs_hist)

    assert err is None
    assert len(df_final) == 1
    assert df_final.iloc[0]["NOTA FISCAL"] == "12346"
    assert len(df_desc) == 1
    assert df_desc.iloc[0]["NOTA FISCAL"] == "12345"
    assert df_desc.iloc[0]["STATUS DA TRANSPORTADORA"] == "DESCARTADA - HISTÓRICO"


def test_pedido_ausente_marca_nao_localizado(processor):
    df_inteli = _df_intelipost([
        ["01/04/2026", "10/04/2026", "SP", "JADLOG", "PED-1", "CHV1", "ML", "ML-999", "99999"],
    ])
    df_sys = _df_sysemp_tratado([
        ["12346", "CHV2", "SH-200", "RJ", "SHOPEE", "TOTAL"],
    ])
    (df_final, _), err = processor.processar_validacao_transportadora(df_inteli, df_sys, set())

    assert err is None
    assert len(df_final) == 1
    assert df_final.iloc[0]["STATUS DA TRANSPORTADORA"] == "Não Localizado"
    assert df_final.iloc[0]["TRANSPORTADORA"] == "JADLOG"  # mantém Intelipost


def test_transportadora_sysemp_vazia_trata_como_nao_localizado(processor):
    df_inteli = _df_intelipost([
        ["01/04/2026", "10/04/2026", "SP", "JADLOG", "PED-1", "CHV1", "ML", "ML-100", "12345"],
    ])
    df_sys = _df_sysemp_tratado([
        ["12345", "CHV1", "ML-100", "SP", "MERCADO LIVRE", ""],
    ])
    (df_final, _), err = processor.processar_validacao_transportadora(df_inteli, df_sys, set())

    assert err is None
    assert df_final.iloc[0]["STATUS DA TRANSPORTADORA"] == "Não Localizado"


def test_pedido_duplicado_no_sysemp_usa_primeiro_match(processor):
    df_inteli = _df_intelipost([
        ["01/04/2026", "10/04/2026", "SP", "JADLOG", "PED-1", "CHV1", "ML", "ML-100", "12345"],
    ])
    df_sys = _df_sysemp_tratado([
        ["12345", "CHV1", "ML-100", "SP", "MERCADO LIVRE", "JADLOG"],
        ["12345", "CHV1", "ML-100", "SP", "MERCADO LIVRE", "TOTAL"],
    ])
    (df_final, _), err = processor.processar_validacao_transportadora(df_inteli, df_sys, set())

    assert err is None
    # Mantém a primeira ocorrência -> JADLOG -> Verdadeiro
    assert len(df_final) == 1
    assert df_final.iloc[0]["STATUS DA TRANSPORTADORA"] == "Verdadeiro"


def test_falta_coluna_obrigatoria_retorna_erro(processor):
    # Intelipost sem coluna 'marketplace'
    df_inteli = pd.DataFrame([{
        "Data Criação": "01/04/2026", "Previsão Entrega Cliente Original": "10/04/2026",
        "UF": "SP", "Transportadora": "JADLOG", "Pedido": "PED-1",
        "Chave da Nota": "CHV1", "Canal de Vendas": "ML", "Nota Fiscal": "12345"
    }])
    df_sys = _df_sysemp_tratado([
        ["12345", "CHV1", "ML-100", "SP", "MERCADO LIVRE", "JADLOG"],
    ])
    (df_final, df_desc), err = processor.processar_validacao_transportadora(df_inteli, df_sys, set())

    assert df_final is None
    assert "marketplace" in (err or "").lower()


def test_ordem_e_presenca_de_todas_as_colunas_finais(processor):
    df_inteli = _df_intelipost([
        ["01/04/2026", "10/04/2026", "SP", "JADLOG", "PED-1", "CHV1", "MERCADO LIVRE", "ML-100", "12345"],
    ])
    df_sys = _df_sysemp_tratado([
        ["12345", "CHV1", "ML-100", "SP", "MERCADO LIVRE", "JADLOG"],
    ])
    (df_final, _), err = processor.processar_validacao_transportadora(df_inteli, df_sys, set())

    assert err is None
    # Ordem fixa esperada
    assert list(df_final.columns) == [
        'DIA DA TRATATIVA', 'DATA PEDIDO', 'DATA PREVISTA', 'UF',
        'TRANSPORTADORA', 'PEDIDO INTELIPOST', 'CHAVE DA NF',
        'MARKETPLACE', 'N° PEDIDO', 'NOTA FISCAL', 'STATUS DA TRANSPORTADORA',
    ]


def test_normalizacao_de_pedido_com_sufixo_ponto_zero(processor):
    """Excel costuma exportar números como 12345.0 — deve casar com 12345 do Sysemp."""
    df_inteli = _df_intelipost([
        ["01/04/2026", "10/04/2026", "SP", "JADLOG", "PED-1", "CHV1", "ML", "12345.0", "12345"],
    ])
    df_sys = _df_sysemp_tratado([
        ["12345", "CHV1", "12345", "SP", "MERCADO LIVRE", "JADLOG"],
    ])
    (df_final, _), err = processor.processar_validacao_transportadora(df_inteli, df_sys, set())

    assert err is None
    assert df_final.iloc[0]["STATUS DA TRANSPORTADORA"] == "Verdadeiro"
    assert df_final.iloc[0]["N° PEDIDO"] == "12345"
