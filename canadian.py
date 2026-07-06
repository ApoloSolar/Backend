# -*- coding: utf-8 -*-
"""
============================================================
  ADAPTADOR CANADIAN — Smart Energy Platform (CSI Solar)
  para o COLETOR UNICO da Apolo Solar
============================================================
Roda DENTRO do coletor.py (mesmo processo, mesmo cron). Nao e
um coletor separado — apenas a parte que sabe falar com a API
Smart Energy e traduzir a resposta para o MESMO formato que a
gravar_leitura() ja usa (campos / mppts / strings).

Fluxo:
  obter_token()          -> POST /open-api/user/authority (appId+appSecret)
  buscar_realtime(sn)    -> GET  /open-api/device/data?deviceSnStr=SN
  extrair_dados(...)     -> traduz fieldCodes -> campos/mppts/strings

Usa apenas a biblioteca padrao (urllib), igual ao coletor.py —
nao adiciona dependencia nova.

Variaveis de ambiente (Railway):
  CANADIAN_APP_ID, CANADIAN_APP_SECRET
  CANADIAN_BASE (opcional; default producao internacional)
"""

import os
import json
import time
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from datetime import datetime

SEP_BASE       = os.environ.get("CANADIAN_BASE", "https://sep-api.csisolar.com")
SEP_APP_ID     = os.environ.get("CANADIAN_APP_ID", "")
SEP_APP_SECRET = os.environ.get("CANADIAN_APP_SECRET", "")

# Erros TRANSITORIOS do backend da Canadian: nao sao falha do request,
# valem retry. 602 = "redis error" (cache deles caiu momentaneamente).
CODIGOS_TRANSITORIOS = {602}

# token reaproveitado durante UMA execucao do coletor
_token_cache = {"token": None}


# ------------------------------------------------------------
# Auxiliares
# ------------------------------------------------------------
def _safe_float(v):
    """'', '--', None -> 0.0 (mesma semantica do safe_float do coletor)."""
    try:
        if v in (None, "", "--"):
            return 0.0
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def _post(path, payload, token=None):
    headers = {"Content-Type": "application/json", "Accept-Language": "pt-BR"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = Request(f"{SEP_BASE}{path}", data=json.dumps(payload).encode("utf-8"),
                  headers=headers, method="POST")
    resp = urlopen(req, timeout=20)
    return json.loads(resp.read())


def _get(path, params, token):
    headers = {"Authorization": f"Bearer {token}", "Accept-Language": "pt-BR"}
    req = Request(f"{SEP_BASE}{path}?{urlencode(params)}", headers=headers)
    resp = urlopen(req, timeout=20)
    return json.loads(resp.read())


# ------------------------------------------------------------
# Autenticacao
# ------------------------------------------------------------
def obter_token(forcar=False):
    if _token_cache["token"] and not forcar:
        return _token_cache["token"]
    if not SEP_APP_ID or not SEP_APP_SECRET:
        raise ValueError("CANADIAN_APP_ID / CANADIAN_APP_SECRET nao configurados.")
    b = _post("/open-api/user/authority",
              {"appId": SEP_APP_ID, "appSecret": SEP_APP_SECRET})
    if b.get("code") not in (0, 200):
        raise ValueError(f"Auth Canadian falhou: code={b.get('code')} msg={b.get('msg')}")
    _token_cache["token"] = b["data"]["accessToken"]
    return _token_cache["token"]


# ------------------------------------------------------------
# Leitura em tempo real
# ------------------------------------------------------------
def buscar_realtime(serial_sn, tentativas=3, backoff=3):
    """Busca o tempo real de 1 inversor, COM RETRY em erros transitorios
    do servidor da Canadian (ex.: code 602 'redis error', 5xx).
    Retorna (ts_utc_naive, dados_dict, online_bool).
      - dados_dict: {fieldCode: valor}
      - ts_utc_naive: lastReportTime (a API entrega em UTC)
    Levanta excecao so depois de esgotar as tentativas, ou em erro
    definitivo (parametro invalido etc.). O coletor trata como ERRO_API."""
    ultimo_erro = None

    for tentativa in range(1, tentativas + 1):
        # na ultima tentativa, forca um token novo (caso o cache/token
        # esteja preso num no problematico do backend deles)
        token = obter_token(forcar=(tentativa == tentativas))

        try:
            b = _get("/open-api/device/data", {"deviceSnStr": serial_sn}, token)
        except Exception as e:
            ultimo_erro = f"conexao: {e}"
            time.sleep(backoff)
            continue

        code = b.get("code")
        msg  = (b.get("msg") or "")

        # token expirado/invalido -> reautentica e tenta de novo
        if code in (401, 508) or "token" in msg.lower():
            obter_token(forcar=True)
            ultimo_erro = f"token: code={code} msg={msg}"
            continue

        # sucesso
        if code in (0, 200):
            lst = b.get("data") or []
            if not lst:
                return None, {}, False
            dev = lst[0]

            dados = {}
            for f in dev.get("realData", []):
                dados[f.get("fieldCode")] = f.get("data")

            ts = None
            lrt = dev.get("lastReportTime")
            if lrt:
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                    try:
                        ts = datetime.strptime(str(lrt).strip(), fmt)
                        break
                    except (ValueError, TypeError):
                        pass

            online = dev.get("status") == 1
            return ts, dados, online

        # erro TRANSITORIO do servidor (redis 602, 5xx) -> espera e repete
        if code in CODIGOS_TRANSITORIOS or (isinstance(code, int) and code >= 500):
            ultimo_erro = f"transitorio: code={code} msg={msg}"
            print(f"    [canadian] {serial_sn}: {ultimo_erro} "
                  f"(tentativa {tentativa}/{tentativas}) — repetindo...")
            time.sleep(backoff)
            continue

        # erro DEFINITIVO (ex.: parametro invalido) -> nao adianta repetir
        raise ValueError(f"device/data Canadian erro: code={code} msg={msg}")

    raise ValueError(f"device/data Canadian falhou apos {tentativas} tentativas "
                     f"({ultimo_erro})")


# ------------------------------------------------------------
# Traducao para o formato do coletor
# ------------------------------------------------------------
def _freq(dados):
    """Frequencia: usa a do sistema se vier > 0; senao a media das fases
    (na captura, grd_fre veio 0 e a_grd_fre/b/c vieram 60)."""
    v = _safe_float(dados.get("grd_fre"))
    if v > 0:
        return v
    fases = [_safe_float(dados.get(k)) for k in ("a_grd_fre", "b_grd_fre", "c_grd_fre")]
    fases = [x for x in fases if x > 0]
    return sum(fases) / len(fases) if fases else 0.0


def extrair_dados(dados, topologia, online=None):
    """Traduz o realData da Canadian nos MESMOS campos/canais do coletor.
    Retorna (status, campos, mppts, strings) — identico ao extrair_dados
    da Chint, para a mesma gravar_leitura().

    Unidades: ap_all/elec_day/elec_all/dp_all ja vem em kW/kWh (NAO dividir);
    dp{n} ja vem em W. tamb ignorado (nao existe no inversor)."""
    num_mppt = topologia["num_mppt"]
    g = lambda k: _safe_float(dados.get(k))

    campos = {
        "pac_kw":     g("ap_all"),     # Potencia Ativa Total (kW)
        "dyield_kwh": g("elec_day"),   # Producao diaria (kWh)
        "tyield_kwh": g("elec_all"),   # Producao total (kWh)
        "freq_hz":    _freq(dados),    # Frequencia da rede (Hz)
        "tmod_c":     g("inv_temp"),   # Temperatura interna ~ Tmod (C)
        "tamb_c":     0.0,             # ignorado (sem fonte no inversor)
        "iso_kohm":   g("ins_res"),    # Resistencia de isolamento (kOhm)
        "pdc_kw":     g("dp_all"),     # Potencia CC total (kW)
    }

    # A API Canadian entrega por ENTRADA (dv/dc/dp{n}), UMA POR STRING —
    # ex.: 12 MPPT x 2 strings = 24 entradas dv1..dv24. As duas strings de
    # um mesmo MPPT compartilham a tensao. Percorre a topologia do banco
    # (quantas strings cada MPPT tem) casando cada string com a proxima
    # entrada; o MPPT recebe tensao da string e corrente/potencia = SOMA.
    strings = []
    entrada    = 0   # indice global da entrada na API (1..num_string)
    string_num = 0
    i_por_mppt = {}  # soma de corrente por MPPT
    p_por_mppt = {}  # soma de potencia por MPPT
    v_por_mppt = {}  # tensao do MPPT (compartilhada pelas strings)
    for m in range(1, num_mppt + 1):
        qtd = topologia["strings_por_mppt"].get(m, 0)
        for _ in range(qtd):
            entrada    += 1
            string_num += 1
            v = g(f"dv{entrada}")
            i = g(f"dc{entrada}")
            # dp{n} da API e a potencia do MPPT INTEIRO, nao da string —
            # usa-la aqui dobra o valor. Potencia da string = tensao do
            # MPPT x corrente da string. A potencia do MPPT vira a soma.
            p = v * i
            strings.append({
                "string_num": string_num,
                "mppt": m,
                "corrente_a": i,
                "potencia_w": p,
            })
            i_por_mppt[m] = i_por_mppt.get(m, 0.0) + i
            p_por_mppt[m] = p_por_mppt.get(m, 0.0) + p
            if v > 0:
                v_por_mppt[m] = v

    # MPPTs: tensao das strings (compartilhada); corrente/potencia = SOMA.
    mppts = []
    for m in range(1, num_mppt + 1):
        mppts.append({
            "mppt": m,
            "tensao_v":   v_por_mppt.get(m, 0.0),
            "corrente_a": i_por_mppt.get(m, 0.0),
            "potencia_w": p_por_mppt.get(m, 0.0),   # ja em W
        })

    status = "ONLINE" if (campos["pac_kw"] > 0 or online) else "OFFLINE"
    return status, campos, mppts, strings
