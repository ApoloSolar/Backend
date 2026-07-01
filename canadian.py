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
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from datetime import datetime

SEP_BASE       = os.environ.get("CANADIAN_BASE", "https://sep-api.csisolar.com")
SEP_APP_ID     = os.environ.get("CANADIAN_APP_ID", "")
SEP_APP_SECRET = os.environ.get("CANADIAN_APP_SECRET", "")

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
def buscar_realtime(serial_sn):
    """Busca o tempo real de 1 inversor.
    Retorna (ts_utc_naive, dados_dict, online_bool).
      - dados_dict: {fieldCode: valor}
      - ts_utc_naive: lastReportTime (a API entrega em UTC)
    Levanta excecao em erro de API (o coletor trata como ERRO_API)."""
    token = obter_token()
    b = _get("/open-api/device/data", {"deviceSnStr": serial_sn}, token)

    # token expirado -> reautentica uma vez
    msg = (b.get("msg") or "").lower()
    if b.get("code") in (401, 508) or "token" in msg:
        token = obter_token(forcar=True)
        b = _get("/open-api/device/data", {"deviceSnStr": serial_sn}, token)

    if b.get("code") not in (0, 200):
        raise ValueError(f"device/data Canadian erro: code={b.get('code')} msg={b.get('msg')}")

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

    # MPPTs: dv{n}=tensao(V), dc{n}=corrente(A), dp{n}=potencia(W)
    mppts = []
    for m in range(1, num_mppt + 1):
        mppts.append({
            "mppt": m,
            "tensao_v":   g(f"dv{m}"),
            "corrente_a": g(f"dc{m}"),
            "potencia_w": g(f"dp{m}"),   # ja em W
        })

    # Strings: conforme a topologia (1 por MPPT no modelo Canadian).
    # A API entrega por ENTRADA (dv/dc/dp), entao a string herda a entrada.
    strings = []
    string_num = 0
    for m in range(1, num_mppt + 1):
        qtd = topologia["strings_por_mppt"].get(m, 0)
        for _ in range(qtd):
            string_num += 1
            strings.append({
                "string_num": string_num,
                "mppt": m,
                "corrente_a": g(f"dc{m}"),
                "potencia_w": g(f"dp{m}"),
            })

    status = "ONLINE" if (campos["pac_kw"] > 0 or online) else "OFFLINE"
    return status, campos, mppts, strings
