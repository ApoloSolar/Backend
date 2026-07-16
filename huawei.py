# -*- coding: utf-8 -*-
"""
============================================================
  ADAPTADOR HUAWEI — FusionSolar / SmartPVMS (Northbound)
  para o COLETOR UNICO da Apolo Solar
============================================================
Roda DENTRO do coletor.py (mesmo processo, mesmo cron). Nao e um
coletor separado — apenas a parte que sabe falar com a API Northbound
e traduzir a resposta para o MESMO formato que a gravar_leitura() usa
(campos / mppts / strings), igual ao canadian.py.

Fluxo:
  obter_token()        -> POST /thirdData/login  (XSRF-TOKEN vem no HEADER)
  precarregar(dev_ids) -> POST /thirdData/getDevRealKpi  EM LOTE (1 chamada)
  get_dev(dev_id)      -> le do cache do ciclo
  extrair_dados(...)   -> traduz o dataItemMap -> campos/mppts/strings

POR QUE O LOTE:
  A Huawei tem flow control por numero de dispositivos a cada 5 min, e o
  getDevRealKpi aceita ate 100 dispositivos do MESMO devTypeId por chamada.
  Chamar 1x por inversor (8 chamadas/ciclo) desperdicaria cota e arrisca o
  failCode 407. Entao o coletor pre-carrega TODOS os Huawei numa chamada so
  e o laco apenas le do cache.

LIMITES (conta de API Northbound):
  - login: 5x a cada 10 min (excedeu = 407; 5 senhas erradas = 30 min bloqueado)
  - token: vale 30 min e renova sozinho enquanto usado; 1 sessao por conta
  - getDevRealKpi: intervalo minimo de 5 min

O QUE A HUAWEI **NAO** DA (ficam zerados):
  - tamb_c   (temperatura ambiente — so via sensor EMI, devTypeId 10)
  - iso_kohm (resistencia de isolamento — nao existe no getDevRealKpi)
  - timestamp da leitura: a resposta traz so devId/sn/dataItemMap. Quem
    carimba a hora e o coletor (relogio local), como slot de 5 min.

Usa apenas a biblioteca padrao (urllib) — sem dependencia nova.

Variaveis de ambiente (Railway):
  HUAWEI_USER, HUAWEI_SYSTEM_CODE   (senha vai em systemCode!)
  HUAWEI_BASE (opcional; default la5 = Brasil/LATAM)
"""

import os
import json
import time
from urllib.request import Request, urlopen
from datetime import datetime

HW_BASE        = os.environ.get("HUAWEI_BASE", "https://la5.fusionsolar.huawei.com")
HW_USER        = os.environ.get("HUAWEI_USER", "")
HW_SYSTEM_CODE = os.environ.get("HUAWEI_SYSTEM_CODE", "")

# devTypeId 1 = inversor string (SUN2000-250KTL-H1 e desta familia)
DEV_TYPE_INVERSOR = 1

# Estados de rede do campo inverter_state que contam como "gerando".
# 512 = conectado a rede, 513 = conectado com potencia limitada,
# 514 = conectado em auto-derating.
ESTADOS_ONLINE = {512, 513, 514}

# token e cache de leitura, validos durante UMA execucao do coletor
_token_cache = {"token": None, "ts": 0}
_dados_cache = {}          # {str(devId): dataItemMap}


# ------------------------------------------------------------
# Auxiliares
# ------------------------------------------------------------
def _safe_float(v):
    """'', '--', None -> 0.0 (mesma semantica do safe_float do coletor).
    A Huawei devolve string vazia quando o inversor esta offline."""
    try:
        if v in (None, "", "--"):
            return 0.0
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def _post(path, payload, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["XSRF-TOKEN"] = token
    req = Request(f"{HW_BASE}{path}", data=json.dumps(payload).encode("utf-8"),
                  headers=headers, method="POST")
    resp = urlopen(req, timeout=30)
    corpo = json.loads(resp.read())
    return corpo, resp.headers


# ------------------------------------------------------------
# Autenticacao
# ------------------------------------------------------------
def obter_token(forcar=False):
    """Faz login e devolve o XSRF-TOKEN (que vem no HEADER, nao no corpo).
    Reaproveita o token por 25 min (validade real: 30)."""
    if (_token_cache["token"] and not forcar
            and (time.time() - _token_cache["ts"]) < 25 * 60):
        return _token_cache["token"]

    if not HW_USER or not HW_SYSTEM_CODE:
        raise ValueError("HUAWEI_USER / HUAWEI_SYSTEM_CODE nao configurados.")

    # ATENCAO: a senha vai no campo systemCode (nao 'password').
    corpo, headers = _post("/thirdData/login",
                           {"userName": HW_USER, "systemCode": HW_SYSTEM_CODE})
    if not corpo.get("success"):
        raise ValueError(f"Login Huawei falhou: failCode={corpo.get('failCode')} "
                         f"msg={corpo.get('message')}")

    token = headers.get("XSRF-TOKEN") or headers.get("xsrf-token")
    if not token:
        raise ValueError("Login Huawei OK, mas sem XSRF-TOKEN no header.")

    _token_cache["token"] = token
    _token_cache["ts"] = time.time()
    return token


# ------------------------------------------------------------
# Leitura em tempo real (EM LOTE)
# ------------------------------------------------------------
def _get_real_kpi(dev_ids, dev_type_id, tentativas=3, backoff=5):
    """Chama getDevRealKpi para uma lista de devIds (mesmo devTypeId).
    Trata token expirado (305/401) e flow control (407)."""
    ids = ",".join(str(d) for d in dev_ids)
    ultimo = None

    for tentativa in range(1, tentativas + 1):
        token = obter_token(forcar=(tentativa == tentativas))
        try:
            corpo, _ = _post("/thirdData/getDevRealKpi",
                             {"devIds": ids, "devTypeId": dev_type_id}, token)
        except Exception as e:
            ultimo = f"conexao: {e}"
            time.sleep(backoff)
            continue

        fail = corpo.get("failCode")

        # token expirado/invalido -> reautentica e repete
        if fail in (305, 401):
            obter_token(forcar=True)
            ultimo = f"token: failCode={fail}"
            continue

        # flow control -> espera e repete
        if fail == 407:
            ultimo = "407 flow control"
            print(f"    [huawei] 407 flow control (tentativa {tentativa}/"
                  f"{tentativas}) — aguardando {backoff}s...")
            time.sleep(backoff)
            continue

        if corpo.get("success") or fail in (0, None):
            return corpo.get("data") or []

        raise ValueError(f"getDevRealKpi erro: failCode={fail} "
                         f"msg={corpo.get('message')}")

    raise ValueError(f"getDevRealKpi falhou apos {tentativas} tentativas ({ultimo})")


def precarregar(dev_ids, dev_type_id=DEV_TYPE_INVERSOR):
    """Busca TODOS os inversores Huawei numa unica chamada (ate 100 por vez)
    e guarda no cache do ciclo. Chamar UMA vez, antes do laco do coletor.

    Retorna quantos dispositivos vieram com dados."""
    _dados_cache.clear()
    ids = [str(d) for d in dev_ids if d]
    if not ids:
        return 0

    for i in range(0, len(ids), 100):        # limite: 100 dispositivos/chamada
        lote = ids[i:i + 100]
        for item in _get_real_kpi(lote, dev_type_id):
            dev_id = str(item.get("devId"))
            _dados_cache[dev_id] = item.get("dataItemMap") or {}

    return len(_dados_cache)


def get_dev(dev_id):
    """Le do cache do ciclo. Retorna (dados_dict, online_bool).
    dados vazio = inversor nao veio na resposta (ou veio sem dataItemMap)."""
    dados = _dados_cache.get(str(dev_id))
    if not dados:
        return {}, False

    # run_state: 1 = online. inverter_state 512/513/514 = conectado a rede.
    run = _safe_float(dados.get("run_state"))
    est = _safe_float(dados.get("inverter_state"))
    online = (run == 1) or (int(est) in ESTADOS_ONLINE if est else False)
    return dados, online


def tem_dados(dev_id):
    """True se o inversor veio com algum dado neste ciclo."""
    return bool(_dados_cache.get(str(dev_id)))


# ------------------------------------------------------------
# Traducao para o formato do coletor
# ------------------------------------------------------------
def extrair_dados(dados, topologia, online=None):
    """Traduz o dataItemMap da Huawei nos MESMOS campos/canais do coletor.
    Retorna (status, campos, mppts, strings) — identico ao extrair_dados
    da Chint, para a mesma gravar_leitura().

    Unidades: active_power ja vem em kW; day_cap/total_cap ja em kWh;
    mppt_power ja em kW. NAO dividir por 1000.

    tamb_c e iso_kohm ficam 0.0: a Huawei nao reporta esses dois no
    inversor (temperatura ambiente so via EMI; isolamento nao existe)."""
    num_mppt = topologia["num_mppt"]
    g = lambda k: _safe_float(dados.get(k))

    campos = {
        "pac_kw":     g("active_power"),   # Potencia ativa (kW)
        "dyield_kwh": g("day_cap"),        # Geracao do dia (kWh)
        "tyield_kwh": g("total_cap"),      # Geracao total (kWh)
        "freq_hz":    g("elec_freq"),      # Frequencia da rede (Hz)
        "tmod_c":     g("temperature"),    # Temperatura interna ~ Tmod (C)
        "tamb_c":     0.0,                 # nao existe no inversor Huawei
        "iso_kohm":   0.0,                 # nao existe no getDevRealKpi
        "pdc_kw":     g("mppt_power"),     # Potencia CC total de entrada (kW)
    }

    # ---- Strings PV: a Huawei da TENSAO e CORRENTE por string ----
    # pv{n}_u (V) e pv{n}_i (A), numeradas 1..num_string de forma sequencial.
    # A filiacao string->MPPT vem da topologia do banco. No SUN2000-250KTL-H1
    # sao 6 MPPTs com 4/5/5/4/5/5 strings = 28 (conforme datasheet).
    strings = []
    tensao_do_mppt = {}          # MPPT -> tensao (strings do mesmo MPPT sao paralelas)
    string_num = 0
    for m in range(1, num_mppt + 1):
        qtd = topologia["strings_por_mppt"].get(m, 0)
        for _ in range(qtd):
            string_num += 1
            u = g(f"pv{string_num}_u")
            i = g(f"pv{string_num}_i")
            # tensao do MPPT = a da 1a string dele que tenha tensao valida
            if m not in tensao_do_mppt and u > 0:
                tensao_do_mppt[m] = u
            strings.append({
                "string_num": string_num,
                "mppt": m,
                "corrente_a": i,
                "potencia_w": u * i,     # tensao REAL da string (melhor que Chint)
            })

    # ---- MPPTs: derivados das strings ----
    # A Huawei NAO reporta tensao/corrente por MPPT (so energia acumulada em
    # mppt_{n}_cap, kWh). Entao: tensao = a da string-filha (paralelas => mesma
    # tensao) e a corrente vira a SOMA das filhas via
    # recompor_corrente_mppt_por_strings() no coletor.
    mppts = []
    for m in range(1, num_mppt + 1):
        v = tensao_do_mppt.get(m, 0.0)
        mppts.append({
            "mppt": m,
            "tensao_v": v,
            "corrente_a": 0.0,      # recomposto pelo coletor (soma das strings)
            "potencia_w": 0.0,      # idem (V * I)
        })

    if online is None:
        online = campos["pac_kw"] > 0
    status = "ONLINE" if (campos["pac_kw"] > 0 or online) else "OFFLINE"
    return status, campos, mppts, strings
