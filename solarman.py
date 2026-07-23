# -*- coding: utf-8 -*-
"""
============================================================
  ADAPTADOR SOLARMAN — Open API (globalapi.solarmanpv.com)
  para o COLETOR UNICO da Apolo Solar
============================================================
Roda DENTRO do coletor.py (mesmo processo, mesmo cron), como o
canadian.py e o huawei.py: fala com a API Solarman e traduz a resposta
para o MESMO (status, campos, mppts, strings) que a gravar_leitura() usa.

Fluxo (V1.1.x):
  1) token de USUARIO  -> POST /account/v1.0/token?appId=... (email + SHA-256)
  2) business relation -> POST /account/v1.0/info            (descobre o orgId)
  3) token de BUSINESS -> POST /account/v1.0/token + orgId
  4) dados atuais      -> POST /device/v1.0/currentData      (dataList)

O passo 2 e pulado se SOLARMAN_ORG_ID estiver definido (recomendado em
producao: economiza 1 request por ciclo).

!! ATENCAO AS UNIDADES !!
  A Solarman entrega POTENCIA EM WATTS (diferente da Canadian/CSI e da
  Huawei, que entregam kW). Entao APo_t1 e os DP{n} SAO DIVIDIDOS POR 1000.
  Ja a energia (Etdy_ge1, Et_ge0) ja vem em kWh — nao dividir.

O QUE ESTE INVERSOR **NAO** DA (ficam zerados):
  - tamb_c   (temperatura ambiente)
  - iso_kohm (resistencia de isolamento)
  - MPPT/strings: os campos DV{n}/DC{n}/DP{n} existem (4 entradas) mas o
    datalogger reporta 0.00 mesmo com o inversor gerando — falha conhecida,
    confirmada tambem no portal Solarman. O adaptador LE os campos assim
    mesmo: se a falha for corrigida, os dados passam a fluir sozinhos.
    Enquanto isso o card do dashboard oculta a secao de strings.

Usa apenas a biblioteca padrao (urllib/hashlib) — sem dependencia nova.

Variaveis de ambiente (Railway):
  SOLARMAN_APP_ID, SOLARMAN_APP_SECRET, SOLARMAN_EMAIL, SOLARMAN_PASSWORD
  SOLARMAN_ORG_ID (opcional, recomendado)
  SOLARMAN_BASE   (opcional; default globalapi = Internacional)
"""

import os
import json
import time
import hashlib
from urllib.request import Request, urlopen


def _normalizar_base(url):
    """Tolera base sem esquema ou com barra no fim."""
    u = (url or "").strip().rstrip("/")
    if not u:
        u = "https://globalapi.solarmanpv.com"
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    return u


SM_BASE     = _normalizar_base(os.environ.get("SOLARMAN_BASE",
                                              "https://globalapi.solarmanpv.com"))
SM_APP_ID   = (os.environ.get("SOLARMAN_APP_ID", "") or "").strip()
SM_SECRET   = (os.environ.get("SOLARMAN_APP_SECRET", "") or "").strip()
SM_EMAIL    = (os.environ.get("SOLARMAN_EMAIL", "") or "").strip()
SM_PASSWORD = os.environ.get("SOLARMAN_PASSWORD", "")
SM_ORG_ID   = (os.environ.get("SOLARMAN_ORG_ID", "") or "").strip() or None

# Erros transitorios que valem retry (>=500 do servidor deles)
_token_cache = {"token": None, "ts": 0, "org": None}

# Numero de entradas PV que a API expoe para este equipamento (DV1..DV4)
NUM_ENTRADAS_PV = 4


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


def _sha256(txt):
    return hashlib.sha256((txt or "").encode("utf-8")).hexdigest()


def _post(path, payload, token=None, params=None):
    url = f"{SM_BASE}{path}"
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{qs}"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"bearer {token}"
    req = Request(url, data=json.dumps(payload).encode("utf-8"),
                  headers=headers, method="POST")
    resp = urlopen(req, timeout=30)
    return json.loads(resp.read())


# ------------------------------------------------------------
# Autenticacao
# ------------------------------------------------------------
def _pedir_token(org_id=None):
    body = {"appSecret": SM_SECRET, "email": SM_EMAIL,
            "password": _sha256(SM_PASSWORD)}
    if org_id:
        body["orgId"] = org_id
    b = _post("/account/v1.0/token", body,
              params={"appId": SM_APP_ID, "language": "en"})
    tok = b.get("access_token")
    if not tok:
        raise ValueError(f"Token Solarman falhou: code={b.get('code')} "
                         f"msg={b.get('msg')}")
    return tok


def obter_token(forcar=False):
    """Token de BUSINESS (com orgId). O token vale ~2 meses, mas como o
    coletor e um processo novo a cada ciclo, cacheamos so durante a execucao."""
    if (_token_cache["token"] and not forcar
            and (time.time() - _token_cache["ts"]) < 30 * 60):
        return _token_cache["token"]

    if not SM_APP_ID or not SM_SECRET or not SM_EMAIL or not SM_PASSWORD:
        raise ValueError("SOLARMAN_APP_ID / APP_SECRET / EMAIL / PASSWORD "
                         "nao configurados.")

    org_id = SM_ORG_ID

    # 1) token de usuario (necessario para consultar a business relation)
    tok_user = _pedir_token()

    # 2) descobre o orgId, se nao veio do ambiente
    if not org_id:
        try:
            b = _post("/account/v1.0/info", {}, token=tok_user)
            orgs = b.get("orgInfoList") or []
            if orgs:
                org_id = orgs[0].get("companyId") or orgs[0].get("orgId")
        except Exception as e:
            print(f"    [solarman] aviso: business relation falhou ({e})")

    # 3) token de business (se houver organizacao)
    token = _pedir_token(org_id) if org_id else tok_user

    _token_cache.update({"token": token, "ts": time.time(), "org": org_id})
    return token


# ------------------------------------------------------------
# Leitura em tempo real
# ------------------------------------------------------------
def buscar_realtime(device_sn, tentativas=3, backoff=4):
    """Busca o currentData de 1 inversor.
    Retorna (dados_dict, online_bool):
      - dados_dict: {key: value} do dataList
    Levanta excecao so apos esgotar as tentativas."""
    ultimo = None

    for tentativa in range(1, tentativas + 1):
        token = obter_token(forcar=(tentativa == tentativas))
        try:
            b = _post("/device/v1.0/currentData", {"deviceSn": device_sn},
                      token=token)
        except Exception as e:
            ultimo = f"conexao: {e}"
            time.sleep(backoff)
            continue

        code = b.get("code")
        msg = (b.get("msg") or "")

        # token invalido/expirado -> reautentica
        if code in (401, 2101010) or "token" in str(msg).lower():
            obter_token(forcar=True)
            ultimo = f"token: {msg}"
            continue

        if b.get("success") or code in (0, None, 200):
            dados = {}
            for it in (b.get("dataList") or []):
                dados[it.get("key")] = it.get("value")
            # deviceState: 1=online, 2=alerting, 3=offline
            estado = b.get("deviceState")
            online = (estado == 1) if estado is not None else None
            return dados, online

        # erro do servidor -> repete
        if isinstance(code, int) and code >= 500:
            ultimo = f"transitorio: code={code} msg={msg}"
            print(f"    [solarman] {device_sn}: {ultimo} "
                  f"(tentativa {tentativa}/{tentativas}) — repetindo...")
            time.sleep(backoff)
            continue

        raise ValueError(f"currentData Solarman erro: code={code} msg={msg}")

    raise ValueError(f"currentData Solarman falhou apos {tentativas} "
                     f"tentativas ({ultimo})")


# ------------------------------------------------------------
# Traducao para o formato do coletor
# ------------------------------------------------------------
def extrair_dados(dados, topologia, online=None):
    """Traduz o dataList da Solarman nos MESMOS campos/canais do coletor.
    Retorna (status, campos, mppts, strings).

    UNIDADES: APo_t1 e DP{n} vem em WATTS -> divididos por 1000.
              Etdy_ge1 e Et_ge0 ja vem em kWh -> nao dividir."""
    g = lambda k: _safe_float(dados.get(k))

    # Potencia CC total = soma das entradas PV (em W -> kW).
    # Hoje da 0.0 (datalogger nao reporta DV/DC/DP), mas se a falha for
    # corrigida o valor passa a aparecer sem mudar nada aqui.
    pdc_w = sum(g(f"DP{n}") for n in range(1, NUM_ENTRADAS_PV + 1))

    campos = {
        "pac_kw":     g("APo_t1") / 1000.0,   # W  -> kW
        "dyield_kwh": g("Etdy_ge1"),          # ja em kWh
        "tyield_kwh": g("Et_ge0"),            # ja em kWh
        "freq_hz":    g("A_Fo1"),             # Hz
        "tmod_c":     g("T_in1"),             # temperatura interna (C)
        "tamb_c":     0.0,                    # nao existe
        "iso_kohm":   0.0,                    # nao existe
        "pdc_kw":     pdc_w / 1000.0,         # W  -> kW
    }

    # ---- MPPTs / strings ----
    # A API expoe 4 entradas (DV/DC/DP 1..4). Elas SAO lidas, mas o
    # datalogger reporta 0.00 (falha conhecida). O dashboard oculta a
    # secao de strings quando nao ha dado — ver index.html.
    num_mppt = min(topologia.get("num_mppt", NUM_ENTRADAS_PV), NUM_ENTRADAS_PV)
    mppts, strings = [], []
    string_num = 0
    for m in range(1, num_mppt + 1):
        u = g(f"DV{m}")
        i = g(f"DC{m}")
        p = g(f"DP{m}")
        mppts.append({"mppt": m, "tensao_v": u, "corrente_a": i,
                      "potencia_w": p})
        # 1 string por entrada
        for _ in range(topologia.get("strings_por_mppt", {}).get(m, 1)):
            string_num += 1
            strings.append({"string_num": string_num, "mppt": m,
                            "corrente_a": i, "potencia_w": p})

    if online is None:
        online = campos["pac_kw"] > 0
    status = "ONLINE" if (campos["pac_kw"] > 0 or online) else "OFFLINE"
    return status, campos, mppts, strings
