# -*- coding: utf-8 -*-
"""
============================================================
  ADAPTADOR SOLIS — SolisCloud Platform API
  para o COLETOR UNICO da Apolo Solar
============================================================
Roda DENTRO do coletor.py, como canadian.py / huawei.py / solarman.py.

!! AUTENTICACAO DIFERENTE DE TODAS AS OUTRAS !!
  Nao ha token nem login. CADA requisicao e assinada com HMAC-SHA1:
    Authorization = "API " + KeyId + ":" + Sign
    Sign = base64(HmacSHA1(KeySecret,
             "POST\\n" + Content-MD5 + "\\n" + Content-Type + "\\n"
             + Date + "\\n" + CanonicalizedResource))
  Validado contra o exemplo da documentacao oficial (o Content-MD5
  calculado bate com o do manual).

!! ATENCAO AS UNIDADES — A ARMADILHA DESTA API !!
  A Solis muda a UNIDADE conforme a grandeza do numero:
      eToday = 231.1  kWh
      eMonth = 25.063 MWh
      eTotal = 1.267  GWh   <-- ler cru daria 1,267 kWh!
  Cada campo X tem um companheiro XStr com a unidade. O adaptador LE
  essa unidade e normaliza tudo para kWh. Sem isso, a geracao total sai
  um milhao de vezes menor.

!! ASSOCIACAO STRING -> MPPT !!
  Os campos mpptUpv1..20 / mpptIpv1..20 / mpptPow1..20 vem TODOS ZERADOS
  neste equipamento. Os dados bons estao por STRING: uPv1..28 / iPv1..28
  / pow1..28. As strings de um mesmo MPPT estao em PARALELO, entao
  compartilham a tensao — e nos dados reais elas aparecem em pares
  consecutivos (uPv1=uPv2, uPv3=uPv4, ...), formando 14 MPPTs x 2 strings.
  O agrupamento usa a TOPOLOGIA DO BANCO (modelo_mppt), nao a tensao em
  tempo real: agrupar por tensao quebraria a noite (tudo zero) e quando
  dois MPPTs coincidissem no mesmo valor.
  Validacao: a soma das 28 strings deu 85,99 kW contra 86,44 kW do dcPac
  reportado pela propria API (0,5% de diferenca).

Usa apenas biblioteca padrao (urllib/hmac/hashlib) — sem dependencia nova.

Variaveis de ambiente (Railway):
  SOLIS_KEY_ID, SOLIS_KEY_SECRET
  SOLIS_BASE (opcional; default https://www.soliscloud.com:13333)
"""

import os
import json
import time
import hmac
import base64
import hashlib
from urllib.request import Request, urlopen
from email.utils import formatdate
from datetime import datetime


def _normalizar_base(url):
    u = (url or "").strip().rstrip("/")
    if not u:
        u = "https://www.soliscloud.com:13333"
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    return u


SOLIS_BASE   = _normalizar_base(os.environ.get("SOLIS_BASE",
                                "https://www.soliscloud.com:13333"))
SOLIS_KEY_ID = (os.environ.get("SOLIS_KEY_ID", "") or "").strip()
SOLIS_SECRET = (os.environ.get("SOLIS_KEY_SECRET", "") or "").strip()

# Limite da API: 2 req/s. Guardamos o instante da ultima chamada.
_ultima_chamada = {"ts": 0.0}
INTERVALO_MIN = 0.6

# Fatores para normalizar energia -> kWh
_FATOR_ENERGIA = {"wh": 0.001, "kwh": 1.0, "mwh": 1000.0,
                  "gwh": 1000000.0, "twh": 1000000000.0}


# ------------------------------------------------------------
# Auxiliares
# ------------------------------------------------------------
def _safe_float(v):
    try:
        if v in (None, "", "--"):
            return 0.0
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def _energia_kwh(dados, campo):
    """Le o valor E a unidade (campo + 'Str') e devolve sempre em kWh."""
    valor = _safe_float(dados.get(campo))
    unidade = str(dados.get(f"{campo}Str") or "kWh").strip().lower()
    return valor * _FATOR_ENERGIA.get(unidade, 1.0)


def _headers(path, body_txt, prefixo="API "):
    content_md5 = base64.b64encode(
        hashlib.md5(body_txt.encode("utf-8")).digest()).decode()
    content_type = "application/json"
    # formatdate(usegmt=True) gera RFC 1123 em ingles sempre; strftime
    # dependeria do locale da maquina e quebraria a assinatura num
    # servidor configurado em pt_BR ("Sex" em vez de "Fri").
    data_gmt = formatdate(timeval=None, localtime=False, usegmt=True)
    to_sign = f"POST\n{content_md5}\n{content_type}\n{data_gmt}\n{path}"
    sign = base64.b64encode(
        hmac.new(SOLIS_SECRET.encode("utf-8"), to_sign.encode("utf-8"),
                 hashlib.sha1).digest()).decode()
    return {
        "Content-MD5": content_md5,
        "Content-Type": content_type,
        "Date": data_gmt,
        "Authorization": f"{prefixo}{SOLIS_KEY_ID}:{sign}",
    }


def _chamar(path, payload, prefixo="API "):
    # respeita o limite de 2 req/s
    espera = INTERVALO_MIN - (time.time() - _ultima_chamada["ts"])
    if espera > 0:
        time.sleep(espera)
    _ultima_chamada["ts"] = time.time()

    body_txt = json.dumps(payload)
    req = Request(f"{SOLIS_BASE}{path}", data=body_txt.encode("utf-8"),
                  headers=_headers(path, body_txt, prefixo), method="POST")
    resp = urlopen(req, timeout=30)
    return json.loads(resp.read())


# ------------------------------------------------------------
# Leitura em tempo real
# ------------------------------------------------------------
def buscar_realtime(serial_sn, tentativas=3, backoff=4):
    """Busca o inverterDetail de 1 inversor.
    Retorna (ts_utc_naive | None, dados_dict, online_bool)."""
    if not SOLIS_KEY_ID or not SOLIS_SECRET:
        raise ValueError("SOLIS_KEY_ID / SOLIS_KEY_SECRET nao configurados.")

    ultimo = None
    for tentativa in range(1, tentativas + 1):
        # A doc oficial usa "API " (com espaco). Ha implementacoes por ai
        # com "API_" — se o oficial falhar, tenta o alternativo.
        prefixo = "API " if tentativa < tentativas else "API_"
        try:
            b = _chamar("/v1/api/inverterDetail",
                        {"sn": serial_sn}, prefixo)
        except Exception as e:
            ultimo = f"conexao: {e}"
            time.sleep(backoff)
            continue

        code = str(b.get("code"))
        if code in ("0", "200", "None"):
            dados = b.get("data") or {}
            if not dados:
                return None, {}, False

            ts = None
            epoch_ms = _safe_float(dados.get("dataTimestamp"))
            if epoch_ms > 0:
                # dataTimestamp vem em epoch ms (UTC)
                ts = datetime.utcfromtimestamp(epoch_ms / 1000.0)

            # state: 1 = online / gerando
            online = str(dados.get("state")) == "1"
            return ts, dados, online

        ultimo = f"code={code} msg={b.get('msg')}"
        print(f"    [solis] {serial_sn}: {ultimo} "
              f"(tentativa {tentativa}/{tentativas})")
        time.sleep(backoff)

    raise ValueError(f"inverterDetail Solis falhou apos {tentativas} "
                     f"tentativas ({ultimo})")


# ------------------------------------------------------------
# Traducao para o formato do coletor
# ------------------------------------------------------------
def extrair_dados(dados, topologia, online=None):
    """Traduz o inverterDetail da Solis nos MESMOS campos/canais do coletor.
    Retorna (status, campos, mppts, strings)."""
    g = lambda k: _safe_float(dados.get(k))

    campos = {
        "pac_kw":     g("pac"),                       # ja em kW
        "dyield_kwh": _energia_kwh(dados, "eToday"),  # normalizado
        "tyield_kwh": _energia_kwh(dados, "eTotal"),  # GWh -> kWh
        "freq_hz":    g("fac"),                       # Hz
        "tmod_c":     g("inverterTemperature"),       # C
        "tamb_c":     0.0,                            # nao existe
        "iso_kohm":   g("insulationResistance"),      # existe (hoje 0.0)
        "pdc_kw":     g("dcPac"),                     # ja em kW
    }

    # ---- Strings: uPv{n} (V), iPv{n} (A), pow{n} (W) ----
    # ---- MPPTs: derivados das strings pela topologia do banco ----
    num_mppt = topologia["num_mppt"]
    strings, mppts = [], []
    string_num = 0
    for m in range(1, num_mppt + 1):
        qtd = topologia["strings_por_mppt"].get(m, 0)
        soma_i = 0.0
        soma_p = 0.0
        tensao = 0.0
        for _ in range(qtd):
            string_num += 1
            u = g(f"uPv{string_num}")
            i = g(f"iPv{string_num}")
            p = g(f"pow{string_num}")
            if tensao == 0.0 and u > 0:
                tensao = u          # strings do MPPT sao paralelas
            soma_i += i
            soma_p += p
            strings.append({"string_num": string_num, "mppt": m,
                            "corrente_a": i, "potencia_w": p})
        mppts.append({"mppt": m, "tensao_v": tensao,
                      "corrente_a": soma_i, "potencia_w": soma_p})

    if online is None:
        online = campos["pac_kw"] > 0
    status = "ONLINE" if (campos["pac_kw"] > 0 or online) else "OFFLINE"
    return status, campos, mppts, strings
