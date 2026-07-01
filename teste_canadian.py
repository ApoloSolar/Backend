# -*- coding: utf-8 -*-
"""
============================================================
  TESTE DE FUMACA — ramo Canadian (Smart Energy / CSI Solar)
============================================================
Exercita SO o adaptador Canadian, SEM tocar no banco:
  1) autentica  (POST /open-api/user/authority)
  2) le tempo real (GET /open-api/device/data?deviceSnStr=SN)
  3) traduz para (status, campos, mppts, strings) e imprime
     exatamente o que a gravar_leitura() gravaria.

Uso:
  CANADIAN_APP_ID=... CANADIAN_APP_SECRET=... python teste_canadian.py
  # opcional: SN=00309925230010J013 (default = inversor de Linhares)

Nao escreve nada no PostgreSQL — e apenas conferencia de leitura.
"""

import os
import sys
import json

import canadian

# Serial do inversor Canadian de Linhares (default). Pode sobrescrever por env.
SN = os.environ.get("SN", "00309925230010J013")

# Topologia do modelo Canadian: 12 MPPTs, 1 string por MPPT (conforme a
# migracao que criou o modelo_inversor/modelo_mppt de Linhares). Aqui e
# hardcoded so para o teste nao precisar do banco.
TOPOLOGIA = {
    "num_mppt": 12,
    "num_string": 12,
    "strings_por_mppt": {m: 1 for m in range(1, 13)},
}


def main():
    if not os.environ.get("CANADIAN_APP_ID") or not os.environ.get("CANADIAN_APP_SECRET"):
        print("ERRO: defina CANADIAN_APP_ID e CANADIAN_APP_SECRET no ambiente.")
        sys.exit(1)

    print("=" * 60)
    print(f"TESTE CANADIAN — SN={SN}")
    print(f"  Base: {canadian.SEP_BASE}")

    # 1) auth
    token = canadian.obter_token()
    print(f"  Token obtido: {token[:12]}... (len={len(token)})")

    # 2) tempo real
    ts_utc, dados, online = canadian.buscar_realtime(SN)
    print(f"  lastReportTime (UTC): {ts_utc}")
    print(f"  online (status==1):   {online}")
    print(f"  fieldCodes recebidos: {len(dados)}")

    if not dados:
        print("  (sem realData — dispositivo nao retornou dados)")
        return

    # 3) traducao — o MESMO formato da gravar_leitura()
    status, campos, mppts, strings = canadian.extrair_dados(dados, TOPOLOGIA, online)

    print("-" * 60)
    print(f"  STATUS : {status}")
    print(f"  CAMPOS : {json.dumps(campos, ensure_ascii=False, indent=2)}")
    print(f"  MPPTs  ({len(mppts)}):")
    for c in mppts:
        print(f"    mppt {c['mppt']:>2} | "
              f"{c['tensao_v']:>8.2f} V | "
              f"{c['corrente_a']:>7.2f} A | "
              f"{c['potencia_w']:>9.2f} W")
    print(f"  STRINGS ({len(strings)}):")
    for c in strings:
        print(f"    string {c['string_num']:>2} (mppt {c['mppt']:>2}) | "
              f"{c['corrente_a']:>7.2f} A | "
              f"{c['potencia_w']:>9.2f} W")
    print("-" * 60)
    print("  OK — nada foi gravado no banco (teste de fumaca).")


if __name__ == "__main__":
    main()
