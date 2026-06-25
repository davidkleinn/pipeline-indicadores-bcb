# extract/extract_daily.py

import json
import sys
from datetime import date
from pathlib import Path

# Garante que o projeto raiz está no sys.path,
# independente de onde o script for chamado.
# Sem isso, "from extract.bcb_client import ..." não encontra o módulo.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from extract.bcb_client import BCBAPIError, get_serie

# ─── Configurações ─────────────────────────────────────────────────────────
SERIES = {
    1:   "dolar",
    11:  "selic",
    433: "ipca",
}
ULTIMOS_N = 5
RAW_DIR = ROOT / "data" / "raw"


# ─── Lógica principal ──────────────────────────────────────────────────────
def main():
    hoje = date.today().isoformat()          # "AAAA-MM-DD"
    pasta_dia = RAW_DIR / hoje
    pasta_dia.mkdir(parents=True, exist_ok=True)  # cria a pasta se não existir

    arquivos_salvos = 0
    total_registros = 0

    for codigo, nome in SERIES.items():
        print(f"[{nome.upper()}] Buscando últimos {ULTIMOS_N} valores...")

        try:
            dados = get_serie(codigo, ultimos=ULTIMOS_N)
        except BCBAPIError as e:
            # Uma série com falha não derruba as outras
            print(f"  [ERRO] {e}")
            continue

        # Serializa datas para string ISO antes de gravar JSON
        # (o tipo `date` não é serializável por json.dumps nativamente)
        payload = [
            {"data": item["data"].isoformat(), "valor": item["valor"]}
            for item in dados
        ]

        caminho = pasta_dia / f"{codigo}.json"
        caminho.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        arquivos_salvos += 1
        total_registros += len(dados)
        print(f"  [ok] {len(dados)} registros -> data/raw/{hoje}/{codigo}.json")

    print(f"\nResumo: {arquivos_salvos}/{len(SERIES)} séries salvas, "
          f"{total_registros} registros totais.")


if __name__ == "__main__":
    main()
