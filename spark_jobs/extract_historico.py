# spark_jobs/extract_historico.py

import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from extract.bcb_client import BCBAPIError, get_serie_completa

# codigo: (nome, data de início do histórico)
SERIES = {
    1:   ("dolar", date(1984, 1, 1)),
    11:  ("selic", date(1986, 1, 1)),
    433: ("ipca",  date(1980, 1, 1)),
}

OUTPUT_DIR = ROOT / "data" / "raw" / "historico"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for codigo, (nome, desde) in SERIES.items():
        print(f"[{nome.upper()}] Buscando histórico completo desde {desde}...")

        try:
            dados = get_serie_completa(codigo, desde=desde)
        except BCBAPIError as e:
            print(f"  [ERRO] {e}")
            continue

        # Sem codigo_serie aqui de propósito — essa coluna é responsabilidade
        # da camada de processamento (Spark), não da zona raw.
        payload = [
            {"data": item["data"].isoformat(), "valor": item["valor"]}
            for item in dados
        ]

        caminho = OUTPUT_DIR / f"{codigo}.json"
        caminho.write_text(json.dumps(payload, ensure_ascii=False, indent=2))

        print(f"  ✓ {len(dados)} registros → data/raw/historico/{codigo}.json")


if __name__ == "__main__":
    main()