# extract/extract_daily.py

import json
import sys
from datetime import date
from pathlib import Path

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


# ─── Função reutilizável (usada pela DAG via XCom e pelo main() local) ─────
def extract_today() -> dict:
    """
    Busca os últimos ULTIMOS_N valores de cada série do BCB.

    Faz duas coisas ao mesmo tempo:
      1. Salva o JSON bruto em data/raw/{hoje}/{codigo}.json  (auditoria)
      2. Retorna os dados como dict serializável               (para o Airflow XCom)

    Returns:
        {
            "1":   [{"data": "2025-06-25", "valor": 5.75}, ...],
            "11":  [...],
            "433": [...]
        }

    Nota: chaves do dict são strings porque JSON não aceita chaves inteiras.
    Nota: datas são strings ISO ("AAAA-MM-DD") porque `date` não é serializável por JSON.
    """
    hoje = date.today().isoformat()
    pasta_dia = RAW_DIR / hoje
    pasta_dia.mkdir(parents=True, exist_ok=True)

    resultado = {}

    for codigo, nome in SERIES.items():
        print(f"[{nome.upper()}] Buscando últimos {ULTIMOS_N} valores...")

        try:
            dados = get_serie(codigo, ultimos=ULTIMOS_N)
        except BCBAPIError as e:
            print(f"  [ERRO] {e}")
            resultado[str(codigo)] = []   # série falhou: lista vazia, não quebra o fluxo
            continue

        # Serializa antes de salvar E antes de retornar
        # (date e float nativos do bcb_client → strings/floats para JSON)
        payload = [
            {"data": item["data"].isoformat(), "valor": item["valor"]}
            for item in dados
        ]

        caminho = pasta_dia / f"{codigo}.json"
        caminho.write_text(json.dumps(payload, ensure_ascii=False, indent=2))

        resultado[str(codigo)] = payload
        print(f"  ✓ {len(dados)} registros → data/raw/{hoje}/{codigo}.json")

    return resultado


# ─── Ponto de entrada para execução local (comportamento anterior mantido) ──
def main():
    resultado = extract_today()
    total = sum(len(v) for v in resultado.values())
    series_ok = sum(1 for v in resultado.values() if v)
    print(f"\nResumo: {series_ok}/{len(SERIES)} séries salvas, {total} registros totais.")


if __name__ == "__main__":
    main()