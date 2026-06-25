import time
from datetime import date, timedelta

import requests

# ─── Configurações globais ────────────────────────────────────────────────────
BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
TIMEOUT = 10       # segundos até desistir de uma requisição
MAX_RETRIES = 3    # quantas tentativas antes de desistir
RETRY_BACKOFF = 2  # segundos de espera entre tentativas


# ─── Exceção customizada ──────────────────────────────────────────────────────
# Por que criar uma exception própria?
# requests.exceptions.RequestException é genérica demais.
# BCBAPIError carrega contexto específico (qual série, qual URL falhou).
# Quem chamar get_serie() pode fazer: except BCBAPIError — limpo e explícito.
class BCBAPIError(Exception):
    pass


# ─── Helpers privados (prefixo _ = não são parte da API pública do módulo) ───
def _format_date(d: date) -> str:
    """Converte date Python → 'DD/MM/AAAA' (formato que a API espera)."""
    return d.strftime("%d/%m/%Y")


def _parse_date(date_str: str) -> date:
    """Converte 'DD/MM/AAAA' → date Python."""
    day, month, year = date_str.split("/")
    return date(int(year), int(month), int(day))


def _add_years(d: date, years: int) -> date:
    """
    Soma `years` anos a uma data de forma segura.
    Caso especial: 29/fev + N anos pode não existir → cai para 28/fev.
    """
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        return d.replace(year=d.year + years, day=28)


# ─── Função principal: uma janela de dados ───────────────────────────────────
def get_serie(
    codigo: int,
    data_inicial: date = None,
    data_final: date = None,
    ultimos: int = None,
) -> list[dict]:
    """
    Busca uma série do BCB e retorna lista de dicts já com tipos corretos:
        [{"data": date(2025, 6, 19), "valor": 5.75}, ...]

    Modos de uso:
        get_serie(11, ultimos=5)                          → últimos 5 valores
        get_serie(11, data_inicial=date(2020,1,1),
                      data_final=date(2021,1,1))         → intervalo específico

    Raises:
        BCBAPIError: qualquer falha de rede ou resposta não-200
        ValueError:  chamada sem ultimos nem data_inicial
    """
    # Monta URL dependendo do modo
    if ultimos is not None:
        url = f"{BASE_URL.format(codigo=codigo)}/ultimos/{ultimos}?formato=json"
    elif data_inicial is not None:
        if data_final is None:
            data_final = date.today()
        url = (
            f"{BASE_URL.format(codigo=codigo)}"
            f"?formato=json"
            f"&dataInicial={_format_date(data_inicial)}"
            f"&dataFinal={_format_date(data_final)}"
        )
    else:
        raise ValueError("Passe 'ultimos' ou 'data_inicial' para get_serie().")

    # Retry loop: tenta MAX_RETRIES vezes antes de desistir
    for tentativa in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=TIMEOUT)

            if response.status_code != 200:
                # Erro da API (400, 404, 500...) — não tem sentido retentar
                raise BCBAPIError(
                    f"Série {codigo}: API retornou HTTP {response.status_code}. "
                    f"URL: {url}"
                )

            raw = response.json()

            # Converte tipos imediatamente — string nunca sai deste módulo
            resultado = []
            for item in raw:
                try:
                    resultado.append({
                        "data": _parse_date(item["data"]),
                        "valor": float(item["valor"]),
                    })
                except (ValueError, KeyError) as e:
                    # Registro malformado: avisa mas não derruba o script
                    print(f"  [aviso] Registro ignorado na série {codigo}: {item} ({e})")

            return resultado

        except BCBAPIError:
            raise  # Erros da API não têm retry — relança direto

        except requests.exceptions.Timeout:
            if tentativa < MAX_RETRIES:
                print(f"  [aviso] Timeout (tentativa {tentativa}/{MAX_RETRIES}). "
                      f"Aguardando {RETRY_BACKOFF}s...")
                time.sleep(RETRY_BACKOFF)
            else:
                raise BCBAPIError(
                    f"Série {codigo}: timeout após {MAX_RETRIES} tentativas. URL: {url}"
                )

        except requests.exceptions.RequestException as e:
            if tentativa < MAX_RETRIES:
                print(f"  [aviso] Erro de rede: {e}. "
                      f"Tentativa {tentativa}/{MAX_RETRIES}. Aguardando {RETRY_BACKOFF}s...")
                time.sleep(RETRY_BACKOFF)
            else:
                raise BCBAPIError(
                    f"Série {codigo}: falha de rede após {MAX_RETRIES} tentativas: {e}"
                )


# ─── Histórico completo: pagina em janelas de 10 anos ────────────────────────
def get_serie_completa(codigo: int, desde: date) -> list[dict]:
    """
    Busca todo o histórico de uma série do BCB desde `desde` até hoje.

    A API rejeita intervalos maiores que 10 anos (retorna HTTP 400).
    Esta função quebra o intervalo em janelas de até 10 anos e concatena.

    Returns:
        Lista de dicts {"data": date, "valor": float}, ordenada e sem duplicatas.
    """
    hoje = date.today()
    todos = []
    inicio = desde

    while inicio <= hoje:
        fim = _add_years(inicio, 10)
        if fim > hoje:
            fim = hoje

        print(f"  [série {codigo}] Janela: {_format_date(inicio)} -> {_format_date(fim)}")
        dados = get_serie(codigo, data_inicial=inicio, data_final=fim)
        todos.extend(dados)

        # Próxima janela começa no dia seguinte (evita duplicata na borda)
        inicio = fim + timedelta(days=1)

    # Remove duplicatas de data que possam ter sobrado nas bordas
    # (usa dict com data como chave — em caso de colisão, mantém o primeiro)
    sem_duplicatas = {}
    for item in todos:
        if item["data"] not in sem_duplicatas:
            sem_duplicatas[item["data"]] = item

    # Retorna ordenado por data
    return sorted(sem_duplicatas.values(), key=lambda x: x["data"])
