# Laboratório de tolerância a falhas do GCS (Ray) — execução local

Cluster local com **1 head + 2 workers + Redis externo**, injeção de falhas e medição de
comportamento/tempo de recuperação a partir **de dentro dos workers**.

## Pré-requisitos
- Linux, macOS ou WSL2 (cluster multi-nó local não é suportado no Windows nativo)
- Python 3.9+ e **um** Redis: Docker (`docker-compose.yml`) **ou** `redis-server` local (`brew install redis`).
  O backend é escolhido sozinho (`LAB_REDIS_BACKEND=auto`); force com `LAB_REDIS_BACKEND=docker|native`.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Como o laboratório mapeia a documentação do Ray
| Doc oficial | Implementação aqui |
|---|---|
| `RAY_REDIS_ADDRESS=host:port ray start --head --redis-password PASS --redis-username default` | `lab/common.py::head_cmd/ray_env` |
| GCS reinicia e recarrega o estado do Redis; raylets tentam reconectar | `lab/head_supervisor.py` reinicia o head com o mesmo endereço/porta/storage (equivale ao restart do Pod head no KubeRay) |
| Somente **um** GCS vivo por vez | Um único supervisor; porta fixa `6380` |
| `ray.io/external-storage-namespace` (KubeRay) | `RAY_external_storage_namespace=ray-ft-lab` fixo — sem isso o restart cria um namespace novo e **não** recupera |
| `RAY_gcs_rpc_server_reconnect_timeout_s` (YAML do KubeRay) | `--reconnect-timeout` (default 60s) |
| Backend alternativo `RAY_gcs_storage=rocksdb RAY_gcs_storage_path=...` | `--storage rocksdb` (alpha, exige Ray recente) |

> Atenção: a doc diz que GCS FT com Redis externo é **oficialmente suportado apenas no KubeRay**
> (para Ray Serve). Rodar via `ray start` local é válido para estudo/teste, mas não é um caminho "suportado".

## Uso
```bash
# crash parcial: só o gcs_server morre (supervisor reinicia o head em 2s)
python lab/run_scenario.py gcs_kill

# GCS indisponível por 20s sem perder memória (ótimo p/ medir o timeout de reconexão)
python lab/run_scenario.py gcs_freeze --down 20

# crash total do head (GCS + raylet do head)
python lab/run_scenario.py head_kill --restart-delay 5

# Redis cai por 15s com o cluster rodando
python lab/run_scenario.py redis_outage --down 15

# crash total do cluster e recuperação só a partir do Redis
python lab/run_scenario.py cluster_kill --down 10

# baseline SEM tolerância a falhas (esperado: cluster não sobrevive)
python lab/run_scenario.py gcs_kill --storage none

# estourar o timeout de reconexão: GCS parado mais tempo que o limite
python lab/run_scenario.py gcs_freeze --down 40 --reconnect-timeout 15
```
Cada execução: derruba/limpa o cluster → sobe tudo → cria probes → aquece 15s → injeta a falha →
observa 60s → verifica estado (marcador no KV + detached actors) → imprime relatório e grava em `logs/<run_id>/`.

Subir/derrubar manualmente: `python lab/cluster.py up|down|status [--storage redis|rocksdb|none]`.
Re-analisar: `python lab/analyze.py logs/<run_id> --thr 2.0`.

## O que é medido
**Dentro de cada worker** (detached actor `hb_wk_a`/`hb_wk_b`, ciclo de 0,5s, log em JSONL local):
- `local`: loop sem Ray — prova que o processo do worker continua vivo
- `task_local`: tarefa fixada no próprio nó (depende só do raylet local)
- `task_any`: tarefa em qualquer nó (depende da visão do cluster)
- `gcs_nodes`: `ray.nodes()` (RPC ao GCS) + nº de nós vivos
- `actor_lookup`: `ray.get_actor(<peer>)` (lookup por nome no GCS)

**Por fora**: porta TCP do GCS (100ms), `ray health-check` (~1–2s de resolução), eventos do supervisor
(`head_process_started/exited`) e verificação pós-falha (`verify_state`: o marcador do KV e os detached actors sobreviveram?).

O relatório mostra, por probe: status (`NO IMPACT`/`RECOVERED`/`NOT RECOVERED`/`SILENT`), instante de
degradação, instante de recuperação, duração do outage, nº de falhas e latência máxima — tudo relativo à injeção da falha.

## Comportamento esperado (hipóteses para validar)
- `gcs_freeze` curto / `gcs_kill` com Redis: `local` e `task_local` sem impacto; `gcs_nodes`/`actor_lookup`
  degradam e voltam após o GCS reaparecer; `task_any` pode degradar.
- `*` com `--storage none`: após o GCS novo subir vazio, raylets devem encerrar → probes `SILENT`.
- `redis_outage`: o GCS depende do Redis para persistir; observe se ele cai e é reiniciado pelo supervisor.
- `cluster_kill`: `kv_marker_preserved=true` indica recuperação do estado via Redis; os detached actors tendem a
  ficar mortos (a doc do KubeRay deixa claro que o **estado** do actor não é persistido).
- `--down` > `--reconnect-timeout`: workers desistem de reconectar → `SILENT`.

## Correções que deixaram o `gcs_kill` funcional de ponta a ponta
| Sintoma | Causa | Correção |
|---|---|---|
| Após o `kill -9` no `gcs_server`, o head entrava em loop de restart (`rc=1` a cada ~0,7s) e o GCS nunca voltava | O `ray start --head` recusa subir quando acha que já existe um cluster no mesmo endereço: lê `/tmp/ray/ray_current_cluster` e encontra os raylets dos workers (vivos, tentando reconectar) apontando para `:6380`. Erro: *"Ray is trying to start at X:6380, but is already running at X:6380"*. Só acontece porque todos os "nós" estão na mesma máquina | `head_supervisor.py` apaga o `ray_current_cluster` antes de cada (re)start e espera as portas fixas do head (6380/6390/6391) liberarem |
| Processos/portas ocupados entre execuções | Supervisor órfão de uma execução interrompida (Ctrl-C) continuava reiniciando o head | `cluster.down()` mata supervisores órfãos, roda `ray stop --force`, espera as portas fecharem e limpa o `ray_current_cluster` |
| Relatório quebrava com `FileNotFoundError: events.jsonl` | `analyze.report()` era chamado antes de copiar os logs para `logs/<run_id>/` | Artefatos copiados antes da análise; `analyze.py` lê os probes da própria pasta da execução |
| Setup dos probes lento (≈60s no macOS) | Threads dos probes iniciavam no `__init__` e competiam com o `ping` do setup | Actors criados em paralelo, `ping` primeiro, depois `start()` dispara as threads (`max_concurrency=4`) |

## Limites conhecidos
- Tudo roda em uma máquina (loopback): não modela partição de rede real. Para isso use `tc netem`/`iptables` no GCS.
- Resolução do `ray health-check` é grosseira; use os probes para tempos reais de recuperação.
- Nomes de flags/variáveis internas do Ray variam entre versões; foi desenhado a partir da doc "latest/master".
  Confira com `ray --version` e os logs em `logs/<run_id>/head.log`.
