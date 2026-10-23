# Descrição dos arquivos Python

Este documento resume a responsabilidade de cada arquivo Python do laboratório
de tolerância a falhas do GCS (Ray).

## Arquivos

### `common.py`

Contém as configurações compartilhadas do laboratório, como:

- caminhos dos diretórios de execução e logs;
- portas utilizadas pelo Ray e pelo Redis;
- nomes dos workers;
- variáveis de ambiente do Ray;
- comandos para iniciar o head e os workers;
- funções auxiliares para verificar a conexão TCP e a saúde do GCS.

### `cluster.py`

Controla o ciclo de vida do cluster local:

- inicia o Redis usando o `docker-compose.yml`;
- verifica se o Redis está respondendo;
- limpa ou reinicia os dados do Redis;
- inicia o head do Ray;
- inicia os workers;
- encerra o supervisor e os processos do Ray.

### `head_supervisor.py`

Funciona como um supervisor do processo head, simulando o comportamento de um
kubelet ou do KubeRay.

Enquanto o experimento estiver ativo, ele:

- inicia o head do Ray;
- monitora o processo iniciado;
- registra eventos de início e encerramento;
- reinicia o head caso ele termine inesperadamente;
- mantém o mesmo endereço e configuração de armazenamento.

### `probes.py`

Cria os atores de monitoramento (`Heartbeat`) nos workers e executa testes
periódicos de disponibilidade.

Os probes verificam:

- execução local no worker;
- execução de tarefas no próprio worker;
- execução de tarefas em qualquer nó;
- comunicação direta com o GCS;
- localização de outros atores pelo nome.

Os resultados são gravados em arquivos JSONL dentro de `logs/probes`.
O arquivo também possui operações para:

- configurar os probes (`setup`);
- verificar se o estado persistido e os atores sobreviveram (`verify`).

### `run_scenario.py`

É o orquestrador principal dos experimentos.

Ele:

1. limpa o cluster anterior;
2. inicia o Redis, o head e os workers;
3. configura os probes;
4. aguarda o período de execução;
5. inicia o monitoramento externo;
6. injeta uma falha no cluster;
7. observa o comportamento durante o período configurado;
8. verifica o estado após a falha;
9. salva os arquivos de log da execução;
10. chama o módulo de análise;
11. encerra o cluster, exceto quando a opção `--keep` é utilizada.

Os cenários de falha disponíveis são:

- `gcs_kill`: encerra somente o processo `gcs_server`;
- `gcs_freeze`: pausa temporariamente o `gcs_server`;
- `head_kill`: encerra toda a árvore de processos do head;
- `redis_outage`: interrompe temporariamente o Redis;
- `cluster_kill`: encerra o head e os workers e depois reinicia o cluster.

### `analyze.py`

Analisa os eventos do experimento e os resultados dos probes.

O módulo calcula:

- latência mediana antes da falha;
- número de falhas;
- latência máxima;
- instante de degradação;
- instante de recuperação;
- duração da indisponibilidade;
- quantidade de nós observados;
- status de cada probe.

Ao final, imprime um relatório no terminal e gera o arquivo
`summary.json` dentro do diretório da execução.

## Fluxo geral

```text
run_scenario.py
    |
    +-- cluster.py
    |      +-- docker-compose.yml / Redis
    |      +-- head_supervisor.py
    |
    +-- probes.py
    |      +-- executa testes nos workers
    |      +-- grava probe_*.jsonl
    |
    +-- monitoramento externo
    |
    +-- injeção de uma falha
    |
    +-- analyze.py
           +-- processa os logs
           +-- gera summary.json
```

## Ordem típica de execução

1. `run_scenario.py` inicia o experimento.
2. `cluster.py` inicia o Redis, o head e os workers.
3. `head_supervisor.py` mantém o head ativo e o reinicia quando necessário.
4. `probes.py` executa os testes nos workers.
5. `run_scenario.py` injeta a falha e acompanha a recuperação.
6. `analyze.py` consolida os resultados e gera o relatório.

## Exemplo

```bash
python run_scenario.py gcs_kill
```

Os resultados são armazenados em uma pasta semelhante a:

```text
logs/20261023-105641_gcs_kill_redis/
```

Essa pasta pode conter:

- `params.json`: parâmetros utilizados na execução;
- `events.jsonl`: eventos registrados pelo orquestrador e pelo supervisor;
- `head.log`: saída do processo head do Ray;
- `supervisor.log`: saída do supervisor;
- `workers.log`: saída dos workers;
- `probe_*.jsonl`: resultados dos probes;
- `summary.json`: resumo analítico da execução.
