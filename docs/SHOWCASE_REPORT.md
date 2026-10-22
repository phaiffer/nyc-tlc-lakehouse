# Relatório da vitrine PhaifferTech

Validação local em 22/10/2026, na branch `main`, partindo de `f2a2491` com árvore
limpa. Alterações deixadas no working tree, sem commit, push, tag, release ou
deploy. Nenhuma alteração em CI, `.env`, credenciais ou outros repositórios.

## Ambiente

- Python 3.12.15 em `.venv`, criada com `python3.12 -m venv .venv`.
- OpenJDK 21.0.12.1 selecionado por `JAVA_HOME`.
- Dependências instaladas com `PIP_CACHE_DIR="$PWD/.local/pip-cache" make setup`.
- PySpark 3.5.9, Delta Spark 3.2.0, Ruff 0.9.6 e pytest 8.3.4.
- Spark local com Delta e Hive embarcado; configuração e comandos completos no
  [README](../README.md#reproduce-the-showcase).

O primeiro acesso à rede no sandbox falhou. Instalação, downloads e processos
Spark foram executados com a permissão necessária, mantendo dependências,
caches e saídas explícitas dentro do projeto. Nenhuma dependência global foi
instalada. `make doctor` foi usado para consultar as versões; as tabelas foram
verificadas pelas execuções e reconciliações reais.

## Reprodução do ponto de partida

| Comando | Resultado observado antes das correções funcionais |
| --- | --- |
| `make lint fmt-check` | Lint passou; formatação falhou somente em `pipelines/common/local_delta_writer.py` e `scripts/export_for_bi.py`. |
| `make test` | Os 27 testes originais passaram. |
| `make download YEAR=2024 MONTH=1` | Download público concluído em `data/raw/`. |
| `make run YEAR=2024 MONTH=1` | Bronze 2.964.606, Silver 2.926.167, Gold 85; seis regras sem falhas; bases de drift inicializadas. |
| `.venv/bin/python scripts/demo_report.py --year 2024 --month 1` | Relatório original criou `c144f980-b930-43ed-90f2-c9d48ff9ceeb`, diferente do pipeline `45a9cbb3-76ef-49f6-b6f6-3b1ff51d0b1f`. |

Após `.venv/bin/python -m ruff format pipelines/common/local_delta_writer.py
scripts/export_for_bi.py`, lint e formatação passaram, e `make test` confirmou
novamente os 27 testes. Naquela etapa, os dois arquivos receberam somente
formatação; a revisão posterior também documentou o histórico no exportador BI.

## Alterações e justificativas

| Arquivos | Alteração |
| --- | --- |
| `pipelines/common/local_delta_writer.py` | Formatação Ruff; comportamento preservado. |
| `scripts/export_for_bi.py` | Formatação e documentação de que o export de qualidade preserva todo o histórico e exige filtro/agrupamento por execução e janela. |
| `pipelines/silver_transform/run_silver_enforced.py` | Contagens por primeira rejeição, incluindo nulos e deduplicação; balanço entre entrada, limpeza, janela, contrato e saída; reconciliação do destino mensal. |
| `pipelines/common/month_window.py`, `orchestration/local/run_pipeline.py` | Janela mensal explícita para os perfis de drift; contagem de qualidade no log limitada à execução/janela atuais. |
| `quality/validators/quality_gate.py` | Avaliação na janela do mês e preservação dos resultados de execuções anteriores por append compatível. |
| `scripts/demo_report.py` | Reutiliza o ID da execução do mês e valida métricas/qualidade correspondentes. A contagem Bronze vem do registro da execução, pois a tabela é sobrescrita mensalmente. |
| `tests/integration/test_pipeline_smoke_wiring.py` | Verifica persistência das remoções, balanço e IDs compartilhados com uma entrada contendo duplicata e violações sobrepostas. |
| `tests/unit/test_silver_cleaning_metrics.py` | Cobre nulos, sobreposição de regras, deduplicação, entrada vazia e soma exata das remoções. |
| `tests/unit/test_demo_report_run_id.py` | Cobre seleção do mês/execução e rejeição de ID ou evidência de qualidade incompatível. |
| `tests/unit/test_quality_month_window.py` | Verifica janela do gate/drift e contagem do log com resultados de duas execuções persistidos. |
| `tests/unit/test_quality_gate_schema_stability.py` | Exige preservação das duas execuções e executa o SQL de exemplo para impedir a soma entre seus resultados. |
| `analytics/examples.sql` | Agrupa resultados de qualidade por dataset, execução, ano, mês e severidade. |
| `notebooks/05_quality_gate.ipynb` | Fornece os argumentos atuais do gate e conta somente a invocação/janela correspondente. |
| `docs/data_quality.md` | Documenta as chaves de agrupamento do histórico e o contrato do export BI. |
| `scripts/render_showcase_assets.py` | Lê exports, logs e relatórios reais, valida a consistência, salva evidência agregada e desenha SVGs. Permite renderizar novamente a captura versionada. |
| `README.md`, este relatório, `docs/showcase_results.json`, `docs/images/` | Comandos, resultados, limitações, proveniência e material visual da demonstração. |

Métricas de limpeza foram preferidas à quarentena porque esta já representa
rejeições de contrato. A contagem pela primeira rejeição evita dupla contagem
quando uma linha viola várias condições. A deduplicação é registrada depois
dos filtros. Não foi necessário duplicar os registros descartados.

A validação do relatório após fevereiro identificou que o gate original
apagava os resultados de janeiro por sobrescrita. Essa perda de histórico foi
corrigida e ambos os meses foram novamente executados após `make reset`.

## Comandos das execuções finais

Os comandos abaixo usam as variáveis de ambiente documentadas no README.

| Comando | Resultado |
| --- | --- |
| `make download YEAR=2024 MONTH=1` | Arquivo público de janeiro disponível localmente. |
| `make download YEAR=2024 MONTH=2` | Arquivo público de fevereiro disponível localmente. |
| `make reset` | Tabelas geradas reiniciadas antes de janeiro. |
| `make run YEAR=2024 MONTH=1` | Fluxo Bronze → Silver → Gold → qualidade/drift concluído. |
| `make run YEAR=2024 MONTH=2` | Mesmo fluxo concluído, preservando janeiro para merge e comparação de drift. |
| `.venv/bin/python scripts/demo_report.py --year 2024 --month 1` | Relatório gerado após fevereiro com o ID e as contagens de janeiro. |
| `.venv/bin/python scripts/demo_report.py --year 2024 --month 2` | Relatório com o ID e as contagens de fevereiro. |
| `make export-bi EXPORT_DIR=.local/showcase/exports` | Gold e tabelas de observabilidade exportadas para Parquet local. |
| `.venv/bin/python scripts/render_showcase_assets.py --months 2024-01 2024-02` | SVGs e captura agregada gerados com validação de contagens, IDs, reconciliação e eventos. |
| `.venv/bin/python scripts/render_showcase_assets.py --snapshot docs/showcase_results.json` | Reprodução visual a partir da captura agregada, sem Spark. |

O export final contém oito tabelas: Gold com 143 linhas, dimensões de fornecedor,
pagamento e tarifa com 2, 7 e 7 linhas, respectivamente; qualidade com 12
resultados; métricas com 4 registros; drift com 3 eventos e 4 perfis. Os eventos
são duas inicializações de base e um alerta. A quarentena não foi materializada
porque os contratos rejeitaram zero registros; o export a identificou como
ausente e continuou. As remoções de limpeza estão nas métricas.

| Mês | `run_id` do pipeline, da qualidade e do relatório |
| --- | --- |
| 2024-01 | `ec388ea3-6584-41d8-bfc6-51931ea5b3e1` |
| 2024-02 | `c8920fc2-fadd-491c-b761-80a506b372b5` |

## Contagens por mês

| Medida | Janeiro/2024 | Fevereiro/2024 |
| --- | ---: | ---: |
| Bronze após janela de embarque | 2.964.606 | 3.007.511 |
| Silver limpa | 2.926.167 | 2.965.974 |
| Gold: data × fornecedor | 85 | 58 |
| Duração não positiva | 870 | 803 |
| Tarifa negativa ou ausente | 37.443 | 40.653 |
| Duplicatas após filtros | 126 | 81 |
| Demais filtros, exclusões de janela e rejeições de contrato | 0 | 0 |
| Total removido | 38.439 | 41.537 |
| Regras avaliadas / reprovadas | 6 / 0 | 6 / 0 |

`870 + 37.443 + 126 = 38.439 = 2.964.606 − 2.926.167`.

`803 + 40.653 + 81 = 41.537 = 3.007.511 − 2.965.974`.

As reconciliações de Silver e Gold tiveram diferença zero nos dois meses.
IDs completos e a igualdade com os relatórios constam em
[showcase_results.json](showcase_results.json), junto dos hashes das fontes,
regras, perfis, eventos e valores da Gold utilizados no gráfico.

Janeiro inicializou as bases. Fevereiro comparou os perfis mensais: nenhum
alerta Silver e um `warn` Gold em `volume_ratio`, de 85 para 58 grupos agregados.
O limiar `warn` é 20% e o de `error`, 35%, conforme a configuração aplicada.
Os valores comparados, inclusive abaixo dos limiares, ficam na captura JSON.
O volume da Gold conta grupos por data e fornecedor; o da Silver conta viagens.

### Causa medida do alerta da Gold

A hipótese foi conferida com os registros de `gold_daily` do snapshot, e o
conteúdo da Gold exportada foi comparado com esse snapshot. O resultado também
é calculado e impresso pelo renderizador versionado nos modos normal e
`--snapshot`, e fica em `gold_vendor_summary` na captura agregada.

| Mês | VendorID | Linhas Gold / dias observados | Viagens |
| --- | --- | ---: | ---: |
| 2024-01 | 1 | 31 | 729.016 |
| 2024-01 | 2 | 31 | 2.196.943 |
| 2024-01 | 6 | 23 | 208 |
| 2024-02 | 1 | 29 | 735.405 |
| 2024-02 | 2 | 29 | 2.230.569 |
| 2024-02 | 6 | 0 | 0 |

A hipótese foi confirmada por medição: janeiro tem `31 × 2 + 23 = 85` grupos;
fevereiro tem `29 × 2 + 0 = 58`. A redução reúne os dias a menos para os
VendorIDs 1 e 2 e a ausência do VendorID 6 em fevereiro. O sinal descreve
calendário e composição por fornecedor. As viagens limpas aumentaram de
2.926.167 para 2.965.974, uma diferença de +39.807.

O gráfico diário mostra os fornecedores 1 e 2, com as 208 viagens do VendorID 6
declaradas no rodapé. O total do período e as evidências continuam incluindo
esse fornecedor. A nota do quadro de execução apresenta a decomposição dos
grupos e o aumento das viagens, calculados dos dados.

Os rótulos de limpeza usam **tarifa negativa ou ausente** e **distância negativa
ou ausente**. Ausente corresponde a `NULL`; valores zero passam pelos filtros
`fare_amount >= 0` e `trip_distance >= 0`. Os identificadores técnicos das regras
foram preservados.

## Revisão dos leitores do histórico

A consulta em `analytics/examples.sql` apresenta todas as execuções, separadas
por `dataset`, `run_id`, `window_year`, `window_month` e `severity`. Executada
contra o export real, ela retornou cinco regras `error` e uma `warn` para cada
execução/mês, todas sem falhas. Um teste executa o mesmo SQL com duas execuções
do mesmo mês e exige a separação de seus resultados.

O notebook de qualidade fornece ID, timestamp e janela ao gate; seu leitor
filtra a contagem para aquela invocação. O orquestrador também limita a contagem
do log à execução/janela atuais, com teste cobrindo histórico acumulado. Os
leitores do relatório e do renderizador já filtravam por execução e mês. O
export BI mantém o histórico completo e suas chaves, com essa convenção agora
documentada no exportador, README e documentação de qualidade. Os modelos,
sources e marts dbt foram inspecionados: não leem `quality.violations_summary`.

O README foi padronizado em inglês; os SVGs continuam em português. Nesta
revisão, as tabelas do pipeline não foram reiniciadas nem reexecutadas. A Gold,
os logs e os exports reais da execução anterior foram usados como evidência.

## Verificação inicial da vitrine

`make lint fmt-check`:

```text
[lint] running ruff check
.venv/bin/python -m ruff check .
All checks passed!
[fmt-check] running ruff format --check
.venv/bin/python -m ruff format --check .
40 files already formatted
```

`make test`, com `PYTEST_ADDOPTS='-o addopts='` para exibir o total:

```text
[test] running pytest -q
.venv/bin/python -m pytest -q
...................................                                      [100%]
35 passed in 287.66s (0:04:47)
```

`make contracts smoke` também passou: contratos válidos, nenhuma mudança
incompatível sem versão e imports bem-sucedidos. Os testes utilizam um
metastore separado das execuções reais.

A reprodução da captura foi executada com prévias PNG apenas no diretório
ignorado, e os SVGs resultantes tiveram hashes idênticos:

```bash
sha256sum docs/images/*.svg > .local/showcase/svg-hashes.txt
.venv/bin/python scripts/render_showcase_assets.py --snapshot docs/showcase_results.json --preview-dir .local/showcase/previews
sha256sum -c .local/showcase/svg-hashes.txt
```

```text
docs/images/architecture.svg: OK
docs/images/execution_summary.svg: OK
docs/images/trips_daily.svg: OK
```

As prévias foram inspecionadas visualmente. `make docs-check` validou os links
Markdown e `git diff --check` verificou o diff. `git status` contém somente os
arquivos intencionais descritos acima, sem dados brutos, Delta ou exports.

## Verificação da revisão

A revisão usou o mesmo working tree, sem commit, push, reset, clean ou stash.
Não foi necessário baixar dados, instalar dependências nem executar novamente
as camadas. A consulta de fornecedores foi inicialmente medida no snapshot;
o export real da Gold foi comparado com ele, e a agregação foi incorporada ao
renderizador para reprodução nos dois modos.

`make lint`, `make fmt-check` e `make test` (Java 21 e
`PYTEST_ADDOPTS='-o addopts='`) concluíram com código zero:

```text
[lint] running ruff check
.venv/bin/python -m ruff check .
All checks passed!
[fmt-check] running ruff format --check
.venv/bin/python -m ruff format --check .
40 files already formatted
[test] running pytest -q
.venv/bin/python -m pytest -q
...................................                                      [100%]
35 passed in 156.55s (0:02:36)
```

Os comandos de renderização executados foram:

```bash
.venv/bin/python scripts/render_showcase_assets.py --months 2024-01 2024-02
sha256sum docs/images/*.svg > .local/showcase/review-svg-hashes.txt
.venv/bin/python scripts/render_showcase_assets.py --snapshot docs/showcase_results.json --preview-dir .local/showcase/review-previews
sha256sum -c .local/showcase/review-svg-hashes.txt
```

Nos dois modos, a medição retornou:

```text
2024-01 VendorID=1 Gold rows=31 days=31 trips=729016
2024-01 VendorID=2 Gold rows=31 days=31 trips=2196943
2024-01 VendorID=6 Gold rows=23 days=23 trips=208
2024-02 VendorID=1 Gold rows=29 days=29 trips=735405
2024-02 VendorID=2 Gold rows=29 days=29 trips=2230569
2024-02 VendorID=6 Gold rows=0 days=0 trips=0
Grupos: 2024-01: 31 × 2 + 23 = 85; 2024-02: 29 × 2 + 0 = 58. Parcela adicional: VendorID 6.
O sinal reflete calendário e presença de fornecedores. Viagens: 2.926.167 → 2.965.974 (+39.807).
```

Os SVGs ficaram idênticos entre os modos:

```text
docs/images/architecture.svg: OK
docs/images/execution_summary.svg: OK
docs/images/trips_daily.svg: OK
```

`git diff --check` concluiu sem saída e com código zero. `make docs-check`
também passou. As prévias foram inspecionadas; a legenda contém somente os
VendorIDs 1 e 2, e o quadro apresenta os novos rótulos e a causa medida do
alerta. O README foi conferido para manter a prosa em inglês.

O notebook completo não foi executado nesta revisão, pois segue o fluxo
exploratório com caminhos próprios. A célula alterada foi validada
sintaticamente; as APIs do gate e a consulta por execução foram verificadas
pela suíte. Os modelos dbt foram inspecionados, sem execução de dbt.

## Artefatos e limites

- [Arquitetura](images/architecture.svg)
- [Viagens diárias por fornecedor](images/trips_daily.svg)
- [Resumo das execuções](images/execution_summary.svg)
- [Captura agregada reproduzível](showcase_results.json)

Os SVGs têm fundo claro e não usam logotipos externos. Dados brutos, tabelas
Delta, metastore, exports e logs permanecem ignorados em `data/raw/` e `.local/`.
O snapshot versionado contém apenas agregações e evidências da execução.

O modo `warn` do gate registra reprovações sem interromper o pipeline. O modo
estrito interrompe por reprovações de severidade `error`; validações de contrato
continuam ativas nos dois modos. Drift é persistido como observabilidade.

A amostra cobre dois meses de táxis yellow em execução local. Não foram
verificados implantação em nuvem, escala distribuída, sazonalidade, operação
contínua nem o fluxo opcional dbt. A CI remota não foi acionada, pois não houve
push. Avisos locais de Hive/Hadoop e de limpeza de temporários no encerramento
da JVM foram observados; os fluxos finais concluíram com código de saída zero.
