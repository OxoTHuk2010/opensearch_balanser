# Справочник конфигурации

Этот документ описывает каждое поле `config.example.yaml`: смысл, место использования, зависимости от других полей и ожидаемый эффект изменения. Имена полей оставлены как в YAML, чтобы инженер мог быстро сопоставить описание с конфигурацией.

## Загрузка и значения по умолчанию

Приложение сначала берет `config.Default()`, затем накладывает значения из YAML. После загрузки часть пустых или некорректных значений нормализуется:

- `cluster.endpoint` обязателен.
- Неположительные duration значения заменяются defaults.
- `planner.max_moves_per_plan`, `planner.severe_shard_imbalance_threshold`, `planner.large_shard_size_gb` и `planner.large_shard_penalty_multiplier` получают defaults, если заданы как `<= 0`.
- Отрицательные planner weights и thresholds сбрасываются в безопасные defaults или `0`, в зависимости от поля.
- Если `planner.node_balance_weight_disk` и `planner.node_balance_weight_shards` оба равны `0`, они сбрасываются в `0.6` и `0.4`.
- `limits.max_churn_per_run` становится равным `planner.max_moves_per_plan`, если задан как `<= 0`.
- `policy.execution_window_start_utc` должен быть в диапазоне `0..23`.
- `policy.execution_window_end_utc` должен быть в диапазоне `1..24`.

## Основные зависимости полей

- `cluster.max_active_recoveries` используется и planner, и safety layer. Если активных OpenSearch recovery/relocation больше этого значения, planning/apply блокируются или execution останавливается.
- `policy.critical_disk_percent` используется analyzer для overflow findings и safety layer для post-batch stop checks при `policy.stop_on_watermark_breach=true`.
- `policy.allow_yellow` влияет на apply preflight и на поведение `policy.stop_on_health_degrade`.
- `policy.enforce_execution_window` включает проверку `policy.execution_window_start_utc` и `policy.execution_window_end_utc`.
- `planner.low_watermark_safety_margin_percent` вычитается из OpenSearch low watermark перед проверкой target eligibility.
- `planner.target_free_gb_per_node` включает pressure mode, если data node имеет меньше свободного места, чем target, и есть другая data node, которая может принять данные без ухудшения deficit.
- `planner.node_balance_weight_pressure`, `planner.pressure_min_shard_size_gb` и `planner.move_score_pressure_size_reward` заметно влияют только при `planner.target_free_gb_per_node > 0` и наличии source node ниже target.
- `planner.large_shard_size_gb`, `planner.large_shard_penalty_multiplier`, `planner.severe_shard_imbalance_threshold` и `planner.move_score_severe_large_shard_extra_mult` вместе управляют избеганием больших shard moves при сильном shard-count imbalance.
- `limits.max_concurrent_moves` и `limits.max_data_gb_per_batch` вместе задают размер execution batch. Batch закрывается, если следующий move превысит лимит по количеству параллельных moves или по суммарному объему данных.
- `runtime.require_manual_approval` управляет интерактивным подтверждением batch в CLI. `force` mode и API apply не используют CLI prompt, но safety checks все равно выполняются.
- `runtime.check_allocator` определяет, будет ли simulation спрашивать OpenSearch allocator через dry-run reroute.
- `observability.audit_sink_path` определяет каталог, который создается при старте runtime, и файл, куда пишутся lifecycle logs.

## Формулы planner

### Estimated cost шага плана

Каждый move получает оценочную стоимость:

```text
networkGB = shardSizeGB
diskIOGB = shardSizeGB * 2
cpuUnits = max(0.1, shardSizeGB / 20)
estimatedCostScore = weight_cost * (networkGB + diskIOGB + cpuUnits) + weight_disk * targetNodeDiskUsedPercent
```

Эта оценка сохраняется в plan artifact и помогает понять цену move. Для выбора кандидата используется отдельный candidate score ниже.

### Score сортировки нод

Source nodes сортируются по weighted score:

```text
diskNorm = nodeDiskUsedPercent / maxDiskUsedPercent
shardNorm = nodeShardCount / maxShardCount
pressureNorm = max(0, target_free_gb_per_node - nodeFreeGB) / target_free_gb_per_node

sourceScore =
  node_balance_weight_disk * diskNorm +
  node_balance_weight_shards * shardNorm +
  node_balance_weight_pressure * pressureNorm
```

Target nodes сортируются по возрастанию score:

```text
targetScore =
  node_balance_weight_disk * diskNorm +
  node_balance_weight_shards * shardNorm
```

### Candidate move score

Shard candidates сортируются по минимальному score:

```text
afterDiskGap = abs(sourceDiskPercentAfterMove - targetDiskPercentAfterMove)
afterShardGap = abs((sourceShardCount - 1) - (targetShardCount + 1))
primaryPenalty = move_score_primary_penalty, если shard primary, иначе 0

sizePenalty =
  shardSizeGB                                      если shardSizeGB < large_shard_size_gb
  shardSizeGB * large_shard_penalty_multiplier    если shardSizeGB >= large_shard_size_gb

если severe shard imbalance и shardSizeGB >= large_shard_size_gb:
  sizePenalty = shardSizeGB * large_shard_penalty_multiplier * move_score_severe_large_shard_extra_mult

candidateScore =
  primaryPenalty +
  afterDiskGap * move_score_weight_disk_gap +
  afterShardGap * move_score_weight_shard_gap +
  sizePenalty * move_score_weight_size

если source node ниже target_free_gb_per_node:
  candidateScore -= min(shardSizeGB, sourceDeficitGB) * move_score_pressure_size_reward
```

### Improvement score

Move принимается, только если улучшает skew или, в pressure mode, строго уменьшает общий free-space deficit:

```text
weightedScore =
  weight_disk * diskSkewPct +
  weight_shards * shardSkewPct +
  weight_risk * riskPenalty

baseImprovement = weightedScore(before) - weightedScore(after)

pressureGainGB = beforeTotalFreeDeficitGB - afterTotalFreeDeficitGB
improvement = baseImprovement + pressureGainGB * node_balance_weight_pressure
```

Если `weight_disk`, `weight_shards` и `weight_risk` все равны `0`, planner использует fallback на старую сырую сумму `diskSkewPct + shardSkewPct + riskPenalty`. Для сценария, где shard count уже выровнен, но disk usage сильно перекошен, увеличивайте `weight_disk` и уменьшайте `weight_shards`, иначе ухудшение shard count может заблокировать полезный disk-relief move.

## Поля конфигурации

### `cluster`

| Поле | Значение | Эффект изменения |
| --- | --- | --- |
| `backend` | Бэкенд collector/executor. Сейчас поддерживается `opensearch`. | Другое значение само по себе не добавит поддержку нового backend. Нужен новый adapter. |
| `endpoint` | Базовый URL OpenSearch HTTP API. Обязателен. | Все collection, dry-run и apply запросы идут сюда. Ошибка в URL блокирует workflow. |
| `username` | Пользователь Basic Auth. | Пустое значение отключает Basic Auth. Для non-apply workflow используйте read-only credentials. |
| `password_env` | Имя переменной окружения с паролем. | Не хранит secret в YAML. Если переменная не задана, пароль будет пустым. |
| `tls_enabled` | Включает TLS transport config. | Если false, `ca_file` и `skip_tls_verify` не влияют на HTTP client. |
| `skip_tls_verify` | Отключает проверку сертификата при TLS. | Полезно только для тестов. В production снижает безопасность transport. |
| `ca_file` | Путь к PEM CA bundle. | Позволяет доверять конкретной CA. Startup adapter упадет, если файл нельзя прочитать или распарсить. |
| `request_timeout` | Таймаут одного HTTP-запроса к OpenSearch. | Больше значение лучше переносит медленный API, но дольше обнаруживает зависшие операции. |
| `max_active_recoveries` | Максимум допустимых active recovery/relocation. | Planner/apply preflight блокируются при превышении; post-batch checks останавливают execution при превышении. |

### `planner`

| Поле | Значение | Эффект изменения |
| --- | --- | --- |
| `max_moves_per_plan` | Максимальное число шагов в плане. | Больше значение создает более длинные планы. Реальный apply batch все равно ограничивается `limits.*`. |
| `weight_disk` | Вес disk skew в gate принятия move и target disk usage в estimated cost шага. | Больше значение помогает принимать moves, которые снижают disk skew, и делает moves на заполненные target nodes дороже в plan artifact. |
| `weight_shards` | Вес shard skew в gate принятия move. | Меньше значение позволяет disk-relief moves, которые временно ухудшают shard count; больше значение жестче защищает shard-count balance. |
| `weight_risk` | Вес risk penalty в gate принятия move. | Больше значение делает снижение risk penalty важнее чистого disk/shard skew. |
| `weight_cost` | Вес network/diskIO/CPU в estimated cost. | Больше значение делает большие moves дороже в plan artifact. |
| `severe_shard_imbalance_threshold` | Shard-count gap, который включает severe imbalance. | Меньше значение чаще включает дополнительное избегание больших shard. |
| `large_shard_size_gb` | Размер, с которого shard считается большим. | Меньше значение относит больше shard к большим и сильнее ограничивает их moves. |
| `large_shard_penalty_multiplier` | Множитель penalty для больших shard. | Больше значение избегает больших moves, если они не дают существенную пользу. |
| `node_balance_weight_disk` | Вес disk usage в сортировке source/target нод. | Больше относительное значение смещает планирование в сторону disk balance. |
| `node_balance_weight_shards` | Вес shard count в сортировке source/target нод. | Больше относительное значение смещает планирование в сторону shard-count balance. |
| `move_score_weight_disk_gap` | Вес post-move disk gap. | Больше значение выбирает moves, сильнее выравнивающие disk percentage. |
| `move_score_weight_shard_gap` | Вес post-move shard-count gap. | Больше значение выбирает moves, сильнее выравнивающие shard count. |
| `move_score_weight_size` | Вес size penalty. | Больше значение предпочитает меньшие shard moves. |
| `move_score_primary_penalty` | Штраф за primary shard. | Больше значение делает replica moves намного предпочтительнее primary moves. |
| `move_score_severe_large_shard_extra_mult` | Дополнительный множитель для больших shard при severe imbalance. | Больше значение мешает исправлять shard-count skew перемещением больших shard. |
| `min_move_shard_size_gb` | Минимальный shard size для включения в план. | Больше значение пропускает больше tiny shard и снижает малополезный churn. |
| `target_free_gb_per_node` | Желаемый free disk на data node. | Значения `> 0` включают pressure scoring; больше значение делает больше нод дефицитными. |
| `node_balance_weight_pressure` | Вес free-space pressure. | Больше значение фокусирует план на снижении free-space deficit. |
| `pressure_min_shard_size_gb` | Минимальный shard size в pressure mode. | Больше значение не дает pressure mode выбирать маленькие shard. |
| `move_score_pressure_size_reward` | Reward за полезный drain size. | Больше значение выбирает shard, которые быстрее уменьшают дефицит source node. |
| `low_watermark_safety_margin_percent` | Процент, вычитаемый из OpenSearch low watermark. | Больше значение оставляет больше headroom и уменьшает число eligible target nodes. |

### `limits`

| Поле | Значение | Эффект изменения |
| --- | --- | --- |
| `max_concurrent_moves` | Максимум moves параллельно в batch. | Больше значение ускоряет apply, но повышает relocation pressure на OpenSearch. |
| `max_data_gb_per_batch` | Максимальный суммарный размер shard в batch. | Меньше значение создает меньшие batch и больше контрольных точек. |
| `cooldown_seconds` | Пауза между batch. | Больше значение замедляет execution, но дает кластеру стабилизироваться. |
| `max_churn_per_run` | Задуманный cap churn за run. | Сейчас нормализуется, но executor его не применяет; держите согласованным с `planner.max_moves_per_plan` до реализации enforcement. |

### `policy`

| Поле | Значение | Эффект изменения |
| --- | --- | --- |
| `allow_yellow` | Разрешает apply при yellow health. | Red всегда блокируется. Если true, green->yellow не считается stop condition в `stop_on_health_degrade`. |
| `critical_disk_percent` | Критический disk threshold. | Analyzer создает overflow findings на этом пороге; safety может остановить execution при пересечении. |
| `stop_on_health_degrade` | Остановка после batch, если health ухудшился относительно baseline. | Отключайте только если внешний контроль берет этот риск на себя. |
| `stop_on_watermark_breach` | Остановка после batch, если нода пересекла `critical_disk_percent`. | Напрямую зависит от `critical_disk_percent`. |
| `enforce_execution_window` | Включает UTC window checks для apply. | Блокирует apply preflight вне настроенного окна. |
| `execution_window_start_utc` | Начальный UTC hour, включительно. | Активно только при `enforce_execution_window=true`. Диапазон `0..23`. |
| `execution_window_end_utc` | Конечный UTC hour, не включительно. | Активно только при `enforce_execution_window=true`. Диапазон `1..24`. |

### `runtime`

| Поле | Значение | Эффект изменения |
| --- | --- | --- |
| `require_manual_approval` | Требует подтверждение оператора перед каждым CLI batch. | Безопаснее для ручных запусков. `force` mode и API apply не спрашивают подтверждение. |
| `check_allocator` | Включает allocator dry-run checks во время simulation. | Если true, simulation ловит OpenSearch allocation rejections. Если false, остаются только локальные проверки. |
| `data_dir` | Каталог persisted execution state. | File execution store пишет сюда `executions.json`. Для production используйте durable storage. |

### `api`

| Поле | Значение | Эффект изменения |
| --- | --- | --- |
| `enabled` | Разрешает запуск `serve-api`. | Если false, API startup блокируется. CLI commands не затрагиваются. |
| `listen` | HTTP listen address. | `127.0.0.1:PORT` ограничивает API локальной машиной; `:PORT` слушает все интерфейсы. |
| `read_timeout` | HTTP server read timeout. | Меньше значение защищает от slow clients; больше переносит медленные uploads. |
| `write_timeout` | HTTP server write timeout. | Должен покрывать медленные responses для plan/simulate/apply-start. |

### `observability`

| Поле | Значение | Эффект изменения |
| --- | --- | --- |
| `audit_sink_path` | Путь к structured audit log. | Parent directory создается при startup. Для auditability используйте persistent storage. |
| `log_level` | Verbosity runtime logs. | Для штатной эксплуатации используйте `info`; более подробные уровни могут увеличить объем логов, если поддержаны logger. |

## Практические рекомендации по tuning

- Чтобы снизить operational risk, уменьшайте `limits.max_concurrent_moves` и `limits.max_data_gb_per_batch`, увеличивайте `limits.cooldown_seconds`, оставляйте `runtime.require_manual_approval=true` и `policy.stop_on_*` включенными.
- Чтобы сильнее фокусироваться на disk balance, увеличивайте `planner.node_balance_weight_disk` и `planner.move_score_weight_disk_gap`.
- Чтобы сильнее фокусироваться на shard-count balance, увеличивайте `planner.node_balance_weight_shards` и `planner.move_score_weight_shard_gap`.
- Чтобы избегать больших или primary moves, увеличивайте `planner.large_shard_penalty_multiplier`, `planner.move_score_weight_size` и `planner.move_score_primary_penalty`.
- Чтобы агрессивнее освобождать перегруженные ноды, задайте реалистичный `planner.target_free_gb_per_node`, увеличьте `planner.node_balance_weight_pressure` и настройте `planner.move_score_pressure_size_reward`.
- Чтобы оставлять больше свободного места на target nodes, увеличивайте `planner.low_watermark_safety_margin_percent`.
