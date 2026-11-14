# Подготовка и запуск

Команды ниже выполняются из корня репозитория. На стенде использовались .NET SDK 10.0.114, runtime 10.0.14, Python 3.14.4, SQLite 3.46.1, Git и SeaweedFS 4.52. Версии NuGet закреплены в `project/packages.lock.json`. Для мониторинга на Ubuntu нужны `sysstat` (`pidstat`, `iostat`, `sar`, `sadf`) и GNU `time`.

## Данные

| Вход          | Что лежит в Git                                                                                      | Что требуется для запуска                                              |
| ------------- | ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| E1, SQLite    | `data/single_file_experiment/manifest.json`, `changes.jsonl`, `policies.json`, `single_file_tool.py` | `data/single_file_experiment/initial.sqlite`, 500 375 552 байта        |
| E2, SQLite    | `data/corpus/manifest.json`, `history/` с рецептами и расписаниями, `corpus_tool.py`                 | `data/corpus/base/sqlite/catalog.sqlite`, 134 553 600 байт             |
| E2, Zstandard | `data/corpus/natural_tool.py` и `natural_zstd/` с `initial.tar`, `commits.json` и всеми 120 патчами  | Достаточно этих файлов и Git. Начальный архив занимает 8 949 760 байт. |

Для SQLite поместите обе базы по указанным путям. Формат отдельного архива данных — `sqlite-inputs.tar.gz` с путями от корня репозитория. При наличии этого файла распаковка из корня выглядит так.

```bash
tar -xzf /path/to/sqlite-inputs.tar.gz
python3 data/single_file_experiment/single_file_tool.py verify
```

Для базы E1 ожидается SHA-256 `477b691d8e490056c7a863e4860deee78dfde6eccbd6492afdcf70569a14fe76`. Для исходного файла SQLite E2 — `a6008780ed732949bb5a49d1f23e937e0164cc6900b2104def0196a7e16c573f`. История zstd воспроизводится локально от `e23d0a96164ed0da410c7a3a334b70e56c7ed022` до `49cf51799ea051457f57243527aa2058f999f005`. Исходный корпус содержит также JSONL, ZIP и справочные файлы. Их состав описан в [паспорте корпуса](../data/corpus/README_RU.md).

## Сборка и проверка

```bash
dotnet restore project/S3Sync.csproj --locked-mode
dotnet build project/S3Sync.csproj -c Release --no-restore
dotnet run --project project/S3Sync.csproj -c Release --no-build --no-launch-profile -- self-test
dotnet run --project project/S3Sync.csproj -c Release --no-build --no-launch-profile -- self-test2
dotnet run --project project/S3Sync.csproj -c Release --no-build --no-launch-profile -- validate2 configs/config.experiment2.zstd.pilot.v3.json
```

Для SQLite E2 у `validate2` укажите `configs/config.experiment2.sqlite240.b00.v3.json`. Он проверит выбранную базу, рецепт и расписание чтений.

## SeaweedFS и серии

На исходном VPS S3 API слушал `http://127.0.0.1:8333`, использовался бакет `s3sync-lab`. Для повтора нужны работающий S3 endpoint, отдельный экспериментальный бакет и доступ к записи, чтению и удалению его объектов. Клиент получает `AWS_ACCESS_KEY_ID` и `AWS_SECRET_ACCESS_KEY` из окружения процесса. При временных учётных данных нужен также `AWS_SESSION_TOKEN`. Endpoint, бакет и регион задаются в секции `Storage`. В `configs/` пути к данным и результатам считаются от каталога самого JSON.

Первый эксперимент и трёхшаговая проверка zstd запускаются отдельно.

```bash
dotnet run --project project/S3Sync.csproj -c Release --no-build --no-launch-profile -- storage-test configs/config.experiment1.seaweed.json
dotnet run --project project/S3Sync.csproj -c Release --no-build --no-launch-profile -- run configs/config.experiment1.seaweed.json
python3 tools/run_monitored.py run2 configs/config.experiment2.zstd.pilot.v3.json --project project/S3Sync.csproj
```

Четыре полные серии E2 на Linux выполняются по очереди. Обёртка ожидает готовую сборку Release. В поставленной версии предусмотрена проверка свободного места в 10 ГиБ.

```bash
(
  set -e
  for config in configs/config.experiment2.{sqlite240,zstd120}.{b00,b08}.v3.json; do
    python3 tools/run_monitored.py run2 "$config" --project project/S3Sync.csproj
  done
)
```

Цикл использует Bash. Новые измерения появляются в `work/results/`, журналы обёртки — в `project/monitoring/` рядом с `.csproj`. Переносимые конфиги уже заданы. Утилита `create_experiment2_profiles.py` использовалась для ранней серии с тремя политиками и записывает профили рядом со своим шаблоном. Прогоны находятся в `results/raw/`. В ранних сериях E2 было три политики. Нынешний `ExperimentTwo.cs` включает ещё и `fixed-3`. Для точного повтора раннего набора нужен соответствующий исходник.

## Пересчёт по журналам

Для таблиц достаточно Python и опубликованных CSV. Скрипт сверяет версии, контрольные суммы восстановлений и учёт байтов. Новый выходной файл должен иметь свободное имя.

```bash
python3 tools/summarize_validation.py \
  results/raw/experiment2-20261108T112932Z-54b3bcb9 \
  results/raw/experiment2-20261108T121413Z-b4bf31f4 \
  --first-version 61 --last-version 240 --output work/sqlite-window.csv

python3 tools/summarize_validation.py \
  results/raw/experiment2-20261108T125821Z-4bffb648 \
  results/raw/experiment2-20261108T132853Z-cdfec2b9 \
  --first-version 0 --last-version 120 --output work/zstd-window.csv
```

`series_directory` в пересчитанной таблице содержит локальный путь. Для сравнения с историческими таблицами используйте ID серии, группу, профиль, политику, номер повтора и числовые столбцы. SHA-256 копий опубликованных результатов проверяются из корня командой `sha256sum -c results/SHA256SUMS`.
