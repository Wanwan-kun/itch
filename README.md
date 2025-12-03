# Краткое назначение

`itch.py` — асинхронный скрипт для массового получения публичного `data.json` с страниц игр на itch.io, сохранения результата в CSV/JSON и подсчёта простых статистик:

* считывает список `app_id` из файла (строка в формате `<author>.itch.io/<game>` или `https://<author>.itch.io/<game>/`);
* для каждой записи делает `GET https://<app_id>/data.json`;
* сохраняет поля `app_id`, `id`, `title`, `original_price`, `price`, `tags` (в JSON — `tags` как список, в CSV — как строка, разделённая `|`);
* формирует текстовый отчёт с блоками:

  * общая статистика (кол-во, ошибки, пропущенные цены),
  * **Price statistics** (Count, Average, Median, Min, Max) — цены берутся из `original_price`, если он пуст — из `price`,
  * **Top N games by price** (Rank, Name, Price, Link `https://{app_id}`),
  * **Top M tags of all games** (с заголовком `Rank  Tag  Frequency`).

Скрипт использует адаптивный token-bucket rate-limiter (additive increase / multiplicative decrease) и обрабатывает `HTTP 429` (читает `Retry-After`, снижает rps и ставит «охлаждение»). Параллельность, начальная rps и поведение лимитера настраиваются флагами.

---

## Установка

Требуется Python 3.8+ и `aiohttp`:

```bash
pip install aiohttp
```

---

## Быстрый пример запуска

Файл `appids.txt` (пример):

```
allkill.itch.io/incel-server
bestgames.itch.io/yuletide-regicide
darknessminotaur.itch.io/quantumlove
```

Запуск (сохранение в CSV):

```bash
python itch.py --appids appids.txt --out-data out.csv --concurrency 3 --rps 1.0 --delay 1.0 --format csv
```

Сохранить JSON и показать топ-20 тегов:

```bash
python itch_fetch.py --appids appids.txt --out-data itch_out.json --format json --tags-top 20
```

Запуск с тонкой настройкой скорости/параллельности:

```bash
python itch_fetch.py --appids appids.txt --out-data out.csv --concurrency 4 --delay 0.5 --rps 1.0
```

Только собрать данные и сохранить (без текста-статистики):

```bash
python itch_fetch.py --appids appids.txt --out-data out.json --format json --fetch-only
```

---

## Описание флагов (важное)

> Формат: **`--flag`** `(тип)` — описание. **(по умолчанию: значение)**

### Ввод / вывод

* **`--appids <path>`** `(string)` — файл со списком `app_id` (один в строке). **(обязательно)**
* **`--out-data <path>`** `(string)` — куда сохранять данные (CSV или JSON). **(по умолчанию: `itch_out.csv`)**
* **`--format {csv|json}`** — формат сохранения. **(по умолчанию: `csv`)**
  Примечание: если `--out-data` имеет расширение `.json`, то сохранится JSON независимо от `--format`.
* **`--out-stats <path>`** `(string)` — путь для текстового отчёта со статистикой. **(по умолчанию: `itch_stats.txt`)**

### Режимы работы

* **`--fetch-only`** — только получение и сохранение данных (не выводится расширенный текстовый отчёт).
* (нет аналога `--stats-only` — скрипт всегда может читать `--out-data` и пересчитать при необходимости — но текущая реализация ориентирована на fetch→save→stats).

### Параллельность / таймаут / задержки

* **`--concurrency, -c <int>`** — число параллельных задач (semaphore). **(по умолчанию: `6`)**
* **`--timeout <int>`** — таймаут HTTP-запроса в секундах. **(по умолчанию: `30`)**
* **`--delay <float>`** — фиксированная задержка (сек) *перед* выполнением каждого запроса внутри задачи. **(по умолчанию: `0.0`)**

### Rate-limiter (Adaptive Token Bucket)

* **`--rps <float>`** — стартовая скорость (requests per second). **(по умолчанию: `1.0`)**
* **`--rps-min <float>`** — минимальная rps после снижения. **(по умолчанию: `0.1`)**
* **`--rps-max <float>`** — максимальная rps. **(по умолчанию: `5.0`)**
* **`--rps-increase <float>`** — additive step увеличения rps при успехе. **(по умолчанию: `0.05`)**
* **`--rps-decrease-factor <float>`** — multiplicative factor при 429 (умножается на текущую rps). **(по умолчанию: `0.5`)**

### Отчёты / топы

* **`--top <int>`** — сколько записей показывать в секции Top N games by price. **(по умолчанию: `5`)**
* **`--tags-top <int>`** — сколько тегов показывать в Top tags. **(по умолчанию: `10`)**

---

## Формат выходных данных

### CSV

Колонки: `app_id,id,title,original_price,price,tags`

`tags` — строка, теги разделены символом `|`.

### JSON

Каждый элемент — объект:

```json
{
  "app_id": "author.itch.io/game-slug",
  "id": 123456,
  "title": "Game Title",
  "original_price": "$3.99",
  "price": "$1.99",
  "tags": ["2d","action","pixel-art"]
}
```

---

## Как считаются цены и статистика

* Для расчётов (Price statistics, Top games by price) берётся **original_price**, если он есть; иначе берётся **price**.
* Значения `original_price` / `price` парсятся из строк типа `$3.99`, `€1,50`, `3.00` и т.д. Если парсинг не удался, значение считается отсутствующим.
* В Price statistics выводятся: Count numeric, Average, Median, Min, Max (в формате валюты `$X.XX`).
* В Top N games by price — выводятся Rank, Name (title), Price и Link (`https://{app_id}`).
* В Top M tags — считается количество вхождений тега во всех успешно полученных игр; печатается таблица с заголовком:

  ```
  Rank  Tag                       Frequency
     1  2d                             42
     2  pixel-art                      39
     ...
  ```

---

## Поведение при ошибках и ограничениях

* Скрипт обрабатывает `HTTP 429` (читает `Retry-After` если есть, уменьшает rps и ждёт). При множественных 429 скорость снижается автоматически.
* Рекомендуется не задавать слишком большой `--concurrency` и `--rps` для публичных неавторизованных запросов — это поможет избежать блокировок.

---

## Рекомендованные конфигурации

* **Безопасный старт (рекомендуется):**

  ```bash
  --concurrency 3 --rps 1.0 --delay 1.0
  ```
* **Быстрый (с риском 429):**

  ```bash
  --concurrency 10 --rps 5.0 --delay 0.0
  ```
* **Медленный / щадящий (растянуть по времени):**

  ```bash
  --concurrency 1 --rps 0.2 --delay 2.0
  ```
