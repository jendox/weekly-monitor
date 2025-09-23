# Weekly Monitor

Инструмент командной строки для еженедельной агрегации и записи метрик Amazon (Business Report, Sellerboard, PPC-кампании, Subscribe & Save и Helium10 ранги) в Google Sheets.

## Возможности

- Загрузка и сведение данных из:
  - **Sellerboard** (current + historical) — прибыль и маржинальность.
  - **Amazon Business Report** (current + historical) — сессии, продажи, юниты, заказы.
  - **Amazon PPC (Campaigns.csv)** — spend, clicks, CTR, CPC, orders, ACOS.
  - **Subscribe & Save (SnS)** — подписки и отгруженные юниты.
  - **Helium10/Pacvue** — недельные органические ранги по ключам (асинхронная выборка с ретраями).
- Батч-загрузка в Google Sheets в нужные диапазоны (по продуктовым листам и сводным листам региона).
- Гибкий CLI-фильтр по источникам и по «текущим/историческим» данным.
- Поддержка нескольких регионов (uk, us, fr, it, es, de).

## Требования

- Python 3.12+
- Доступ к Google Sheets через Service Account (JSON credentials).
- Экспортные файлы:
  - `dashboard_entries.xlsx`, `dashboard_entries_update.xlsx` (Sellerboard)
  - `BusinessReport.csv`, `BusinessReport_update.csv`
  - `Campaigns.csv` (Amazon Ads)
  - `sns_performance_report.csv`, `sns_manage_products.csv` (SnS)

## Установка и запуск

Ставим зависимости через [uv](https://github.com/astral-sh/uv):

```bash
uv sync # установка зависимостей
```
```bash
uv run python3 main.py # запуск программы
```
