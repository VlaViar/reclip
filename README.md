# ReClip backend

Готовый backend для вашего `index.html`.

## Что реализовано

- `GET /` — отдает ваш исходный `index.html`
- `POST /api/info` — достает метаданные видео и список доступных форматов
- `POST /api/prepare` — создаёт асинхронную задачу на скачивание файла
- `GET /api/job/<job_id>` — статус задачи
- `GET /api/fetch/<job_id>` — скачивание готового файла
- `POST /api/cancel/<job_id>` — отмена задачи
- `GET /healthz` — простая health-check точка
- frontend и backend работают в одном контейнере

## Стек

- Python 3.14
- Flask
- yt-dlp
- ffmpeg
- gunicorn

## Важные детали

- Для MP4 backend отдает только MP4-совместимые форматы, чтобы фронтенд не показывал качество, которое нельзя стабильно собрать в ожидаемый контейнер.
- Для MP3 backend использует `yt-dlp` + ffmpeg-постобработку.
- Если пользователь включает тайминг, backend после загрузки дополнительно обрезает файл через ffmpeg.
- Плейлисты принудительно отключены: один URL = одна карточка.
- DRM-защищенные источники этот backend не обходит.

## Структура

```text
.
├── app.py
├── compose.yaml
├── Dockerfile
├── README.md
├── requirements.txt
├── static/
│   └── favicon.svg
└── templates/
    └── index.html
```

## Сборка

```bash
docker build -t reclip:latest .
```

## Запуск

```bash
docker run --rm -p 8080:8080 reclip:latest
```

Открыть:

```text
http://localhost:8080
```

## Запуск через Compose

```bash
docker compose up --build
```

## Настраиваемые переменные

- `LOG_LEVEL` — уровень логов
- `FFMPEG_PRESET` — preset для обрезки MP4, по умолчанию `veryfast`
- `FFMPEG_CRF` — CRF для обрезки MP4, по умолчанию `23`
- `YTDLP_CONCURRENT_FRAGMENT_DOWNLOADS` — число потоков, которые `yt-dlp` будет использовать для HLS/DASH-фрагментов. По умолчанию `1`; можно установить `2–4`, но не более — может сработать rate limit на источнике
- `JOB_RETENTION_SECONDS` — время хранения завершённых задач, по умолчанию `43200` (12 часов)
- `PURGE_INTERVAL_SECONDS` — интервал очистки устаревших задач, по умолчанию `300` (5 минут)

## Как frontend общается с backend

### `POST /api/info`

Тело:

```json
{
  "url": "https://example.com/video"
}
```

Пример ответа:

```json
{
  "title": "Example",
  "thumbnail": "https://...",
  "duration": 123,
  "uploader": "Channel",
  "is_vertical": false,
  "formats": [
    {"id": "137", "label": "1080p · 30 fps · без звука"}
  ],
  "m4a_formats": [],
  "mp3_formats": [
    {"id": "320", "label": "MP3 · 320 kbps"},
    {"id": "192", "label": "MP3 · 192 kbps"},
    {"id": "128", "label": "MP3 · 128 kbps"}
  ],
  "audio_caps": {"mp3": true, "m4a": true}
}
```

### `POST /api/prepare`

Тело (JSON):

```json
{
  "url": "https://example.com/video",
  "format": "video",
  "format_id": "137",
  "start_sec": "",
  "end_sec": "",
  "title": "Example"
}
```

Ответ:

```json
{
  "job_id": "abc123...",
  "status": "queued",
  "message": "В очереди"
}
```

### `GET /api/job/<job_id>`

Ответ:

```json
{
  "job_id": "abc123...",
  "status": "ready",
  "message": "Файл готов к скачиванию",
  "percent": 100,
  "ready": true
}
```

### `GET /api/fetch/<job_id>`

Возвращает готовый файл как `attachment`.

### `POST /api/cancel/<job_id>`

Отменяет задачу. Ответ: `{"ok": true}`

## Что можно улучшить потом

- cookies / auth для приватных источников
- ограничение размера и длительности
- rate limiting
