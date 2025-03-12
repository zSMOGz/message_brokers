FROM golang:1.23-alpine

WORKDIR /app

# Устанавливаем необходимые зависимости
RUN apk add --no-cache gcc musl-dev bash

# Копируем сначала go.mod и go.sum для кэширования зависимостей
COPY go.mod go.sum ./
RUN go mod download

# Копируем код
COPY . .

# Компилируем только основное приложение
RUN go build -o mb cmd/mb/main.go

# Делаем файл исполняемым
RUN chmod +x mb

# Копируем конфигурацию
COPY config.yaml .

# Стандартная команда запуска
CMD ["./mb"]