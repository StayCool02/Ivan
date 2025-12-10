# Makefile (ИСПРАВЛЕННАЯ ВЕРСИЯ)

# Имя исполняемого файла
TARGET_NAME = fat16_fuse

# Директории
SRCDIR = src
BUILDDIR = build
RELEASEDIR = $(BUILDDIR)/release
TARGET = $(RELEASEDIR)/$(TARGET_NAME)

# Исходные файлы (автоматически найдет все .c файлы в src/)
SOURCES = $(wildcard $(SRCDIR)/*.c)

# Компилятор и флаги
CC = gcc
# ИСПРАВЛЕНО: Используем 'fuse', а не 'fuse3', и добавляем флаг _FILE_OFFSET_BITS=64
CFLAGS = -Wall -Wextra -g -D_FILE_OFFSET_BITS=64 `pkg-config fuse --cflags`
LDFLAGS = `pkg-config fuse --libs`

# Цели
.PHONY: all clean run stop help

# Цель по умолчанию: собрать проект
all: $(TARGET)

# Правило для сборки исполняемого файла
$(TARGET): $(SOURCES) | $(RELEASEDIR)
	@echo "Компиляция $< в $@"
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

# Правило для создания директории сборки
$(RELEASEDIR):
	@echo "Создание директории сборки $(RELEASEDIR)"
	@mkdir -p $(RELEASEDIR)

# Цель для очистки
clean:
	@echo "Очистка проекта..."
	@rm -rf $(BUILDDIR)

# Цели для удобного запуска и остановки
MNT ?= mnt
IMG ?= fat16_default.img

run: all
	@echo "Монтирование ФС с образом '$(IMG)' в директорию '$(MNT)'"
	@./$(TARGET) $(MNT) --image=$(IMG) -f

stop:
	@echo "Демонтирование ФС из '$(MNT)'"
	@fusermount -u $(MNT)

# Цель для справки
help:
	@echo "Доступные команды:"
	@echo "  make        - Собрать проект"
	@echo "  make clean  - Удалить сборочные артефакты"
	@echo "  make run    - Собрать и запустить ФС в foreground режиме"
	@echo "  make stop   - Демонтировать ФС"
	@echo ""
	@echo "Переменные для 'run' и 'stop':"
	@echo "  MNT=my_dir  - Указать точку монтирования (по умолч.: mnt)"
	@echo "  IMG=my.img  - Указать файл-образ диска (по умолч.: fat16_default.img)"
	@echo "Пример: make run MNT=/tmp/myfat IMG=production.img"