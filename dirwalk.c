#include <stdio.h>      // Для стандартного ввода/вывода (printf, fprintf)
#include <stdlib.h>     // Для EXIT_SUCCESS, EXIT_FAILURE, malloc/realloc/free
#include <string.h>     // Для работы со строками (strcmp, strncpy)
#include <unistd.h>     // Для функции getopt (разбор опций командной строки)
#include <dirent.h>     // Для работы с каталогами (DIR, opendir, readdir, closedir)
#include <sys/types.h>  // Для базовых типов данных
#include <sys/stat.h>   // Для stat, lstat (получение информации о файлах)
#include <errno.h>      // Для errno и strerror (обработка ошибок)
#include <libgen.h>     // Для basename (не используется в финальном коде, но полезно)

// === Маски фильтров (для выбора типа файла) ===
#define FILTER_NONE (0)
#define FILTER_LINK (1 << 0) // Флаг для символических ссылок (-l)
#define FILTER_DIR  (1 << 1) // Флаг для каталогов (-d)
#define FILTER_FILE (1 << 2) // Флаг для обычных файлов (-f)

// Максимальная длина пути (стандартное значение для POSIX)
#define PATH_MAX 4096

// === Структура для элемента, который нужно отсортировать ===
typedef struct {
    char path[PATH_MAX];    // Полный путь к файлу/каталогу
    char name[NAME_MAX + 1]; // Имя самого файла (нужно для сравнения)
} file_entry_t;

// === Глобальные переменные (настройки программы) ===
static int filter_mask = FILTER_NONE; // Какие типы файлов выводить
static int sort_output = 0;           // Флаг: 1 - если нужна сортировка (-s)

// Переменные для динамического массива (списка файлов для сортировки)
static file_entry_t *file_list = NULL; // Указатель на начало массива
static size_t list_size = 0;           // Текущее количество элементов в массиве
static size_t list_capacity = 0;       // Максимальная вместимость массива

// === Прототипы функций (объявление функций) ===
void parse_options(int argc, char *argv[], char **start_dir);
void process_dir(const char *dir_name);
int filter_entry(const char *full_path, const char *name);
void print_entry(const char *full_path);
void print_and_clear_list(void);
void add_to_list(const char *full_path, const char *name);


/**
 * @brief Проверяет, должен ли элемент быть выведен, и определяет его тип.
 * @param full_path Полный путь к элементу.
 * @param name Имя элемента (например, "файл.txt").
 * @return 1, если элемент подходит под фильтр; 0, если нет.
 */
int filter_entry(const char *full_path, const char *name) {
    struct stat stat_buf; // Структура, куда lstat запишет данные о файле
    int current_type;     // Переменная для хранения типа найденного файла

    // 1. Игнорируем специальные каталоги "." (текущий) и ".." (родительский)
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
        return 0;
    }

    // 2. Получаем информацию о файле.
    // lstat(2) получает данные о самом файле/ссылке.
    // ВАЖНО: lstat НЕ переходит по символическим ссылкам.
    if (lstat(full_path, &stat_buf) == -1) {
        fprintf(stderr, "dirwalk: Ошибка lstat для '%s': %s\n", full_path, strerror(errno));
        return 0; // Пропускаем элемент, если произошла ошибка
    }

    // 3. Определяем тип элемента с помощью макросов S_IS*
    if (S_ISLNK(stat_buf.st_mode)) {
        current_type = FILTER_LINK; // Символическая ссылка
    } else if (S_ISDIR(stat_buf.st_mode)) {
        current_type = FILTER_DIR;  // Каталог
    } else if (S_ISREG(stat_buf.st_mode)) {
        current_type = FILTER_FILE; // Обычный файл
    } else {
        return 0; // Игнорируем другие типы (сокеты, устройства, FIFO и т.д.)
    }

    // 4. Проверяем, соответствует ли тип элемента установленному фильтру
    // Используем побитовую операцию AND (&): если (current_type & filter_mask) не равно нулю,
    // значит, этот тип файла был запрошен.
    if (filter_mask & current_type) {
        return 1; // Выводим элемент
    }

    return 0; // Не выводим
}

/**
 * @brief Выводит информацию об элементе (полный путь).
 * @param full_path Полный путь к элементу.
 */
void print_entry(const char *full_path) {
    // В простейшем случае, как у find, просто печатаем путь
    printf("%s\n", full_path);
}


/**
 * @brief Добавляет элемент в динамический список для последующей сортировки.
 */
void add_to_list(const char *full_path, const char *name) {
    // Проверяем, достаточно ли места в массиве
    if (list_size >= list_capacity) {
        // Увеличиваем вместимость (начинаем с 100, потом удваиваем)
        list_capacity = (list_capacity == 0) ? 100 : list_capacity * 2;
        
        // realloc(3) пытается изменить размер ранее выделенной памяти
        file_list = (file_entry_t *)realloc(file_list, list_capacity * sizeof(file_entry_t));
        
        if (file_list == NULL) {
            perror("dirwalk: Ошибка realloc (недостаточно памяти)");
            exit(EXIT_FAILURE);
        }
    }
    
    // Копируем полный путь и имя в новую ячейку массива
    strncpy(file_list[list_size].path, full_path, PATH_MAX - 1);
    file_list[list_size].path[PATH_MAX - 1] = '\0'; // Гарантируем завершающий ноль

    strncpy(file_list[list_size].name, name, NAME_MAX);
    file_list[list_size].name[NAME_MAX] = '\0';

    list_size++; // Увеличиваем счетчик элементов
}

/**
 * @brief Рекурсивный обход каталога (основная логика).
 * @param dir_name Путь к каталогу, который нужно сканировать.
 */
void process_dir(const char *dir_name) {
    DIR *dir_ptr;           // Указатель на структуру каталога
    struct dirent *entry;   // Структура для хранения данных об одном элементе каталога
    struct stat stat_buf;   // Структура для stat/lstat
    char full_path[PATH_MAX]; // Буфер для полного пути

    // 1. Открываем каталог с помощью opendir(3)
    dir_ptr = opendir(dir_name);
    if (dir_ptr == NULL) {
        // Если не удалось открыть (например, нет прав доступа), выводим ошибку и выходим
        fprintf(stderr, "dirwalk: Не удалось открыть каталог '%s': %s\n", dir_name, strerror(errno));
        return;
    }

    // 2. Читаем элементы каталога в цикле с помощью readdir(3)
    while ((entry = readdir(dir_ptr)) != NULL) {
        
        // 2.1. Формируем полный путь: "каталог/имя_файла"
        if (snprintf(full_path, sizeof(full_path), "%s/%s", dir_name, entry->d_name) >= sizeof(full_path)) {
            // Проверка на переполнение буфера
            fprintf(stderr, "dirwalk: Слишком длинный путь\n");
            continue;
        }

        // 2.2. Проверяем фильтр (filter_entry)
        if (filter_entry(full_path, entry->d_name)) {
            if (sort_output) {
                add_to_list(full_path, entry->d_name); // Добавляем в список для сортировки
            } else {
                print_entry(full_path); // Сразу выводим
            }
        }
        
        // 2.3. Проверка, является ли элемент каталогом для рекурсии
        
        // Используем lstat, чтобы проверить тип файла
        if (lstat(full_path, &stat_buf) == -1) {
            continue; // Пропускаем, если lstat снова дал ошибку
        }

        // Если это каталог (S_ISDIR) И это не "." И это не ".."
        if (S_ISDIR(stat_buf.st_mode) && strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
            // Рекурсивный вызов: заходим в найденный подкаталог
            process_dir(full_path);
        }
    }

    // 3. Закрываем каталог с помощью closedir(3)
    if (closedir(dir_ptr) == -1) {
        fprintf(stderr, "dirwalk: Не удалось закрыть каталог '%s': %s\n", dir_name, strerror(errno));
    }
}

/**
 * @brief Разбор опций командной строки с помощью getopt(3).
 */
void parse_options(int argc, char *argv[], char **start_dir) {
    int opt;
    // Строка опций "ldfs": ожидаем опции -l, -d, -f, -s
    while ((opt = getopt(argc, argv, "ldfs")) != -1) {
        switch (opt) {
            case 'l': filter_mask |= FILTER_LINK; break;
            case 'd': filter_mask |= FILTER_DIR; break;
            case 'f': filter_mask |= FILTER_FILE; break;
            case 's': sort_output = 1; break;
            case '?': exit(EXIT_FAILURE); // getopt(3) уже выведет сообщение об ошибке
            default: break;
        }
    }

    // optind (глобальная переменная getopt) - индекс следующего аргумента
    if (optind < argc) {
        *start_dir = argv[optind]; // Начальный каталог указан
    } else {
        *start_dir = "."; // Начальный каталог не указан, используем текущий (".")
    }
    
    // Если пользователь не задал фильтры (-l, -d, -f), выводим все
    if (filter_mask == FILTER_NONE) {
        filter_mask = FILTER_LINK | FILTER_DIR | FILTER_FILE;
    }
}

/**
 * @brief Функция для qsort, сравнивает пути, используя LC_COLLATE.
 */
int compare_file_entries(const void *a, const void *b) {
    const file_entry_t *entry_a = (const file_entry_t *)a;
    const file_entry_t *entry_b = (const file_entry_t *)b;
    // strcoll(3) сравнивает строки, учитывая правила сортировки текущей локали
    return strcoll(entry_a->path, entry_b->path);
}

/**
 * @brief Сортирует собранный список, печатает его и освобождает память.
 */
void print_and_clear_list(void) {
    if (list_size > 0) {
        // Сортировка: qsort(3)
        // file_list: что сортируем; list_size: сколько элементов; sizeof(file_entry_t): размер элемента;
        // compare_file_entries: функция сравнения
        qsort(file_list, list_size, sizeof(file_entry_t), compare_file_entries);

        // Печать отсортированных элементов
        for (size_t i = 0; i < list_size; i++) {
            printf("%s\n", file_list[i].path);
        }
    }
    
    // Освобождение динамически выделенной памяти
    if (file_list != NULL) {
        free(file_list);
        file_list = NULL;
    }
}

// === MAIN: Точка входа в программу ===
int main(int argc, char *argv[]) {
    char *start_dir = NULL;

    // 1. Сначала разбираем все опции и находим стартовый каталог
    parse_options(argc, argv, &start_dir);

    // 2. Начинаем рекурсивный обход с заданного каталога
    process_dir(start_dir);

    // 3. Если была включена сортировка, выводим собранный список
    if (sort_output) {
        print_and_clear_list();
    }
    
    // Если сортировка НЕ была включена, print_and_clear_list не вызовется,
    // но в этом случае file_list и так должен быть NULL.
    // Если что-то было выделено, но сортировка не нужна, это ошибка логики, 
    // но на всякий случай проверяем и освобождаем (хотя в текущем коде это уже не нужно)
    // if (file_list != NULL) { free(file_list); }

    return EXIT_SUCCESS;
}