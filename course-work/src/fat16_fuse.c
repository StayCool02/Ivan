/**
 * @file fat16_fuse.c
 * @brief Реализация драйвера файловой системы FAT16 с помощью FUSE.
 * 
 * Этот файл содержит полную реализацию файловой системы, работающей
 * поверх файла-образа диска. Поддерживаются вложенные каталоги,
 * основные файловые операции (создание, чтение, запись, удаление)
 * и корректное управление памятью.
 */

 #define FUSE_USE_VERSION 26

 #include <fuse.h>
 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <errno.h>
 #include <fcntl.h>
 #include <stddef.h>
 #include <assert.h>
 #include <time.h>
 #include <unistd.h>
 #include <sys/mman.h>
 #include <ctype.h>
 
 // --- Константы и определения файловой системы ---
 
 #define DISK_SIZE (16 * 1024 * 1024) // Размер виртуального диска: 16 МБ
 #define CLUSTER_SIZE 4096            // Размер одного кластера (блока данных)
 #define MAX_FILENAME 8               // Макс. длина имени файла (формат 8.3)
 #define MAX_EXTENSION 3              // Макс. длина расширения (формат 8.3)
 
 // Атрибуты файла/каталога (используются как битовые флаги)
 #define ATTR_READ_ONLY 0x01
 #define ATTR_HIDDEN 0x02
 #define ATTR_SYSTEM 0x04
 #define ATTR_VOLUME_ID 0x08
 #define ATTR_DIRECTORY 0x10 // Флаг, отличающий каталог от файла
 #define ATTR_ARCHIVE 0x20
 
 // Специальные значения в таблице FAT
 #define FAT_ENTRY_FREE 0x0000 // Кластер свободен
 #define FAT_ENTRY_EOF 0xFFFF  // Конец цепочки кластеров файла
 
 // --- Структуры данных ---
 
 /**
  * @brief Запись в каталоге (Directory Entry).
  * Роль: "Паспорт" или метаданные для каждого файла и подкаталога.
  * Хранит имя, атрибуты, размер и номер первого кластера с данными.
  */
 typedef struct {
     char filename[MAX_FILENAME];     // Имя файла, добитое пробелами
     char extension[MAX_EXTENSION];   // Расширение, добитое пробелами
     uint8_t attributes;              // Атрибуты (файл, каталог, скрытый и т.д.)
     uint8_t reserved[10];            // Зарезервировано стандартом
     uint16_t last_modified_time;     // Время последней модификации
     uint16_t last_modified_date;     // Дата последней модификации
     uint16_t first_cluster;          // Номер первого кластера данных. Ключевое поле!
     uint32_t file_size;              // Размер файла в байтах
 } __attribute__((packed)) fat16_dir_entry; // `packed` убирает выравнивание, чтобы структура в памяти соответствовала формату на диске
 
 /**
  * @brief Метаданные файловой системы.
  * Роль: "Суперблок" нашей ФС. Хранится в самом начале файла-образа
  * и содержит "карту" диска: смещения до ключевых областей.
  */
 typedef struct {
     uint32_t total_clusters;         // Общее количество кластеров
     uint32_t fat_start_offset;       // Смещение до начала таблицы FAT
     uint32_t root_dir_start_offset;  // Смещение до начала корневого каталога
     uint32_t data_area_start_offset; // Смещение до начала области данных
     uint32_t cluster_size;           // Размер кластера
 } fs_metadata;
 
 /**
  * @brief Структура для хранения опций, переданных через командную строку.
  * Роль: передача пути к файлу-образу из `main` в FUSE.
  */
 struct fat_options {
     const char *image_path;
 };
 
 
 // --- Глобальные переменные-указатели на области ФС в памяти ---
 
 static uint8_t *fs_memory = NULL;      // Указатель на начало всей отображенной в память области файла-образа
 static fs_metadata *meta;              // Указатель на структуру метаданных в fs_memory
 static uint16_t *fat_table;            // Указатель на таблицу FAT в fs_memory
 static fat16_dir_entry *root_dir;      // Указатель на корневой каталог в fs_memory
 static uint8_t *data_area;             // Указатель на начало области данных в fs_memory
 static int image_fd = -1;              // Файловый дескриптор файла-образа
 
 
 // --- Вспомогательные функции ---
 
 /**
  * @brief Находит первый свободный кластер.
  * Роль: выделение нового блока данных для файла или каталога.
  * @return Номер свободного кластера или 0, если свободных нет.
  */
 static uint16_t find_free_cluster() {
     for (uint16_t i = 2; i < meta->total_clusters; ++i) {
         if (fat_table[i] == FAT_ENTRY_FREE) return i;
     }
     return 0; // 0 означает, что свободные кластеры не найдены
 }
 
 /**
  * @brief Получает указатель на данные каталога по номеру его первого кластера.
  * Роль: "разрешает" номер кластера в реальный адрес в памяти.
  * @param cluster Номер кластера. 0 - специальное значение для корневого каталога.
  * @return Указатель на начало данных каталога.
  */
 fat16_dir_entry* get_dir_from_cluster(uint16_t cluster) {
     if (cluster == 0) {
         return root_dir;
     }
     return (fat16_dir_entry*)(data_area + (cluster - 2) * meta->cluster_size);
 }
 
 /**
  * @brief Преобразует обычное имя файла в формат FAT 8.3.
  * Роль: обеспечивает совместимость имен с форматом FAT.
  * @param path Исходное имя (например, "myfile.txt").
  * @param fat_name Буфер для имени в формате FAT (8 байт).
  * @param fat_ext Буфер для расширения в формате FAT (3 байта).
  */
 void to_fat_name(const char *path, char *fat_name, char *fat_ext) {
     int i = 0, j = 0;
     const char *dot = strrchr(path, '.');
     memset(fat_name, ' ', MAX_FILENAME);
     memset(fat_ext, ' ', MAX_EXTENSION);
     for (i = 0; path[i] && path[i] != '.' && i < MAX_FILENAME; ++i) fat_name[i] = toupper(path[i]);
     if (dot && *(dot+1) != '\0') {
         for (i = 0, j = 1; dot[j] && i < MAX_EXTENSION; ++i, ++j) fat_ext[i] = toupper(dot[j]);
     }
 }
 
 /**
  * @brief Ищет запись по имени внутри ОДНОГО блока данных каталога.
  * Роль: низкоуровневая операция поиска файла/подкаталога в его родителе.
  * @param dir Указатель на данные каталога, в котором ищем.
  * @param name Имя искомой записи.
  * @return Указатель на найденную запись или NULL.
  */
 static fat16_dir_entry* find_entry_in_dir(fat16_dir_entry* dir, const char* name) {
     char req_name[MAX_FILENAME];
     char req_ext[MAX_EXTENSION];
     
     if (strcmp(name, ".") == 0) {
         memset(req_name, ' ', MAX_FILENAME);
         memset(req_ext, ' ', MAX_EXTENSION);
         req_name[0] = '.';
     } else if (strcmp(name, "..") == 0) {
         memset(req_name, ' ', MAX_FILENAME);
         memset(req_ext, ' ', MAX_EXTENSION);
         req_name[0] = '.';
         req_name[1] = '.';
     } else {
         to_fat_name(name, req_name, req_ext);
     }
 
     for (unsigned int i = 0; i < (meta->cluster_size / sizeof(fat16_dir_entry)); ++i) {
         if (dir[i].filename[0] != 0x00 && dir[i].filename[0] != (char)0xE5) {
             if (strncmp(req_name, dir[i].filename, MAX_FILENAME) == 0 &&
                 strncmp(req_ext, dir[i].extension, MAX_EXTENSION) == 0) {
                 return &dir[i];
             }
         }
     }
     return NULL;
 }
 
 /**
  * @brief Главная функция навигации. Обходит путь и находит целевую запись и ее родителя.
  * Роль: "мозг" файловой системы, преобразует путь вида "/dir/file.txt" в конкретные указатели.
  * @param path Полный путь к файлу/каталогу.
  * @param parent_dir_data Выходной параметр: указатель на ДАННЫЕ родительского каталога.
  * @param target_entry Выходной параметр: указатель на ЗАПИСЬ искомого файла/каталога.
  * @return 0 в случае успеха, или код ошибки (-ENOENT, -ENOTDIR).
  */
 int find_path_entry(const char* path, fat16_dir_entry** parent_dir_data, fat16_dir_entry** target_entry) {
     if (strcmp(path, "/") == 0) {
         *parent_dir_data = NULL;
         *target_entry = root_dir;
         return 0;
     }
 
     char path_copy[1024];
     strncpy(path_copy, path, sizeof(path_copy) - 1);
     path_copy[sizeof(path_copy) - 1] = '\0';
 
     fat16_dir_entry* current_dir = root_dir;
     *parent_dir_data = root_dir;
 
     char* token = strtok(path_copy, "/");
     if (!token) return -ENOENT;
 
     while (token != NULL) {
         *parent_dir_data = current_dir;
         *target_entry = find_entry_in_dir(current_dir, token);
         char* next_token = strtok(NULL, "/");
 
         if (*target_entry == NULL) {
             if (next_token == NULL) {
                 return -ENOENT;
             } else {
                 return -ENOENT;
             }
         }
 
         if (next_token == NULL) {
             return 0;
         }
 
         if (!((*target_entry)->attributes & ATTR_DIRECTORY)) {
             return -ENOTDIR;
         }
 
         current_dir = get_dir_from_cluster((*target_entry)->first_cluster);
         token = next_token;
     }
 
     return -ENOENT;
 }
 
 /**
  * @brief Находит пустой слот для новой записи в каталоге.
  * Роль: поиск места для создания нового файла/подкаталога.
  * @param dir Указатель на данные каталога.
  * @return Указатель на свободную запись или NULL, если места нет.
  */
 fat16_dir_entry* find_free_dir_entry(fat16_dir_entry* dir) {
     for (unsigned int i = 0; i < (meta->cluster_size / sizeof(fat16_dir_entry)); ++i) {
         if (dir[i].filename[0] == 0x00 || dir[i].filename[0] == (char)0xE5) {
             return &dir[i];
         }
     }
     return NULL;
 }
 
 
 // --- Реализация операций FUSE ---
 
 /**
  * @brief Получить атрибуты файла.
  * Роль: вызывается командами `ls -l`, `stat` и др. для получения информации о файле (тип, размер, права).
  */
 static int fat_getattr(const char *path, struct stat *stbuf) {
     memset(stbuf, 0, sizeof(struct stat));
     
     if (strcmp(path, "/") == 0) {
         stbuf->st_mode = S_IFDIR | 0755;
         stbuf->st_nlink = 2;
         return 0;
     }
 
     fat16_dir_entry *parent_dir_data, *target_entry;
     int res = find_path_entry(path, &parent_dir_data, &target_entry);
     if (res != 0) return res;
 
     if (target_entry->attributes & ATTR_DIRECTORY) {
         stbuf->st_mode = S_IFDIR | 0755;
         stbuf->st_nlink = 2;
     } else {
         stbuf->st_mode = S_IFREG | 0644;
         stbuf->st_nlink = 1;
         stbuf->st_size = target_entry->file_size;
     }
     stbuf->st_uid = getuid();
     stbuf->st_gid = getgid();
     stbuf->st_mtime = time(NULL); 
     stbuf->st_atime = time(NULL);
     stbuf->st_ctime = time(NULL);
     return 0;
 }
 
 /**
  * @brief Прочитать содержимое каталога.
  * Роль: вызывается командой `ls` для получения списка файлов и подкаталогов.
  */
 static int fat_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                        off_t offset, struct fuse_file_info *fi) {
     (void)offset; (void)fi;
     fat16_dir_entry *dir_data;
 
     if (strcmp(path, "/") == 0) {
         dir_data = root_dir;
     } else {
         fat16_dir_entry *parent, *target;
         int res = find_path_entry(path, &parent, &target);
         if (res != 0) return res;
         if (!(target->attributes & ATTR_DIRECTORY)) return -ENOTDIR;
         dir_data = get_dir_from_cluster(target->first_cluster);
     }
 
     filler(buf, ".", NULL, 0);
     filler(buf, "..", NULL, 0);
 
     for (unsigned int i = 0; i < (meta->cluster_size / sizeof(fat16_dir_entry)); ++i) {
         if (dir_data[i].filename[0] != 0x00 && dir_data[i].filename[0] != (char)0xE5) {
             if (strncmp(dir_data[i].filename, ".       ", 8) == 0 || strncmp(dir_data[i].filename, "..      ", 8) == 0) {
                 continue;
             }
             char name[13];
             int k = 0;
             for (int j = 0; j < MAX_FILENAME && dir_data[i].filename[j] != ' '; ++j) name[k++] = tolower(dir_data[i].filename[j]);
             if (dir_data[i].extension[0] != ' ') {
                 name[k++] = '.';
                 for (int j = 0; j < MAX_EXTENSION && dir_data[i].extension[j] != ' '; ++j) name[k++] = tolower(dir_data[i].extension[j]);
             }
             name[k] = '\0';
             filler(buf, name, NULL, 0);
         }
     }
     return 0;
 }
 
 /**
  * @brief Создать новый каталог.
  * Роль: вызывается командой `mkdir`.
  */
 static int fat_mkdir(const char *path, mode_t mode) {
     (void)mode;
     
     char parent_path[1024] = "";
     char basename[256] = "";
     const char* last_slash = strrchr(path, '/');
     
     if (last_slash == NULL) return -EINVAL; 
     
     if (last_slash == path) { 
         if (strlen(path) == 1) return -EINVAL;
         strcpy(parent_path, "/");
         strcpy(basename, path + 1);
     } else { 
         strncpy(parent_path, path, last_slash - path);
         parent_path[last_slash - path] = '\0';
         strcpy(basename, last_slash + 1);
     }
     if (strlen(basename) == 0) return -EINVAL;
 
     fat16_dir_entry *p_ignored, *t_exists;
     if (find_path_entry(path, &p_ignored, &t_exists) == 0) {
         return -EEXIST;
     }
 
     fat16_dir_entry *parent_dir_data;
     uint16_t parent_cluster_id;
 
     if (strcmp(parent_path, "/") == 0) {
         parent_dir_data = root_dir;
         parent_cluster_id = 0; 
     } else {
         fat16_dir_entry *grandparent_data, *parent_entry;
         int res = find_path_entry(parent_path, &grandparent_data, &parent_entry);
         if (res != 0) return -ENOENT; 
         if (!(parent_entry->attributes & ATTR_DIRECTORY)) return -ENOTDIR;
         parent_cluster_id = parent_entry->first_cluster;
         parent_dir_data = get_dir_from_cluster(parent_cluster_id);
     }
 
     fat16_dir_entry* new_entry = find_free_dir_entry(parent_dir_data);
     if (!new_entry) return -ENOSPC;
     
     uint16_t new_cluster_id = find_free_cluster();
     if (new_cluster_id == 0) return -ENOSPC;
     fat_table[new_cluster_id] = FAT_ENTRY_EOF;
 
     memset(new_entry, 0, sizeof(fat16_dir_entry));
     to_fat_name(basename, new_entry->filename, new_entry->extension);
     new_entry->attributes = ATTR_DIRECTORY;
     new_entry->first_cluster = new_cluster_id;
     
     fat16_dir_entry* new_dir_data = get_dir_from_cluster(new_cluster_id);
     memset(new_dir_data, 0, CLUSTER_SIZE);
     
     fat16_dir_entry* dot_entry = &new_dir_data[0];
     memset(dot_entry, 0, sizeof(fat16_dir_entry));
     memset(dot_entry->filename, ' ', MAX_FILENAME);
     memset(dot_entry->extension, ' ', MAX_EXTENSION);
     dot_entry->filename[0] = '.';
     dot_entry->attributes = ATTR_DIRECTORY;
     dot_entry->first_cluster = new_cluster_id;
     
     fat16_dir_entry* dotdot_entry = &new_dir_data[1];
     memset(dotdot_entry, 0, sizeof(fat16_dir_entry));
     memset(dotdot_entry->filename, ' ', MAX_FILENAME);
     memset(dotdot_entry->extension, ' ', MAX_EXTENSION);
     dotdot_entry->filename[0] = '.';
     dotdot_entry->filename[1] = '.';
     dotdot_entry->attributes = ATTR_DIRECTORY;
     dotdot_entry->first_cluster = parent_cluster_id;
 
     return 0;
 }
 
 /**
  * @brief Создать новый пустой файл.
  * Роль: вызывается при открытии файла с флагом O_CREAT (например, `touch`, `> file.txt`).
  */
 static int fat_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
     (void)mode; (void)fi;
     fat16_dir_entry *parent_dir_data, *target_entry;
     int res = find_path_entry(path, &parent_dir_data, &target_entry);
     if (res == 0) return -EEXIST;
     if (res != -ENOENT) return res;
 
     const char* basename = strrchr(path, '/');
     basename = (basename == NULL) ? path : basename + 1;
 
     fat16_dir_entry* new_entry = find_free_dir_entry(parent_dir_data);
     if (!new_entry) return -ENOSPC;
 
     memset(new_entry, 0, sizeof(fat16_dir_entry));
     to_fat_name(basename, new_entry->filename, new_entry->extension);
     new_entry->attributes = ATTR_ARCHIVE;
     new_entry->first_cluster = FAT_ENTRY_EOF; 
     new_entry->file_size = 0;
     return 0;
 }
 
 /**
  * @brief Удалить файл.
  * Роль: вызывается командой `rm`.
  */
 static int fat_unlink(const char *path) {
     fat16_dir_entry *parent_dir_data, *target_entry;
     int res = find_path_entry(path, &parent_dir_data, &target_entry);
     if (res != 0) return res;
     if (target_entry->attributes & ATTR_DIRECTORY) return -EISDIR;
 
     uint16_t cluster = target_entry->first_cluster;
     while(cluster != 0 && cluster != FAT_ENTRY_EOF) {
         uint16_t next_cluster = fat_table[cluster];
         fat_table[cluster] = FAT_ENTRY_FREE;
         cluster = next_cluster;
     }
     target_entry->filename[0] = (char)0xE5;
     return 0;
 }
 
 /**
  * @brief Удалить пустой каталог.
  * Роль: вызывается командой `rmdir`.
  */
 static int fat_rmdir(const char *path) {
     if (strcmp(path, "/") == 0) {
         return -EBUSY;
     }
     fat16_dir_entry *parent_dir_data, *target_entry;
     int res = find_path_entry(path, &parent_dir_data, &target_entry);
     if (res != 0) return res;
     if (!(target_entry->attributes & ATTR_DIRECTORY)) return -ENOTDIR;
 
     fat16_dir_entry* dir_data = get_dir_from_cluster(target_entry->first_cluster);
     for (unsigned int i = 2; i < (meta->cluster_size / sizeof(fat16_dir_entry)); ++i) { 
         if (dir_data[i].filename[0] != 0x00 && dir_data[i].filename[0] != (char)0xE5) {
             return -ENOTEMPTY;
         }
     }
     
     uint16_t cluster = target_entry->first_cluster;
     if (cluster != 0) {
         fat_table[cluster] = FAT_ENTRY_FREE;
     }
     target_entry->filename[0] = (char)0xE5;
     return 0;
 }
 
 /**
  * @brief Прочитать данные из файла.
  * Роль: вызывается системным вызовом `read` (например, при `cat file.txt`).
  */
 static int fat_read(const char *path, char *buf, size_t size, off_t offset,
                     struct fuse_file_info *fi) {
     (void)fi;
     fat16_dir_entry *parent_dir_data, *entry;
     int res = find_path_entry(path, &parent_dir_data, &entry);
     if (res != 0) return res;
 
     if (entry->attributes & ATTR_DIRECTORY) return -EISDIR;
     if (offset >= entry->file_size) return 0;
     if (offset + size > entry->file_size) {
         size = entry->file_size - offset;
     }
     if (size == 0) return 0;
 
     size_t bytes_read = 0;
     uint16_t current_cluster = entry->first_cluster;
     if(current_cluster == FAT_ENTRY_EOF || current_cluster == FAT_ENTRY_FREE) return 0;
 
     off_t current_offset = 0;
     while(current_offset + meta->cluster_size <= offset) {
         current_cluster = fat_table[current_cluster];
         current_offset += meta->cluster_size;
         if(current_cluster == FAT_ENTRY_EOF || current_cluster == FAT_ENTRY_FREE) return 0;
     }
     while (bytes_read < size) {
         off_t offset_in_cluster = offset + bytes_read - current_offset;
         size_t to_read_in_cluster = meta->cluster_size - offset_in_cluster;
         if (to_read_in_cluster > size - bytes_read) to_read_in_cluster = size - bytes_read;
         uint8_t *cluster_ptr = data_area + (current_cluster - 2) * meta->cluster_size;
         memcpy(buf + bytes_read, cluster_ptr + offset_in_cluster, to_read_in_cluster);
         bytes_read += to_read_in_cluster;
         if (bytes_read < size) {
             current_cluster = fat_table[current_cluster];
             if (current_cluster == FAT_ENTRY_EOF || current_cluster == FAT_ENTRY_FREE) break;
             current_offset += meta->cluster_size;
         }
     }
     return bytes_read;
 }
 
 /**
  * @brief Записать данные в файл.
  * Роль: вызывается системным вызовом `write` (например, при `echo "..." > file.txt`).
  */
 static int fat_write(const char *path, const char *buf, size_t size, off_t offset,
                      struct fuse_file_info *fi) {
     (void)fi;
     fat16_dir_entry *parent_dir_data, *entry;
     int res = find_path_entry(path, &parent_dir_data, &entry);
     if (res != 0) return res;
     if (entry->attributes & ATTR_DIRECTORY) return -EISDIR;
     if (size == 0) return 0;
 
     size_t required_size = offset + size;
     size_t current_clusters_count = 0;
     if (entry->first_cluster != 0 && entry->first_cluster != FAT_ENTRY_EOF) {
         uint16_t c = entry->first_cluster;
         while(c != FAT_ENTRY_EOF && c != FAT_ENTRY_FREE) {
             current_clusters_count++;
             c = fat_table[c];
         }
     }
     size_t required_clusters = (required_size + meta->cluster_size - 1) / meta->cluster_size;
     if (required_size == 0) required_clusters = 0;
 
     if (required_clusters > current_clusters_count) {
         uint16_t last_cluster = 0;
         if(entry->first_cluster != 0 && entry->first_cluster != FAT_ENTRY_EOF) {
             last_cluster = entry->first_cluster;
             while(fat_table[last_cluster] != FAT_ENTRY_EOF) last_cluster = fat_table[last_cluster];
         }
         for (size_t i = current_clusters_count; i < required_clusters; ++i) {
             uint16_t new_cluster = find_free_cluster();
             if (new_cluster == 0) break;
             fat_table[new_cluster] = FAT_ENTRY_EOF;
             if (last_cluster == 0) {
                  entry->first_cluster = new_cluster;
             } else {
                  fat_table[last_cluster] = new_cluster;
             }
             last_cluster = new_cluster;
         }
     }
     
     current_clusters_count = 0;
     if (entry->first_cluster != 0 && entry->first_cluster != FAT_ENTRY_EOF) {
         uint16_t c = entry->first_cluster;
         while(c != FAT_ENTRY_EOF && c != FAT_ENTRY_FREE) {
             current_clusters_count++;
             c = fat_table[c];
         }
     }
     size_t max_writable_size = current_clusters_count * meta->cluster_size;
     if ((size_t)offset >= max_writable_size) 
     if(offset + size > max_writable_size){
         size = max_writable_size - offset;
     }
 
     size_t bytes_written = 0;
     uint16_t current_cluster = entry->first_cluster;
     off_t current_offset = 0;
     while(current_offset + meta->cluster_size <= offset) {
         current_cluster = fat_table[current_cluster];
         current_offset += meta->cluster_size;
     }
     while (bytes_written < size) {
         off_t offset_in_cluster = offset + bytes_written - current_offset;
         size_t to_write_in_cluster = meta->cluster_size - offset_in_cluster;
         if (to_write_in_cluster > size - bytes_written) to_write_in_cluster = size - bytes_written;
         uint8_t *cluster_ptr = data_area + (current_cluster - 2) * meta->cluster_size;
         memcpy(cluster_ptr + offset_in_cluster, buf + bytes_written, to_write_in_cluster);
         bytes_written += to_write_in_cluster;
         if (bytes_written < size) {
             current_cluster = fat_table[current_cluster];
             if (current_cluster == FAT_ENTRY_EOF || current_cluster == FAT_ENTRY_FREE) break;
             current_offset += meta->cluster_size;
         }
     }
     if (offset + bytes_written > entry->file_size) entry->file_size = offset + bytes_written;
     return bytes_written;
 }
 
 /**
  * @brief Обрезать или расширить файл до заданного размера.
  * Роль: вызывается системным вызовом `truncate`.
  */
 static int fat_truncate(const char* path, off_t size) {
     fat16_dir_entry *parent_dir_data, *entry;
     int res = find_path_entry(path, &parent_dir_data, &entry);
     if (res != 0) return res;
     if (entry->attributes & ATTR_DIRECTORY) return -EISDIR;
     entry->file_size = size;
     return 0;
 }
 
 /**
  * @brief Изменить время доступа и модификации.
  * Роль: вызывается командой `touch`. В данной реализации - заглушка.
  */
 static int fat_utimens(const char *path, const struct timespec ts[2]) {
     (void)ts;
     fat16_dir_entry *parent_dir_data, *target_entry;
     int res = find_path_entry(path, &parent_dir_data, &target_entry);
     return res == 0 ? 0 : res;
 }
 
 
 // --- Инициализация, уничтожение и структура операций FUSE ---
 
 /**
  * @brief Инициализация файловой системы.
  * Роль: вызывается один раз при монтировании ФС. Отвечает за открытие или создание
  * файла-образа, его отображение в память (mmap) и инициализацию глобальных указателей.
  */
 static void* fat_init(struct fuse_conn_info *conn) {
     (void)conn;
     struct fuse_context* context = fuse_get_context();
     struct fat_options *opts = (struct fat_options *)context->private_data;
     const char* image_path = opts->image_path;
     
     // Системный вызов open: пытается открыть файл-образ на чтение и запись.
     image_fd = open(image_path, O_RDWR);
     if (image_fd == -1) {
         printf("Файл-образ не найден. Создание и форматирование %s...\n", image_path);
         // Системный вызов open с флагами O_CREAT (создать) и правами 0666.
         image_fd = open(image_path, O_RDWR | O_CREAT, 0666);
         if (image_fd == -1) { perror("Не удалось создать файл-образ"); exit(1); }
         
         // Системный вызов ftruncate: "растягивает" файл до нужного размера.
         if (ftruncate(image_fd, DISK_SIZE) != 0) { perror("ftruncate не удался"); exit(1); }
         
         // Системный вызов mmap: ключевая операция! Отображает весь файл-образ
         // в память, позволяя работать с ним как с обычным массивом.
         // MAP_SHARED означает, что изменения в памяти будут записаны в файл.
         fs_memory = mmap(NULL, DISK_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, image_fd, 0);
         if (fs_memory == MAP_FAILED) { perror("mmap не удался"); exit(1); }
         
         // Форматирование "диска"
         memset(fs_memory, 0, DISK_SIZE);
         meta = (fs_metadata*)fs_memory;
         meta->cluster_size = CLUSTER_SIZE;
         uint32_t current_offset = sizeof(fs_metadata);
         meta->total_clusters = (DISK_SIZE - current_offset) / (CLUSTER_SIZE + sizeof(uint16_t));
         meta->fat_start_offset = current_offset;
         current_offset += meta->total_clusters * sizeof(uint16_t);
         meta->root_dir_start_offset = current_offset;
         root_dir = (fat16_dir_entry*)(fs_memory + meta->root_dir_start_offset);
         current_offset += meta->cluster_size;
         meta->data_area_start_offset = current_offset;
         data_area = fs_memory + meta->data_area_start_offset;
         fat_table = (uint16_t*)(fs_memory + meta->fat_start_offset);
         fat_table[0] = 0xFFF8;
         fat_table[1] = FAT_ENTRY_EOF;
         printf("Форматирование завершено.\n");
     } else {
         printf("Открытие существующего файла-образа %s...\n", image_path);
         fs_memory = mmap(NULL, DISK_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, image_fd, 0);
         if (fs_memory == MAP_FAILED) { perror("mmap для существующего файла не удался"); exit(1); }
     }
     
     // Инициализация указателей для уже существующего образа
     meta = (fs_metadata*)fs_memory;
     fat_table = (uint16_t*)(fs_memory + meta->fat_start_offset);
     root_dir = (fat16_dir_entry*)(fs_memory + meta->root_dir_start_offset);
     data_area = fs_memory + meta->data_area_start_offset;
     printf("FAT16 FUSE FS инициализирована.\n");
     return opts;
 }
 
 /**
  * @brief "Уничтожение" файловой системы.
  * Роль: вызывается один раз при демонтировании. Отвечает за сохранение данных
  * на диск и освобождение всех ресурсов.
  */
 void fat_destroy(void *private_data) {
     if (fs_memory != NULL && fs_memory != MAP_FAILED) {
         // Системный вызов msync: принудительно записывает изменения из памяти в файл.
         msync(fs_memory, DISK_SIZE, MS_SYNC);
         // Системный вызов munmap: отключает отображение файла из памяти.
         munmap(fs_memory, DISK_SIZE);
     }
     if (image_fd != -1) close(image_fd);
     
     struct fat_options *opts = (struct fat_options *)private_data;
 
     if (opts && opts->image_path) {
         // Освобождаем строку, которую для нас выделила fuse_opt_parse
         free((void *)opts->image_path);
     }
     
     // Освобождаем саму структуру опций
     free(private_data);
     
     printf("FAT16 FUSE FS демонтирована, данные сохранены.\n");
 }
 
 /**
  * @brief Главная структура FUSE, связывающая операции ФС с их реализациями.
  * Роль: "таблица переходов", которую FUSE использует для вызова наших функций.
  */
 static struct fuse_operations fat_oper = {
     .init       = fat_init,
     .destroy    = fat_destroy,
     .getattr    = fat_getattr,
     .readdir    = fat_readdir,
     .mkdir      = fat_mkdir,
     .rmdir      = fat_rmdir,
     .create     = fat_create,
     .unlink     = fat_unlink,
     .read       = fat_read,
     .write      = fat_write,
     .truncate   = fat_truncate,
     .utimens    = fat_utimens,
 };
 
 /**
  * @brief Описание наших кастомных опций командной строки для FUSE.
  */
 static const struct fuse_opt fat_fuse_opts[] = {
     { "--image=%s", offsetof(struct fat_options, image_path), 0 },
     FUSE_OPT_END
 };
 
 /**
  * @brief Главная функция программы.
  * Роль: точка входа. Парсит аргументы, инициализирует FUSE и передает
  * управление главному циклу FUSE.
  */
 int main(int argc, char *argv[]) {
     struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
     
     struct fat_options *fat_opts = malloc(sizeof(struct fat_options));
     if (fat_opts == NULL) {
         perror("malloc");
         fuse_opt_free_args(&args);
         return 1;
     }
     memset(fat_opts, 0, sizeof(struct fat_options));
 
     if (fuse_opt_parse(&args, fat_opts, fat_fuse_opts, NULL) == -1) {
         fprintf(stderr, "Ошибка парсинга опций FUSE\n");
         free(fat_opts);
         fuse_opt_free_args(&args);
         return 1;
     }
     
     int no_image_path = (fat_opts->image_path == NULL);
     
     if (no_image_path) {
         fprintf(stderr, "Ошибка: необходимо указать путь к файлу-образу с помощью --image=<path>\n");
         fprintf(stderr, "Пример: %s mnt --image=mydisk.img\n", argv[0]);
     }
     
     int ret = fuse_main(args.argc, args.argv, &fat_oper, no_image_path ? NULL : fat_opts);
     
     fuse_opt_free_args(&args);
     
     if (no_image_path) {
         free(fat_opts);
     }
 
     return ret;
 }