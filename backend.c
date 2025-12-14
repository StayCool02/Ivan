#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <libpq-fe.h>

// -------------------------------------------------------------
// КОНФИГУРАЦИЯ
// -------------------------------------------------------------
// Используйте IP адрес вашего Windows-хоста
#define HOST "192.168.36.41" 
#define PORT "5432"
#define DBNAME "parking_lab2"
#define ADMIN_USER "postgres" // Используем суперпользователя для настройки
#define ADMIN_PASS "0511"
#define BACKUP_FILE "parking_lab2_backup.sql"

// -------------------------------------------------------------
// ОБЪЯВЛЕНИЕ ФУНКЦИЙ
// -------------------------------------------------------------
PGconn *connect_db(const char *user, const char *password);
int execute_simple_query(PGconn *c, const char *query);
void setup_db(void);
void populate_lookup_tables(void);
void generate_large_tables(int count);
void backup_db(void);
void restore_db(void);
void setup_logic(void);
void show_menu(void);

// -------------------------------------------------------------
// ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ LIBPQ
// -------------------------------------------------------------

PGconn *connect_db(const char *user, const char *password) {
    char conn_info[256];
    snprintf(conn_info, sizeof(conn_info), 
             "host=%s port=%s dbname=%s user=%s password=%s",
             HOST, PORT, DBNAME, user, password);

    PGconn *c = PQconnectdb(conn_info);

    if (PQstatus(c) != CONNECTION_OK) {
        // Убрали вывод пароля в ошибке
        fprintf(stderr, "Connection failed for user %s: %s\n", user, PQerrorMessage(c));
        PQfinish(c);
        return NULL;
    }
    printf("Successfully connected to DB as user %s.\n", user);
    return c;
}

int execute_simple_query(PGconn *c, const char *query) {
    PGresult *res = PQexec(c, query);

    if (PQresultStatus(res) != PGRES_COMMAND_OK && PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Query failed: %s -> %s\n", query, PQerrorMessage(c));
        PQclear(res);
        return 1;
    }

    if (PQresultStatus(res) == PGRES_TUPLES_OK) {
        printf("Query executed successfully. Rows retrieved: %d\n", PQntuples(res));
    } else {
        printf("Command executed successfully.\n");
    }
    
    PQclear(res);
    return 0;
}

// -------------------------------------------------------------
// ФУНКЦИИ BACKEND (соответствуют требованиям)
// -------------------------------------------------------------

/**
 * Опция 1. Создание ролей и прав
 */
void setup_db(void) {
    PGconn *conn = connect_db(ADMIN_USER, ADMIN_PASS);
    if (!conn) return;

    printf("\n--- 1. Создание ролей и прав (user_login / admin_login) ---\n");
    
    // Создание ролей (если их нет)
    execute_simple_query(conn, "CREATE ROLE admin_login WITH LOGIN PASSWORD 'adminpass';");
    execute_simple_query(conn, "CREATE ROLE user_login WITH LOGIN PASSWORD 'userpass';");
    
    // Выдача прав: user_login может читать, но не писать в LookUp Table (parking_place)
    // Права пользователя (на основе вашего DDL)
    execute_simple_query(conn, "GRANT SELECT ON ALL TABLES IN SCHEMA public TO user_login;");
    execute_simple_query(conn, "GRANT INSERT ON Parking_event, Event_car, Event_empl TO user_login;");
    
    // REVOKE для справочников (чтобы только admin мог писать)
    // Note: Ваша схема DDL дает SELECT на ВСЕ таблицы, это нужно для GUI.
    // Мы явно отзываем право на запись в parking_place (справочник).
    execute_simple_query(conn, "REVOKE INSERT, UPDATE, DELETE ON parking_place FROM user_login;");
    
    // Права суперпользователя (admin_login)
    execute_simple_query(conn, "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin_login;");

    PQfinish(conn);
}

/**
 * Опция 2. Наполнение справочников (LookUp Tables)
 */
void populate_lookup_tables(void) {
    PGconn *conn = connect_db(ADMIN_USER, ADMIN_PASS); 
    if (!conn) return;

    printf("\n--- 2. Наполнение справочников (parking_place, driver) ---\n");

    // =================================================================
    // INSERT в parking_place (Схема: p_number, floor, state, type)
    // =================================================================
    execute_simple_query(conn, 
        "INSERT INTO parking_place (p_number, floor, state, type) VALUES (101, 1, 'free', 'standard') ON CONFLICT (p_number) DO NOTHING;"
    );
    execute_simple_query(conn, 
        "INSERT INTO parking_place (p_number, floor, state, type) VALUES (102, 1, 'free', 'standard') ON CONFLICT (p_number) DO NOTHING;"
    );
    execute_simple_query(conn, 
        "INSERT INTO parking_place (p_number, floor, state, type) VALUES (201, 2, 'free', 'premium') ON CONFLICT (p_number) DO NOTHING;"
    );
    
    // =================================================================
    // INSERT в driver (Схема: name, gender, passport, phone_number)
    // =================================================================
    // Используем уникальные значения для NOT NULL полей
    execute_simple_query(conn, 
        "INSERT INTO driver (name, gender, passport, phone_number) VALUES ('Svetlana Ivanova', 'F', '1234567890', '+79009876543') ON CONFLICT (name) DO NOTHING;"
    );
    execute_simple_query(conn, 
        "INSERT INTO driver (name, gender, passport, phone_number) VALUES ('Maxim Smirnov', 'M', '1098765432', '+79115554433') ON CONFLICT (name) DO NOTHING;"
    );

    PQfinish(conn);
}

/**
 * Опция 3. Генерация и наполнение основной/большой таблицы (employee)
 */
void generate_large_tables(int count) {
    PGconn *conn = connect_db(ADMIN_USER, ADMIN_PASS); 
    if (!conn) return;
    
    srand(time(NULL));

    printf("\n--- 3. Генерация и вставка %d записей в employee ---\n", count);
    
    // Набор данных для генерации
    const char *names[] = {"Alexey", "Mariya", "Sergey", "Elena", "Dmitry", "Anna"};
    const char *jobs[] = {"Guard", "Cashier", "Manager", "Security"};
    const char *shifts[] = {"Day", "Night"};
    int num_names = sizeof(names) / sizeof(names[0]);
    int num_jobs = sizeof(jobs) / sizeof(jobs[0]);
    int num_shifts = sizeof(shifts) / sizeof(shifts[0]);

    int success_count = 0;

    for (int i = 0; i < count; i++) {
        // Генерация обязательных NOT NULL полей:
        // 1. name
        char random_name[64];
        snprintf(random_name, 64, "%s %d", names[rand() % num_names], (rand() % 1000) + 1);
        
        // 2. brth (Дата рождения: 1980-2000)
        char birth_date[12];
        int year = 1980 + (rand() % 21);
        int month = 1 + (rand() % 12);
        int day = 1 + (rand() % 28);
        snprintf(birth_date, 12, "%d-%02d-%02d", year, month, day);

        // 3. salary
        char salary_str[10];
        int salary = 15000 + (rand() % 35000); // 15000 до 49999
        snprintf(salary_str, 10, "%d.00", salary);

        // 4. job (из массива jobs)
        
        // 5. tel (опционально) и shift (опционально, но добавим)
        const char *shift_val = shifts[rand() % num_shifts];
        
        // Запрос с параметрами для всех NOT NULL полей и shift/tel
        const char *insert_query = 
            "INSERT INTO employee (name, brth, salary, job, shift) VALUES ($1, $2::date, $3::numeric, $4, $5)";
        
        const char *param_values[5];
        param_values[0] = random_name;
        param_values[1] = birth_date;
        param_values[2] = salary_str;
        param_values[3] = jobs[rand() % num_jobs];
        param_values[4] = shift_val;
        
        PGresult *res = PQexecParams(conn,
                                     insert_query,
                                     5,         // Количество параметров
                                     NULL,      
                                     param_values,
                                     NULL,      
                                     NULL,      
                                     0);        

        if (PQresultStatus(res) == PGRES_COMMAND_OK) {
            success_count++;
        }
        PQclear(res);
    }

    printf("Successfully inserted %d records into employee.\n", success_count);
    PQfinish(conn);
}

/**
 * Опция 4. Создание функций и триггеров
 */
void setup_logic(void) {
    PGconn *conn = connect_db(ADMIN_USER, ADMIN_PASS);
    if (!conn) return;

    printf("\n--- 4. Создание функций и триггеров ---\n");

    // Функция, которая обновляет 'state' парковочного места при добавлении машины
    // Используем column 'state' в parking_place
    const char *function_sql = 
        "CREATE OR REPLACE FUNCTION update_parking_state() RETURNS trigger AS $$ "
        "BEGIN "
        // При INSERT в car_on_parking, место считается занятым (occupied)
        "  UPDATE parking_place SET state = 'occupied' WHERE p_number = NEW.parking_number; "
        "  RETURN NEW; "
        "END; "
        "$$ LANGUAGE plpgsql;";
        
    execute_simple_query(conn, function_sql);
    
    // Триггер, который вызывает функцию после вставки в car_on_parking
    const char *trigger_sql = 
        "CREATE OR REPLACE TRIGGER parking_state_update "
        "AFTER INSERT ON car_on_parking "
        "FOR EACH ROW EXECUTE PROCEDURE update_parking_state();";
        
    execute_simple_query(conn, trigger_sql);
    
    PQfinish(conn);
}


void backup_db(void) {
    printf("\n--- 5. Создание резервной копии (pg_dump) ---\n");
    char command[512];
    snprintf(command, sizeof(command), 
             "pg_dump -U %s -d %s -h %s -p %s > %s", 
             ADMIN_USER, DBNAME, HOST, PORT, BACKUP_FILE);

    // ВНИМАНИЕ: Для работы этой команды pg_dump должен быть в PATH на Debian.
    int result = system(command);
    
    if (result == 0) {
        printf("Backup successful! Database saved to %s\n", BACKUP_FILE);
    } else {
        fprintf(stderr, "Backup failed! System call returned code %d. Проверьте, установлен ли pg_dump на Debian.\n", result);
    }
}

void restore_db(void) {
    printf("\n--- 6. Восстановление БД (psql) ---\n");
    char command[512];
    snprintf(command, sizeof(command), 
             "psql -U %s -d %s -h %s -p %s < %s", 
             ADMIN_USER, DBNAME, HOST, PORT, BACKUP_FILE);

    int result = system(command);
    
    if (result == 0) {
        printf("Restore successful!\n");
    } else {
        fprintf(stderr, "Restore failed! System call returned code %d. Проверьте, существует ли файл бэкапа.\n", result);
    }
}


// -------------------------------------------------------------
// МЕНЮ И ГЛАВНАЯ ФУНКЦИЯ (Без изменений)
// -------------------------------------------------------------

void show_menu(void) {
    printf("\n==========================================\n");
    printf("   Parking DB Backend (C/PostgreSQL) Menu\n");
    printf("==========================================\n");
    printf("1. Настройка БД (Роли, Права)\n");
    printf("2. Наполнение справочников (LookUp Tables)\n");
    printf("3. Генерация и наполнение основных таблиц\n");
    printf("4. Создание функций и триггеров\n");
    printf("5. Сохранить БД (Backup)\n");
    printf("6. Восстановить БД (Restore)\n");
    printf("0. Выход\n");
    printf("------------------------------------------\n");
    printf("Enter choice: ");
}

int main() {
    int choice;
    int records_to_generate;

    do {
        show_menu();
        
        if (scanf("%d", &choice) != 1) {
            int c;
            while ((c = getchar()) != '\n' && c != EOF);
            choice = -1; 
            printf("Invalid input. Please enter a number.\n");
            continue;
        }

        switch (choice) {
            case 1:
                setup_db();
                break;
            case 2:
                populate_lookup_tables();
                break;
            case 3:
                printf("Enter number of records to generate (e.g., 500): ");
                if (scanf("%d", &records_to_generate) != 1) {
                    printf("Invalid number of records.\n");
                    int c; while ((c = getchar()) != '\n' && c != EOF); 
                } else {
                    generate_large_tables(records_to_generate);
                }
                break;
            case 4:
                setup_logic();
                break;
            case 5:
                backup_db();
                break;
            case 6:
                restore_db();
                break;
            case 0:
                printf("Exiting backend application.\n");
                break;
            default:
                printf("Invalid choice. Please select an option from 0 to 6.\n");
                break;
        }
    } while (choice != 0);

    return 0;
}