#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <libpq-fe.h>

// -------------------------------------------------------------
// КОНФИГУРАЦИЯ
// -------------------------------------------------------------
#define HOST "localhost"
#define PORT "5432"
#define DBNAME "parking_lab2"
#define ADMIN_USER "postgres"
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

void setup_db(void) {
    PGconn *conn = connect_db(ADMIN_USER, ADMIN_PASS);
    if (!conn) return;

    printf("\n--- 1. Создание ролей и прав ---\n");
    // Создание ролей (если их нет)
    execute_simple_query(conn, "CREATE ROLE admin_role WITH LOGIN PASSWORD 'adminpass';");
    execute_simple_query(conn, "CREATE ROLE user_role WITH LOGIN PASSWORD 'userpass';");
    
    // Выдача прав: user_role может читать, но не писать в LookUp Table (parking_place)
    execute_simple_query(conn, "GRANT SELECT ON parking_place TO user_role;");
    execute_simple_query(conn, "REVOKE INSERT, UPDATE, DELETE ON parking_place FROM user_role;");

    PQfinish(conn);
}

void populate_lookup_tables(void) {
    PGconn *conn = connect_db(ADMIN_USER, ADMIN_PASS); 
    if (!conn) return;

    printf("\n--- 2. Наполнение справочников (LookUp Tables) ---\n");

    // Пример INSERT в справочник parking_place
    execute_simple_query(conn, 
        "INSERT INTO parking_place (p_number, status, area) VALUES ('B101', 'free', 2) ON CONFLICT (p_number) DO NOTHING;"
    );
    execute_simple_query(conn, 
        "INSERT INTO parking_place (p_number, status, area) VALUES ('B102', 'free', 2) ON CONFLICT (p_number) DO NOTHING;"
    );
    
    // Пример INSERT в справочник driver
    execute_simple_query(conn, 
        "INSERT INTO driver (name, phone) VALUES ('Svetlana Ivanova', '+79009876543') ON CONFLICT (name) DO NOTHING;"
    );

    PQfinish(conn);
}

void generate_large_tables(int count) {
    PGconn *conn = connect_db(ADMIN_USER, ADMIN_PASS); 
    if (!conn) return;
    
    srand(time(NULL));

    printf("\n--- 3. Генерация и вставка %d записей в employee ---\n", count);
    
    const char *names[] = {"Alexey", "Mariya", "Sergey", "Elena", "Dmitry", "Anna"};
    const char *jobs[] = {"Guard", "Cashier", "Manager"};
    int num_names = sizeof(names) / sizeof(names[0]);
    int num_jobs = sizeof(jobs) / sizeof(jobs[0]);

    int success_count = 0;

    for (int i = 0; i < count; i++) {
        char random_name[64];
        snprintf(random_name, 64, "%s %d", names[rand() % num_names], (rand() % 1000) + 1);
        int salary = 1000 + (rand() % 1000); 

        const char *param_values[3];
        param_values[0] = random_name;
        char salary_str[10];
        snprintf(salary_str, 10, "%d", salary);
        param_values[1] = salary_str;
        param_values[2] = jobs[rand() % num_jobs];
        
        const char *insert_query = 
            "INSERT INTO employee (name, salary, job) VALUES ($1, $2::numeric, $3)";
        
        PGresult *res = PQexecParams(conn, insert_query, 3, NULL, param_values, NULL, NULL, 0);

        if (PQresultStatus(res) == PGRES_COMMAND_OK) {
            success_count++;
        }
        PQclear(res);
    }

    printf("Successfully inserted %d records into employee.\n", success_count);
    PQfinish(conn);
}

void setup_logic(void) {
    PGconn *conn = connect_db(ADMIN_USER, ADMIN_PASS);
    if (!conn) return;

    printf("\n--- 4. Создание функций и триггеров ---\n");

    const char *function_sql = 
        "CREATE OR REPLACE FUNCTION check_parking_status() RETURNS trigger AS $$ "
        "BEGIN "
        "  IF NEW.status = 'occupied' THEN "
        "    UPDATE parking_place SET status = 'occupied' WHERE p_number = NEW.parking_number; "
        "  END IF; "
        "  RETURN NEW; "
        "END; "
        "$$ LANGUAGE plpgsql;";
        
    execute_simple_query(conn, function_sql);
    
    const char *trigger_sql = 
        "CREATE OR REPLACE TRIGGER parking_status_update "
        "AFTER INSERT ON car_on_parking "
        "FOR EACH ROW EXECUTE PROCEDURE check_parking_status();";
        
    execute_simple_query(conn, trigger_sql);
    
    PQfinish(conn);
}

void backup_db(void) {
    printf("\n--- 5. Создание резервной копии (pg_dump) ---\n");
    char command[512];
    snprintf(command, sizeof(command), 
             "pg_dump -U %s -d %s -h %s -p %s > %s", 
             ADMIN_USER, DBNAME, HOST, PORT, BACKUP_FILE);

    int result = system(command);
    
    if (result == 0) {
        printf("Backup successful! Database saved to %s\n", BACKUP_FILE);
    } else {
        fprintf(stderr, "Backup failed! System call returned code %d.\n", result);
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
        fprintf(stderr, "Restore failed! System call returned code %d.\n", result);
    }
}

// -------------------------------------------------------------
// МЕНЮ И ГЛАВНАЯ ФУНКЦИЯ
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
        
        // Чтение ввода пользователя
        if (scanf("%d", &choice) != 1) {
            // Очистка буфера ввода при ошибке (для безопасности)
            int c;
            while ((c = getchar()) != '\n' && c != EOF);
            choice = -1; // Устанавливаем недопустимый выбор
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
                    // Очистка буфера после неудачного scanf
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