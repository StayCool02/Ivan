#!/usr/bin/env python3
"""
parking_app_full.py
Полнофункциональный Tkinter GUI для работы с твоей БД парковки.
Поддерживает: просмотр, фильтрацию, CRUD, спецзапросы, бэкап (pg_dump), экспорт TXT, авторизация (admin/user),
горячие клавиши (Ctrl+A/V/D/U/Q/S/B/E), F10 и Alt+буквы, пагинацию стрелками и Prev/Next.
"""

import os
import sys
import subprocess
import csv
import getpass
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog, filedialog
import psycopg2
from psycopg2 import sql
from datetime import datetime

# -------------------------
# Конфигурация подключения
# ПОДСТАВЬ СВОИ ДАННЫЕ
# -------------------------
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "dbname": "parking_lab2",
    "user": "postgres",
    "password": "0511"
}

PAGE_SIZE = 20  # строк на страницу

# -------------------------
# Вспомогательные функции DB
# -------------------------
def get_conn():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        messagebox.showerror("DB Error", f"Не удалось подключиться к БД: {e}")
        return None

def fetch_all(query, params=None):
    conn = get_conn()
    if not conn:
        return None, "Нет соединения"
    try:
        cur = conn.cursor()
        cur.execute(query, params or ())
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return (cols, rows), None
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        return None, str(e)

def execute(query, params=None):
    conn = get_conn()
    if not conn:
        return "Нет соединения"
    try:
        cur = conn.cursor()
        cur.execute(query, params or ())
        conn.commit()
        cur.close()
        conn.close()
        return None
    except Exception as e:
        try:
            # Откат изменений в случае ошибки
            conn.rollback() 
            conn.close()
        except:
            pass
        return str(e)

# -------------------------
# Users table helper (authorization)
# -------------------------
# [Оставлено без изменений]

def ensure_users_table():
    """Если таблицы users нет — создаём и вставляем примеры."""
    q = """SELECT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema='public' AND table_name='users'
    );"""
    res, err = fetch_all(q)
    if err:
        return False, err
    exists = res[1][0][0]
    if not exists:
        # создаём таблицу и вставим две учетки: admin/admin и user/user
        qcreate = """
        CREATE TABLE users(
            login varchar PRIMARY KEY,
            pass varchar NOT NULL,
            role varchar NOT NULL check (role in ('admin','user'))
        );
        """
        e = execute(qcreate)
        if e:
            return False, e
        e = execute("INSERT INTO users(login, pass, role) VALUES (%s,%s,%s)", ("admin","admin","admin"))
        if e:
            return False, e
        e = execute("INSERT INTO users(login, pass, role) VALUES (%s,%s,%s)", ("user","user","user"))
        if e:
            # если вставка не удалась, это не критично
            pass
    return True, None

def check_credentials(login, password):
    # возвращает роль или None
    conn = get_conn()
    if not conn:
        return None, "Нет соединения"
    try:
        cur = conn.cursor()
        cur.execute("SELECT role FROM users WHERE login=%s AND pass=%s", (login, password))
        r = cur.fetchone()
        cur.close()
        conn.close()
        if not r:
            return None, None
        return r[0], None
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        return None, str(e)

# -------------------------
# GUI: Login dialog
# -------------------------
class LoginDialog(simpledialog.Dialog):
    def body(self, master):
        tk.Label(master, text="Login:").grid(row=0, column=0, sticky="e")
        tk.Label(master, text="Password:").grid(row=1, column=0, sticky="e")
        self.ent_login = tk.Entry(master)
        self.ent_pass = tk.Entry(master, show="*")
        self.ent_login.grid(row=0, column=1, padx=5, pady=5)
        self.ent_pass.grid(row=1, column=1, padx=5, pady=5)
        return self.ent_login

    def buttonbox(self):
        box = tk.Frame(self)
        b1 = tk.Button(box, text="OK", width=10, command=self.ok, default=tk.ACTIVE)
        b2 = tk.Button(box, text="Cancel", width=10, command=self.cancel)
        b1.pack(side="left", padx=5, pady=5)
        b2.pack(side="left", padx=5, pady=5)
        self.bind("<Return>", lambda e: self.ok())
        self.bind("<Escape>", lambda e: self.cancel())
        box.pack()

    def apply(self):
        self.result = (self.ent_login.get().strip(), self.ent_pass.get().strip())

# -------------------------
# Main application
# -------------------------
class ParkingApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Parking App — Retro UI (Full)")
        self.root.geometry("1000x640")
        self.user = None
        self.role = None
        self.last_query_result = None  # tuple (cols, rows)
        self.current_table = None
        self.page = 0
        self.page_size = PAGE_SIZE
        self.total_rows = 0
        self.filter_col = None
        self.filter_val = None

        # --- СЛОВАРЬ ДЛЯ РУЧНЫХ PK ---
        self.manual_pk_tables = {
            #"employee", "parking_event", "parking_place", 
            "car", "driver", 
            "car_on_parking", "event_car", "event_empl"
        }

        ok, err = ensure_users_table()
        if not ok:
            messagebox.showwarning("Users table", f"Не удалось обеспечить таблицу users: {err}")

        if not self.login():
            root.destroy()
            return

        self.build_menu()
        self.build_ui()
        self.bind_keys()
        self.show_table("parking_place")  # стартовая таблица

    # -------------------------
    # Login flow (unchanged)
    # -------------------------
    def login(self):
        d = LoginDialog(self.root)
        if not d.result:
            return False
        login, pwd = d.result
        role, err = check_credentials(login, pwd)
        if err:
            messagebox.showerror("Ошибка", f"Ошибка проверки учётных данных: {err}")
            return False
        if role is None:
            messagebox.showerror("Ошибка", "Неправильный логин или пароль")
            return self.login()  # повторить
        self.user = login
        self.role = role
        self.root.title(f"Parking App — User: {self.user} ({self.role})")
        return True

    # -------------------------
    # Menu build (unchanged)
    # -------------------------
    def build_menu(self):
        menubar = tk.Menu(self.root)

        file_menu = tk.Menu(menubar, tearoff=False)
        file_menu.add_command(label="Save last result (Ctrl+S)", underline=0, accelerator="Ctrl+S", command=self.save_last_result)
        file_menu.add_command(label="Backup DB (Ctrl+B)", underline=0, accelerator="Ctrl+B", command=self.backup_db)
        file_menu.add_separator()
        file_menu.add_command(label="Exit (Ctrl+E)", underline=1, accelerator="Ctrl+E", command=self.quit_app)
        menubar.add_cascade(label="File", menu=file_menu, underline=0)  # Alt+F

        tables_menu = tk.Menu(menubar, tearoff=False)
        tables_menu.add_command(label="Employee (F1)", underline=0, accelerator="F1", command=lambda: self.show_table("employee"))
        tables_menu.add_command(label="Parking_place (F2)", underline=0, accelerator="F2", command=lambda: self.show_table("parking_place"))
        tables_menu.add_command(label="Cars on parking (F3)", underline=0, accelerator="F3", command=self.cmd_show_cars_on_parking)
        tables_menu.add_command(label="Parking_event (F4)", underline=0, accelerator="F4", command=lambda: self.show_table("parking_event"))
        menubar.add_cascade(label="Tables", menu=tables_menu, underline=1)  # Alt+T

        ops_menu = tk.Menu(menubar, tearoff=False)
        ops_menu.add_command(label="Add record (Ctrl+A)", underline=0, accelerator="Ctrl+A", command=self.cmd_add)
        ops_menu.add_command(label="View (Ctrl+V)", underline=0, accelerator="Ctrl+V", command=self.cmd_view)
        ops_menu.add_command(label="Delete (Ctrl+D)", underline=0, accelerator="Ctrl+D", command=self.cmd_delete)
        ops_menu.add_command(label="Update (Ctrl+U)", underline=0, accelerator="Ctrl+U", command=self.cmd_update)
        ops_menu.add_separator()
        ops_menu.add_command(label="Special queries (Ctrl+Q)", underline=0, accelerator="Ctrl+Q", command=self.cmd_special_query)
        menubar.add_cascade(label="Ops", menu=ops_menu, underline=1)  # Alt+O

        help_menu = tk.Menu(menubar, tearoff=False)
        help_menu.add_command(label="About", command=lambda: messagebox.showinfo("About", "Parking App\nУчебный проект"))
        menubar.add_cascade(label="Help", menu=help_menu, underline=1)  # Alt+H

        self.root.config(menu=menubar)
        # Bind F10 to focus menu
        self.root.bind("<F10>", lambda e: self.root.focus_force())

    # -------------------------
    # UI build (unchanged)
    # -------------------------
    def build_ui(self):
        # Top bar (retro)
        top = tk.Frame(self.root, bg="#0033aa", height=34)
        top.pack(fill="x")
        tk.Label(top, text="  Parking App — (F1..F4: tables)    Alt+T: Tables  Alt+O: Ops  F10: Menu",
                 fg="white", bg="#0033aa", font=("Consolas", 11)).pack(pady=6)

        # Main area
        main = tk.Frame(self.root)
        main.pack(expand=True, fill="both")

        left = tk.Frame(main, width=240, bg="#efefef")
        left.pack(side="left", fill="y")

        tk.Label(left, text="COMMANDS", bg="#efefef", font=("Consolas", 12, "bold")).pack(pady=8)
        btns = [
            ("F1 Employees", lambda: self.show_table("employee")),
            ("F2 Places", lambda: self.show_table("parking_place")),
            ("F3 Cars on parking", self.cmd_show_cars_on_parking),
            ("F4 Events", lambda: self.show_table("parking_event")),
            ("Filter...", self.open_filter_dialog),
            ("Special Q (Ctrl+Q)", self.cmd_special_query),
        ]
        for t,cmd in btns:
            b = tk.Button(left, text=t, anchor="w", command=cmd)
            b.pack(fill="x", padx=6, pady=3)

        # Center/Right area: Treeview + controls
        right = tk.Frame(main)
        right.pack(side="left", expand=True, fill="both")

        # status / hint
        self.hint = tk.Label(right, text=f"User: {self.user} | Role: {self.role}", anchor="w")
        self.hint.pack(fill="x", padx=6, pady=(6,0))

        # Treeview frame
        self.tree_frame = tk.Frame(right)
        self.tree_frame.pack(expand=True, fill="both", padx=6, pady=6)

        # Bottom controls: pagination + actions
        bottom = tk.Frame(self.root, relief="sunken", bd=1)
        bottom.pack(fill="x")
        self.status_label = tk.Label(bottom, text="Ready", anchor="w")
        self.status_label.pack(side="left", padx=6)

        # pagination controls on right
        pageframe = tk.Frame(bottom)
        pageframe.pack(side="right", padx=6)
        self.prev_btn = tk.Button(pageframe, text="← Prev", command=self.prev_page)
        self.prev_btn.pack(side="left", padx=2)
        self.page_label = tk.Label(pageframe, text="Page 0/0")
        self.page_label.pack(side="left", padx=2)
        self.next_btn = tk.Button(pageframe, text="Next →", command=self.next_page)
        self.next_btn.pack(side="left", padx=2)

        # Tree placeholder
        self.tree = None
        self.vscroll = None
        self.hscroll = None

        # Bind arrow keys for paging
        self.root.bind("<Left>", lambda e: self.prev_page())
        self.root.bind("<Right>", lambda e: self.next_page())

    # -------------------------
    # Key bindings (hotkeys Ctrl+...) (unchanged)
    # -------------------------
    def bind_keys(self):
        self.root.bind_all("<Control-a>", lambda e: self.cmd_add())
        self.root.bind_all("<Control-A>", lambda e: self.cmd_add())
        self.root.bind_all("<Control-v>", lambda e: self.cmd_view())
        self.root.bind_all("<Control-V>", lambda e: self.cmd_view())
        self.root.bind_all("<Control-d>", lambda e: self.cmd_delete())
        self.root.bind_all("<Control-D>", lambda e: self.cmd_delete())
        self.root.bind_all("<Control-u>", lambda e: self.cmd_update())
        self.root.bind_all("<Control-U>", lambda e: self.cmd_update())
        self.root.bind_all("<Control-q>", lambda e: self.cmd_special_query())
        self.root.bind_all("<Control-Q>", lambda e: self.cmd_special_query())
        self.root.bind_all("<Control-s>", lambda e: self.save_last_result())
        self.root.bind_all("<Control-S>", lambda e: self.save_last_result())
        self.root.bind_all("<Control-b>", lambda e: self.backup_db())
        self.root.bind_all("<Control-B>", lambda e: self.backup_db())
        self.root.bind_all("<Control-e>", lambda e: self.quit_app())
        self.root.bind_all("<Control-E>", lambda e: self.quit_app())

    # -------------------------
    # Table display / pagination (unchanged)
    # -------------------------
    def show_table(self, table_name):
        self.current_table = table_name
        self.page = 0
        self.filter_col = None
        self.filter_val = None
        self.reload_page()

    def reload_page(self):
        if not self.current_table:
            return
        # count total
        # fetch_all expects string, so let's do simple:
        res, err = fetch_all(f"SELECT count(*) FROM {self.current_table}")
        if err:
            self.status_label.config(text=f"Ошибка: {err}")
            return
        total = res[1][0][0] if res and res[1] else 0
        self.total_rows = total
        total_pages = max(1, (total + self.page_size -1)//self.page_size)
        if self.page >= total_pages:
            self.page = total_pages -1
        offset = self.page * self.page_size
        q = f"SELECT * FROM {self.current_table}"
        params = []
        if self.filter_col and self.filter_val is not None:
            q += f" WHERE {sql.Identifier(self.filter_col).string} ILIKE %s"
            params.append(f"%{self.filter_val}%")
        q += f" ORDER BY 1 LIMIT {self.page_size} OFFSET {offset};"
        # Note: to keep it simple, we form q as string with identifiers assumed safe (table/column names)
        # but we ensure filter_col from UI is one of table columns
        res, err = fetch_all(q, params)
        if err:
            self.status_label.config(text="Ошибка: " + err)
            return
        cols, rows = res
        self.last_query_result = (cols, rows)
        self.build_tree(cols)
        self.fill_tree(rows)
        self.update_page_label()

    def build_tree(self, columns):
        # clear tree frame
        for w in self.tree_frame.winfo_children():
            w.destroy()
        self.vscroll = ttk.Scrollbar(self.tree_frame, orient="vertical")
        self.hscroll = ttk.Scrollbar(self.tree_frame, orient="horizontal")
        self.tree = ttk.Treeview(self.tree_frame, columns=columns, show="headings", yscrollcommand=self.vscroll.set, xscrollcommand=self.hscroll.set)
        self.vscroll.config(command=self.tree.yview)
        self.hscroll.config(command=self.tree.xview)
        self.vscroll.pack(side="right", fill="y")
        self.hscroll.pack(side="bottom", fill="x")
        self.tree.pack(expand=True, fill="both")
        for c in columns:
            # make heading readable
            self.tree.heading(c, text=c)
            self.tree.column(c, width=140, anchor="w")

    def fill_tree(self, rows):
        # clear
        for r in self.tree.get_children():
            self.tree.delete(r)
        for row in rows:
            safe_row = tuple("" if v is None else v for v in row)
            self.tree.insert("", "end", values=safe_row)
        self.status_label.config(text=f"Showing {len(rows)} rows (total {self.total_rows})")

    def update_page_label(self):
        total_pages = max(1, (self.total_rows + self.page_size -1)//self.page_size)
        self.page_label.config(text=f"Page {self.page+1}/{total_pages}")

    def prev_page(self):
        if self.page > 0:
            self.page -= 1
            self.reload_page()

    def next_page(self):
        total_pages = max(1, (self.total_rows + self.page_size -1)//self.page_size)
        if self.page < total_pages -1:
            self.page += 1
            self.reload_page()

    # -------------------------
    # Commands: view / add / delete / update / special queries
    # -------------------------
    def cmd_view(self):
        # re-run current view (Ctrl+V)
        self.reload_page()

    def cmd_add(self):
        if self.role != "admin":
            messagebox.showwarning("Permission", "Добавление доступно только администратору")
            return
        if not self.current_table:
            messagebox.showinfo("Info", "Выберите таблицу")
            return
        
        cols, pk_col = self.get_table_columns_and_pk(self.current_table)
        
        if not cols:
            messagebox.showerror("Error", "Не удалось получить колонки.")
            return
        
        # ### [ИЗМЕНЕНИЕ 1: Логика пропуска PK]
        
        # Если таблица требует ручного ввода PK, мы не пропускаем PK.
        is_manual_pk = self.current_table in self.manual_pk_tables

        # Пропускаем PK только если он не ручной (т.е. предполагаем SERIAL)
        skip_cols = []
        if pk_col and not is_manual_pk:
            skip_cols = [pk_col]

        # ### [Конец изменения 1]

        # Получаем данные от пользователя
        # Если is_manual_pk = True, то pk_col будет показан и запрошен
        vals = self.open_record_editor(f"Add to {self.current_table}", cols, {}, skip_cols=skip_cols)
        
        if vals is None:
            return  # cancelled
        
        # Фильтруем колонки и значения для вставки
        insert_cols = [c for c in cols if c not in skip_cols]
        insert_vals = [vals.get(c) for c in insert_cols]
        
        # build insert
        col_names = ",".join([f'"{c}"' for c in insert_cols])
        placeholders = ",".join(["%s"]*len(insert_cols))
        
        q = f'INSERT INTO "{self.current_table}" ({col_names}) VALUES ({placeholders})'
        e = execute(q, insert_vals)
        
        if e:
            # Теперь, если ошибка, пользователь увидит, что ему нужно вводить PK
            messagebox.showerror("Error", f"Insert failed. Возможно, нужно ввести уникальное значение для PK: {e}")
        else:
            messagebox.showinfo("OK", "Запись добавлена")
            self.reload_page()

    def cmd_delete(self):
        if self.role != "admin":
            messagebox.showwarning("Permission", "Удаление доступно только администратору")
            return
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Info", "Выделите строку для удаления")
            return
        row = self.tree.item(sel[0])["values"]
        cols = self.last_query_result[0]
        # assume first column is PK
        pk_col = cols[0]
        pk_val = row[0]
        if messagebox.askyesno("Confirm", f"Удалить запись {pk_val} из {self.current_table}?"):
            q = f'DELETE FROM "{self.current_table}" WHERE "{pk_col}" = %s'
            e = execute(q, (pk_val,))
            if e:
                messagebox.showerror("Error", f"Delete failed: {e}")
            else:
                messagebox.showinfo("OK", "Удалено")
                self.reload_page()

    def cmd_update(self):
        if self.role != "admin":
            messagebox.showwarning("Permission", "Обновление доступно только администратору")
            return
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Info", "Выделите строку для редактирования")
            return
        row = self.tree.item(sel[0])["values"]
        cols = self.last_query_result[0]
        data = {cols[i]: row[i] for i in range(len(cols))}
        
        # ### [ИЗМЕНЕНИЕ 2: Запрет редактирования PK]
        cols_all, pk_col = self.get_table_columns_and_pk(self.current_table)
        
        if not cols_all:
             messagebox.showerror("Error", "Не удалось получить колонки для обновления.")
             return
        # В режиме Update Primary Key (PK) редактировать нельзя.
        # Это предотвратит неявные ошибки ссылочной целостности.
        skip_cols = [pk_col] if pk_col in cols_all else []
        
        newvals = self.open_record_editor(f"Update {self.current_table}", cols, data, skip_cols=skip_cols)
        
        if newvals is None:
            return
            
        # update by PK = first column (или pk_col, если известен)
        pk_col_name = pk_col if pk_col else cols[0]
        pk_val = data[pk_col_name]
        
        # Формируем SET-часть, исключая PK
        update_cols = [c for c in cols if c != pk_col_name]
        set_clause = ", ".join([f'"{c}" = %s' for c in update_cols])
        
        # Параметры: сначала значения обновляемых колонок, потом значение PK
        params = [newvals[c] for c in update_cols] + [pk_val]
        
        q = f'UPDATE "{self.current_table}" SET {set_clause} WHERE "{pk_col_name}" = %s'
        e = execute(q, params)
        
        if e:
            messagebox.showerror("Error", f"Update failed: {e}")
        else:
            messagebox.showinfo("OK", "Обновлено")
            self.reload_page()

    def cmd_special_query(self):
        # simple dialog with choices (unchanged)
        choice = simpledialog.askstring("Special queries", "1: Cars on floor\n2: Events by date (YYYY-MM-DD)\n3: Occupancy per floor\nChoose number:")
        if not choice:
            return
        if choice.strip() == "1":
            floor = simpledialog.askinteger("Floor", "Enter floor number:")
            if floor is None:
                return
            q = """
            SELECT p.p_number, cop.car_number, car.mark, car.model
            FROM Parking_place p
            JOIN Car_on_parking cop ON p.p_number = cop.parking_number
            JOIN Car car ON car.c_number = cop.car_number
            WHERE p.floor = %s
            ORDER BY p.p_number;
            """
            res, err = fetch_all(q, (floor,))
            if err:
                messagebox.showerror("Error", err)
                return
            cols, rows = res
            self.last_query_result = (cols, rows)
            self.build_tree(cols)
            self.fill_tree(rows)
        elif choice.strip() == "2":
            date = simpledialog.askstring("Date", "Enter date (YYYY-MM-DD):")
            if not date:
                return
            q = "SELECT e.id, e.e_date, e.e_time, e.event, ec.car_number FROM Parking_event e LEFT JOIN Event_car ec ON e.id = ec.event_id WHERE e.e_date = %s;"
            res, err = fetch_all(q, (date,))
            if err:
                messagebox.showerror("Error", err)
                return
            cols, rows = res
            self.last_query_result = (cols, rows)
            self.build_tree(cols)
            self.fill_tree(rows)
        elif choice.strip() == "3":
            q = """
            SELECT p.floor, count(cop.car_number) as occupied
            FROM Parking_place p
            LEFT JOIN Car_on_parking cop ON p.p_number = cop.parking_number
            GROUP BY p.floor ORDER BY p.floor;
            """
            res, err = fetch_all(q)
            if err:
                messagebox.showerror("Error", err)
                return
            cols, rows = res
            self.last_query_result = (cols, rows)
            self.build_tree(cols)
            self.fill_tree(rows)
        else:
            messagebox.showinfo("Info", "Unknown choice")

    # -------------------------
    # Helpers: open record editor dialog
    # -------------------------
    def open_record_editor(self, title, cols, data, skip_cols=[]): # ### [ИЗМЕНЕНИЕ 3: Добавлен skip_cols]
        # data: dict col->value (existing) or {}
        dlg = tk.Toplevel(self.root)
        dlg.title(title)
        dlg.transient(self.root)
        dlg.grab_set()
        entries = {}
        frm = tk.Frame(dlg)
        frm.pack(padx=8, pady=8)
        r = 0
        for c in cols:
            if c in skip_cols: # ### [ИЗМЕНЕНИЕ 4: Пропускаем колонку]
                continue
                
            tk.Label(frm, text=c).grid(row=r, column=0, sticky="e", padx=4, pady=2)
            e = tk.Entry(frm, width=40)
            e.grid(row=r, column=1, sticky="w", padx=4, pady=2)
            
            # Для UPDATE - вставляем текущее значение
            initial_value = "" if data.get(c) is None else str(data.get(c))
            e.insert(0, initial_value)
            
            # Если поле - часть ключа, но не пропускается, его нельзя менять
            if c in self.get_pk_cols(self.current_table) and not skip_cols:
                e.config(state='readonly')
                
            entries[c] = e
            r += 1
            
        # buttons
        btnf = tk.Frame(dlg)
        btnf.pack(pady=6)
        result = {"ok": False, "values": None}
        def on_ok():
            # Собираем значения только для тех колонок, которые были показаны
            vals = {}
            for c in cols:
                if c in entries:
                    vals[c] = entries[c].get().strip()
                elif c in skip_cols and c in data:
                    # Для пропускаемой колонки (PK при UPDATE) берем старое значение
                    vals[c] = data[c]
                else:
                    # Если поле пропущено, но не было в data (PK при ADD), оставляем None
                    vals[c] = None

            result["ok"] = True
            result["values"] = vals
            dlg.destroy()
            
        def on_cancel():
            dlg.destroy()
            
        okb = tk.Button(btnf, text="OK", width=10, command=on_ok)
        okb.pack(side="left", padx=6)
        canb = tk.Button(btnf, text="Cancel (Esc)", width=12, command=on_cancel)
        canb.pack(side="left", padx=6)
        dlg.bind("<Return>", lambda e: on_ok())
        dlg.bind("<Escape>", lambda e: on_cancel())
        
        # Focus first field that wasn't skipped
        first_entry_col = next((c for c in cols if c not in skip_cols), None)
        if first_entry_col and first_entry_col in entries:
            entries[first_entry_col].focus_set()
            
        self.root.wait_window(dlg)
        if result["ok"]:
            return result["values"]
        return None

    # -------------------------
    # Helpers: Column and PK extraction
    # -------------------------
    # -------------------------
    # Helpers: Column and PK extraction (ИСПРАВЛЕННАЯ ВЕРСИЯ)
    # -------------------------
    # -------------------------
    # Helpers: Column and PK extraction (ФИНАЛЬНОЕ ИСПРАВЛЕНИЕ: ПЕРВАЯ КОЛОНКА = PK)
    # -------------------------
    # -------------------------
    # Helpers: Column and PK extraction
    # -------------------------
    # -------------------------
    # Helpers: Column and PK extraction (ФИНАЛЬНОЕ ИСПРАВЛЕНИЕ РЕГИСТРА)
    # -------------------------
    def get_table_columns_and_pk(self, table):
        """
        Возвращает список всех колонок. Предполагает, что первая колонка - первичный ключ.
        Возвращает: (['col1', 'col2', ...], 'pk_name' или None)
        """
        # !!! КРИТИЧНОЕ ИСПРАВЛЕНИЕ: table.lower() для information_schema !!!
        table_for_schema = table.lower() 
        
        q = f"SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position;"
        res, err = fetch_all(q, (table_for_schema,))
        
        if err:
            # Если ошибка - скорее всего, проблема с подключением или правами.
            messagebox.showerror("Metadata Error", f"Ошибка получения метаданных для {table}: {err}")
            return None, None
            
        cols = [r[0] for r in res[1]] if res and res[1] else []
        
        pk_col = None
        if cols:
             # Предполагаем, что первая колонка является первичным ключом
             pk_col = cols[0]
             
        if not cols:
             # Если cols пуст, значит, не найдена таблица (или нет колонок)
             messagebox.showerror("Error", f"Не удалось найти колонки для таблицы: {table}")
             return None, None
             
        return cols, pk_col


    def get_pk_cols(self, table):
        # Этот метод остается без изменений, он использует get_table_columns_and_pk
        _, pk_col = self.get_table_columns_and_pk(table)
        return [pk_col] if pk_col else []
        
    def get_table_columns(self, table):
        # Этот метод остается без изменений
        cols, _ = self.get_table_columns_and_pk(table)
        return cols
        
    def get_pk_cols(self, table):
        """Возвращает список PK колонок (для простоты - список из одного элемента)"""
        _, pk_col = self.get_table_columns_and_pk(table)
        return [pk_col] if pk_col else []

    def get_table_columns(self, table):
        # Старый метод теперь использует новый для обратной совместимости
        cols, _ = self.get_table_columns_and_pk(table)
        return cols

    # -------------------------
    # Filter dialog (unchanged)
    # -------------------------
    def open_filter_dialog(self):
        if not self.current_table:
            messagebox.showinfo("Info", "Выберите таблицу")
            return
        cols, _ = self.get_table_columns_and_pk(self.current_table) # Используем новый метод
        if not cols:
            messagebox.showerror("Error", "Не удалось получить колонки")
            return
        dlg = tk.Toplevel(self.root)
        dlg.title("Filter")
        dlg.transient(self.root)
        dlg.grab_set()
        tk.Label(dlg, text="Column:").grid(row=0, column=0, padx=6, pady=6)
        cb = ttk.Combobox(dlg, values=cols, state="readonly")
        cb.grid(row=0, column=1, padx=6, pady=6)
        tk.Label(dlg, text="Value (contains):").grid(row=1, column=0, padx=6, pady=6)
        ent = tk.Entry(dlg)
        ent.grid(row=1, column=1, padx=6, pady=6)
        
        def on_ok():
            vals = {}
            for c in cols:
                if c in entries:
                    input_val = entries[c].get().strip()
                    # *** ИСПРАВЛЕНИЕ 1: Преобразуем пустую строку в None ***
                    # Это предотвратит попытку Postgres преобразовать "" в integer.
                    vals[c] = input_val if input_val else None 
                elif c in skip_cols and c in data:
                    vals[c] = data[c]
                else:
                    vals[c] = None

            # *** ИСПРАВЛЕНИЕ 2: КРИТИЧЕСКАЯ ПРОВЕРКА PK ***
            # Проверяем, что ID/PK заполнен, если он ручной (manual_pk_tables)
            cols_all, pk_col = self.get_table_columns_and_pk(self.current_table)
            
            if pk_col and self.current_table in self.manual_pk_tables:
                # Проверка, что значение не None и не пустая строка
                if vals.get(pk_col) is None or str(vals.get(pk_col)).strip() == "":
                    messagebox.showerror("Validation Error", f"Поле '{pk_col}' должно быть заполнено (это Первичный ключ, не SERIAL).")
                    return # Остаемся в диалоге

            result["ok"] = True
            result["values"] = vals
            dlg.destroy()


        def on_cancel():
            dlg.destroy()
        btnf = tk.Frame(dlg)
        btnf.grid(row=2, column=0, columnspan=2, pady=8)
        tk.Button(btnf, text="OK", command=on_ok).pack(side="left", padx=6)
        tk.Button(btnf, text="Cancel", command=on_cancel).pack(side="left", padx=6)
        dlg.bind("<Return>", lambda e: on_ok())
        dlg.bind("<Escape>", lambda e: on_cancel())
        cb.focus_set()
        self.root.wait_window(dlg)

    # -------------------------
    # Save last result to TXT (unchanged)
    # -------------------------
    def save_last_result(self):
        if not self.last_query_result:
            messagebox.showinfo("Info", "Нет результата для сохранения")
            return
        cols, rows = self.last_query_result
        file = filedialog.asksaveasfilename(defaultextension=".txt", filetypes=[("Text files","*.txt")])
        if not file:
            return
        try:
            with open(file, "w", encoding="utf-8") as f:
                f.write("\t".join(cols) + "\n")
                for r in rows:
                    f.write("\t".join("" if v is None else str(v) for v in r) + "\n")
            messagebox.showinfo("Saved", f"Saved to {file}")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    # -------------------------
    # Backup DB via pg_dump (unchanged)
    # -------------------------
    def backup_db(self):
        if self.role != "admin":
            messagebox.showwarning("Permission", "Бэкап доступен только администратору")
            return
        out = filedialog.asksaveasfilename(defaultextension=".dump", filetypes=[("Custom dump","*.dump"),("SQL","*.sql")])
        if not out:
            return
        # Use PGPASSWORD env to avoid interactive prompt
        env = os.environ.copy()
        env["PGPASSWORD"] = DB_CONFIG.get("password","")
        cmd = [
            "pg_dump",
            "-h", DB_CONFIG.get("host","localhost"),
            "-p", str(DB_CONFIG.get("port",5432)),
            "-U", DB_CONFIG.get("user"),
            "-F", "c",
            "-b",
            "-v",
            "-f", out,
            DB_CONFIG.get("dbname")
        ]
        try:
            proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
            if proc.returncode == 0:
                messagebox.showinfo("Backup", f"Backup saved to {out}")
            else:
                messagebox.showerror("Backup error", f"{proc.returncode}\n{proc.stdout}\n{proc.stderr}")
        except FileNotFoundError:
            messagebox.showerror("Backup error", "pg_dump not found. Ensure PostgreSQL bin directory is in PATH.")
        except Exception as e:
            messagebox.showerror("Backup error", str(e))

    # -------------------------
    # Show cars on parking (special) (unchanged)
    # -------------------------
    def cmd_show_cars_on_parking(self):
        self.current_table = None
        q = """
            SELECT car.c_number, car.mark, car.model, driver.name as driver_name, cop.parking_number
            FROM Car_on_parking cop
            JOIN Car car ON cop.car_number = car.c_number
            LEFT JOIN Driver driver ON car.driver_name = driver.name
            ORDER BY cop.parking_number;
        """
        res, err = fetch_all(q)
        if err:
            messagebox.showerror("Error", err)
            return
        cols, rows = res
        self.last_query_result = (cols, rows)
        self.build_tree(cols)
        self.fill_tree(rows)

    # -------------------------
    # Quit (unchanged)
    # -------------------------
    def quit_app(self):
        if messagebox.askyesno("Exit", "Выход?"):
            self.root.destroy()

# -------------------------
# Run
# -------------------------
def main():
    root = tk.Tk()
    app = ParkingApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()