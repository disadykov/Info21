-- Сначала (перед проверкой задания) необходимо создать пустую базу данных и выбрать ее.
-- Далее запустить указанные ниже конструкции 

DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

-- Создадим тестовые таблицы БД и заполним их тестовыми данными
CREATE TABLE IF NOT EXISTS Peer
(
    Nickname VARCHAR PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS ADM
(
    id         SERIAL PRIMARY KEY,
    Punishment VARCHAR,
    Peer       VARCHAR,
    CONSTRAINT peer_adm FOREIGN KEY (Peer) REFERENCES Peer (Nickname)
);

CREATE TABLE IF NOT EXISTS Penalty
(
    id       SERIAL PRIMARY KEY,
    ADM_id   INT,
    Nickname VARCHAR NOT NULL,
    "Date"   Date,
    CONSTRAINT adm_id_penalty FOREIGN KEY (ADM_id) REFERENCES ADM (id),
    CONSTRAINT nickname_penalty FOREIGN KEY (Nickname) REFERENCES Peer (Nickname)
);

CREATE TABLE TableName_1
(
    col1 VARCHAR,
    col2 VARCHAR
);

CREATE TABLE TableName_2
(
    col1 VARCHAR,
    col2 VARCHAR
);

insert into Peer
VALUES ('Celestac');
insert into Peer
VALUES ('Celestac1');
insert into Peer
VALUES ('Celestac2');
insert into Peer
VALUES ('Celestac3');
insert into Peer
VALUES ('ivettepe');
insert into Peer
VALUES ('ivettepe1');
insert into Peer
VALUES ('ivettepe2');

insert into ADM
VALUES (1, 'Punishment1', 'Celestac');
insert into ADM
VALUES (2, 'Punishment2', 'Celestac1');
insert into ADM
VALUES (3, 'Punishment3', 'Celestac2');
insert into ADM
VALUES (4, 'Punishment4', 'ivettepe');
insert into ADM
VALUES (5, 'Punishment2', 'ivettepe2');
insert into ADM
VALUES (6, 'Punishment3', 'ivettepe');
insert into ADM
VALUES (7, 'Punishment1', 'Celestac3');
insert into ADM
VALUES (8, 'Punishment5', 'Celestac');

insert into Penalty
VALUES (1, 1, 'Celestac', '10-10-2023');
insert into Penalty
VALUES (2, 2, 'Celestac1', '09-09-2023');
insert into Penalty
VALUES (3, 3, 'Celestac2', '08-08-2023');
insert into Penalty
VALUES (4, 4, 'Celestac3', '07-07-2023');
insert into Penalty
VALUES (5, 5, 'ivettepe', '06-06-2023');
insert into Penalty
VALUES (6, 6, 'ivettepe1', '05-05-2023');
insert into Penalty
VALUES (7, 7, 'ivettepe1', '04-04-2023');

insert into TableName_1
VALUES ('item1', 'item2');
insert into TableName_1
VALUES ('item3', 'item4');


-- Созадние функций для тестирования результатов заданий
CREATE OR REPLACE FUNCTION fnc_nickname(pname VARCHAR)
    RETURNS VARCHAR
AS
$$
SELECT nickname
FROM peer
WHERE nickname = pname;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION test_func()
    RETURNS TRIGGER AS
$$
BEGIN
    INSERT INTO Peer VALUES (NEW.Nickname);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_get_punishment(nickname VARCHAR)
    RETURNS VARCHAR
AS
$$
SELECT punishment
FROM adm
WHERE Peer = nickname;
$$ LANGUAGE SQL;

-- CREATE TRIGGER
CREATE TRIGGER before_insert_peer
    BEFORE INSERT
    ON Peer
    FOR EACH ROW
EXECUTE FUNCTION test_func();



-- 1) Создать хранимую процедуру, которая, не уничтожая базу данных, уничтожает все те таблицы
--    текущей базы данных, имена которых начинаются с фразы 'TableName'.

DROP PROCEDURE IF EXISTS prc_drop_table CASCADE;
CREATE OR REPLACE PROCEDURE prc_drop_table(IN tablename VARCHAR) AS
$$
BEGIN
    FOR tablename IN (SELECT table_name
                      FROM information_schema.tables
                      WHERE table_name LIKE concat(tablename, '%')
                        AND table_schema LIKE 'public')
        LOOP
            EXECUTE 'DROP TABLE IF EXISTS ' || tablename || ' CASCADE';
        END LOOP;
END ;
$$
    LANGUAGE plpgsql;

-- посмотрим список таблиц до удаления
SELECT table_name
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'public';

-- запустим процедуру и удалим из базы таблицы, именя которых начинаются с фразы 'TableName'
    CALL prc_drop_table('tablename');

-- посмотрим список таблиц до удаления - их стало меньще
SELECT table_name
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'public';


-- 2) Создать хранимую процедуру с выходным параметром, которая выводит список имен и параметров всех скалярных 
--    SQL функций пользователя в текущей базе данных. Имена функций без параметров не выводить. 
--    Имена и список параметров должны выводиться в одну строку. Выходной параметр возвращает количество найденных функций.

DROP PROCEDURE IF EXISTS prc_get_scalar_functions CASCADE;
CREATE OR REPLACE PROCEDURE prc_get_scalar_functions()
    LANGUAGE plpgsql AS
$$
BEGIN
    DECLARE
        rec RECORD;
    BEGIN
        FOR rec IN (SELECT proname                                AS function_name,
                           pg_get_function_arguments(pg_proc.oid) AS function_arguments,
                           pg_get_function_result(pg_proc.oid)    AS function_result
                    FROM pg_proc
                             JOIN pg_namespace ON pg_namespace.oid = pg_proc.pronamespace
                    WHERE pg_namespace.nspname = 'public'
                      AND pg_proc.prokind = 'f'
                      AND pg_proc.pronargs >= 0
                      AND pg_get_function_arguments(pg_proc.oid) != '')
            LOOP
                RAISE NOTICE 'function_name: % ; function_arguments: % ; function_result: %', rec.function_name, rec.function_arguments, rec.function_result;
            END LOOP;
    END;
END;
$$;

-- Запустим процедуру для проверки работоспособности:
CALL prc_get_scalar_functions();


-- 3) Создать хранимую процедуру с выходным параметром, которая уничтожает все SQL DML триггеры в текущей базе данных. 
-- Выходной параметр возвращает количество уничтоженных триггеров.

-- Проверим, какие тригеры созданы в базе
SELECT trigger_name
FROM information_schema.triggers;

DROP PROCEDURE IF EXISTS prc_destroy_all_triggers CASCADE;
CREATE OR REPLACE PROCEDURE prc_destroy_all_triggers(OUT count_destroy_triggers INT) AS
$$
DECLARE
    trg_name   text;
    table_name text;
BEGIN
    SELECT COUNT(DISTINCT trigger_name)
    INTO count_destroy_triggers
    FROM information_schema.triggers
    WHERE trigger_schema = 'public';
    FOR trg_name, table_name IN (SELECT DISTINCT trigger_name, event_object_table
                                 FROM information_schema.triggers
                                 WHERE trigger_schema = 'public')
        LOOP
            EXECUTE concat('DROP TRIGGER ', trg_name, ' ON ', table_name);
        END LOOP;
END;
$$
    LANGUAGE plpgsql;

-- Запустим процедуру и убедимся, что триггеры удалены
CALL prc_destroy_all_triggers(NULL);

-- Проверить, какие триггеры есть в базе
SELECT trigger_name
FROM information_schema.triggers;

-- 4) Создать хранимую процедуру с входным параметром, которая выводит имена и описания типа объектов 
-- (только хранимых процедур и скалярных функций), в тексте которых на языке SQL встречается строка, задаваемая параметром процедуры.

DROP PROCEDURE IF EXISTS prc_search_objects CASCADE;
CREATE OR REPLACE PROCEDURE prc_search_objects(
    IN search_string text,
    IN cursor refcursor default 'cursor') AS
$$
BEGIN
    OPEN cursor FOR
        SELECT routine_name AS object_name,
               routine_type AS object_type
        FROM information_schema.routines
        WHERE specific_schema = 'public'
          AND routine_name LIKE concat('%', search_string, '%');
END;
$$
    LANGUAGE plpgsql;

-- Проверка, что все работает
BEGIN;
CALL prc_search_objects('fnc');
FETCH ALL IN "cursor";
END;