-- Создание таблицы Peers
-- Ник пира
-- День рождения
DROP TABLE IF EXISTS Peers CASCADE;
CREATE TABLE Peers
(
    nickname VARCHAR(40) PRIMARY KEY NOT NULL,
    birthday DATE                    NOT NULL
);

-- Создание таблицы Tasks
-- Название задания
-- Название задания, являющегося условием входа
-- Максимальное количество XP
DROP TABLE IF EXISTS Tasks CASCADE;
CREATE TABLE Tasks
(
    title       VARCHAR(40) PRIMARY KEY NOT NULL,
    parent_task VARCHAR(40),
    max_xp      INTEGER                 NOT NULL
);

-- Создание типа перечисления для статуса проверки REVIEW_STATUS
-- 0 = Start - начало проверки
-- 1 = Success - успешное окончание проверки
-- 2 = Failure - неудачное окончание проверки
DROP TYPE IF EXISTS REVIEW_STATUS CASCADE;
CREATE TYPE REVIEW_STATUS AS ENUM ('0', '1', '2');

-- Создание таблицы Checks
-- ID
-- Ник пира
-- Название задания
-- Дата проверки
DROP TABLE IF EXISTS Checks CASCADE;
CREATE TABLE Checks
(
    id   SERIAL PRIMARY KEY,
    peer VARCHAR(40) NOT NULL,
    task VARCHAR(40) NOT NULL,
    DATE DATE        NOT NULL,
    CONSTRAINT fk_name FOREIGN KEY (peer) REFERENCES Peers (nickname),
    CONSTRAINT fk_task FOREIGN KEY (task) REFERENCES Tasks (title)
);

-- Создание таблицы P2P
-- ID
-- ID проверки
-- Ник проверяющего пира
-- Статус P2P проверки
-- Время
DROP TABLE IF EXISTS P2P CASCADE;
CREATE TABLE P2P
(
    id            SERIAL PRIMARY KEY,
    "check"       INTEGER       NOT NULL,
    checking_peer VARCHAR(40)   NOT NULL,
    state         REVIEW_STATUS NOT NULL,
    TIME          TIME          NOT NULL,
    CONSTRAINT fk_check_id FOREIGN KEY ("check") REFERENCES Checks (id),
    CONSTRAINT fk_reviwer FOREIGN KEY (checking_peer) REFERENCES Peers (nickname)
);

-- Создание таблицы Verter
-- ID
-- ID проверки
-- Статус проверки Verter'ом
-- Время
DROP TABLE IF EXISTS Verter;
CREATE TABLE Verter
(
    id      SERIAL PRIMARY KEY NOT NULL,
    "check" INTEGER            NOT NULL,
    state   REVIEW_STATUS      NOT NULL,
    TIME    TIME               NOT NULL,
    CONSTRAINT fk_check_id FOREIGN KEY ("check") REFERENCES Checks (id)
);

-- Создание таблицы TransferredPoints
-- ID
-- Ник проверяющего пира
-- Ник проверяемого пира
-- Количество переданных пир поинтов за всё время (только от проверяемого к проверяющему)
DROP TABLE IF EXISTS TransferredPoints CASCADE;
CREATE TABLE TransferredPoints
(
    id            INTEGER PRIMARY KEY NOT NULL,
    checking_peer VARCHAR(40)         NOT NULL,
    checked_peer  VARCHAR(40)         NOT NULL,
    points_amount INTEGER             NOT NULL,
    CONSTRAINT fk_reviwer FOREIGN KEY (checking_peer) REFERENCES Peers (nickname),
    CONSTRAINT fk_being_reviwed FOREIGN KEY (checked_peer) REFERENCES Peers (nickname)
);

-- Создание таблицы Friends
-- ID
-- Ник первого пира
-- Ник второго пира
DROP TABLE IF EXISTS Friends CASCADE;
CREATE TABLE Friends
(
    id    INTEGER PRIMARY KEY NOT NULL,
    peer1 VARCHAR(40)         NOT NULL,
    peer2 VARCHAR(40)         NOT NULL,
    CONSTRAINT fk_peer1 FOREIGN KEY (peer1) REFERENCES Peers (nickname),
    CONSTRAINT fk_peer2 FOREIGN KEY (peer2) REFERENCES Peers (nickname)
);

-- Создание таблицы Recommendations
-- ID
-- Ник пира
-- Ник пира, к которому рекомендуют идти на проверку
DROP TABLE IF EXISTS Recommendations CASCADE;
CREATE TABLE Recommendations
(
    id               INTEGER PRIMARY KEY NOT NULL,
    peer             VARCHAR(40)         NOT NULL,
    recommended_peer VARCHAR(40)         NOT NULL,
    CONSTRAINT fk_recommender FOREIGN KEY (peer) REFERENCES Peers (nickname),
    CONSTRAINT fk_recommended FOREIGN KEY (recommended_peer) REFERENCES Peers (nickname)
);

-- Создание таблицы XP
-- ID
-- ID проверки
-- Количество полученного XP
DROP TABLE IF EXISTS XP CASCADE;
CREATE TABLE XP
(
    id        SERIAL PRIMARY KEY NOT NULL,
    "check"   INTEGER            NOT NULL,
    xp_amount INTEGER            NOT NULL,
    CONSTRAINT fk_check_id FOREIGN KEY ("check") REFERENCES Checks (id)
);

-- Создание таблицы TimeTracking
-- ID
-- Ник пира
-- Дата
-- Время
-- Состояние (1 - пришел, 2 - вышел)
DROP TABLE IF EXISTS TimeTracking CASCADE;
CREATE TABLE TimeTracking
(
    id    INTEGER PRIMARY KEY NOT NULL,
    peer  VARCHAR(40)         NOT NULL,
    DATE  DATE                NOT NULL,
    TIME  TIME                NOT NULL,
    state INTEGER             NOT NULL,
    CONSTRAINT fk_peer FOREIGN KEY (peer) REFERENCES Peers (nickname)
);

-- Процедура import_data
-- Принимает на вход имя таблицы и путь к файлу с данными, а также разделитель
CREATE OR REPLACE PROCEDURE import_data(input_table VARCHAR(255), files_path VARCHAR(255), delimiter CHAR)
    LANGUAGE PLPGSQL AS
$$
DECLARE
    id_column_is_serial BOOLEAN; -- Переменная для хранения информации о наличии столбца id типа serial
BEGIN
    -- Проверяем существование таблицы с указанным именем
    PERFORM * FROM information_schema.tables WHERE table_name = input_table;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table % does not exist', input_table; -- Выводим исключение, если таблица не существует
    END IF;

    -- Проверяем, является ли столбец id типа serial
    SELECT EXISTS (SELECT 1
                   FROM information_schema.columns
                   WHERE table_name = input_table
                     AND column_name = 'id'
                     AND data_type = 'serial')
    INTO id_column_is_serial;

    -- Загружаем данные из CSV-файла в указанную таблицу с использованием заданного разделителя
    EXECUTE format('COPY %I FROM %L DELIMITER %L CSV HEADER;', input_table, files_path, delimiter);

    -- Если столбец id является типом serial, обновляем значение счетчика последовательности
    IF id_column_is_serial THEN
        EXECUTE format('SELECT setval(''%I_id_seq'', (SELECT MAX(id) FROM %I))', input_table, input_table);
    END IF;
END
$$;


-- Процедура export_data
-- Принимает на вход имя таблицы, путь к файлу для экспорта данных и разделитель
CREATE OR REPLACE PROCEDURE export_data(table_name VARCHAR(255), file_path VARCHAR(255), delimiter CHAR)
    LANGUAGE PLPGSQL AS
$$
BEGIN
    -- Используя динамически сформированный запрос, копируем данные из указанной
    -- таблицы в CSV-файл с использованием заданного разделителя
    EXECUTE format('COPY %I TO %L DELIMITER %L CSV HEADER;', table_name, file_path, delimiter);
END;
$$;

-- Устанавливаем параметры формата для времени и даты для привычного вида
SET datestyle = 'ISO, DMY';

-- Процедура import_db
-- Принимает путь к папке, в которой находятся CSV-файлы для импорта
CREATE OR REPLACE PROCEDURE import_db(csv_folder text)
    LANGUAGE PLPGSQL AS
$$
BEGIN
    -- Вызываем процедуру import_data для каждой таблицы, передавая соответствующий путь к CSV-файлу и разделитель
    CALL import_data('peers', csv_folder || 'peers.csv', ';');
    CALL import_data('tasks', csv_folder || 'tasks.csv', ';');
    CALL import_data('checks', csv_folder || 'checks.csv', ';');
    CALL import_data('p2p', csv_folder || 'P2P.csv', ';');
    CALL import_data('verter', csv_folder || 'verter.csv', ';');
    CALL import_data('transferredpoints', csv_folder || 'transferred_points.csv', ';');
    CALL import_data('friends', csv_folder || 'friends.csv', ';');
    CALL import_data('recommendations', csv_folder || 'recommendations.csv', ';');
    CALL import_data('xp', csv_folder || 'xp.csv', ';');
    CALL import_data('timetracking', csv_folder || 'time_tracking.csv', ';');
END
$$;

-- На маке закинул файлы в /tmp, так как выдача прав на файлы, прав для
-- пользователя postgres не устроняла проблему при попытке копирования
-- данных из файлов Permission denied
CALL import_db('/tmp/');


-- Второй вариант процедуры для импорта данных из CVS
CREATE OR REPLACE PROCEDURE CopyFromCSV(Delim_etr CHAR(1), file_name VARCHAR, table_name VARCHAR)
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE 'COPY ' || table_name || ' FROM ' || '''' || file_name || ''' DELIMITER ''' || Delim_etr || '''' ||
            'CSV HEADER';
END;
$$;

-- Загрузим данные, предварительно поместив все файлы cvs в /tmp
CALL CopyFromCSV(';', '/tmp/peers.csv', 'Peers');
CALL CopyFromCSV(';', '/tmp/tasks.csv', 'Tasks');
CALL CopyFromCSV(';', '/tmp/checks.csv', 'Checks');
CALL CopyFromCSV(';', '/tmp/P2P.csv', 'P2P');
CALL CopyFromCSV(';', '/tmp/verter.csv', 'Verter');
CALL CopyFromCSV(';', '/tmp/transferred_points.csv', 'TransferredPoints');
CALL CopyFromCSV(';', '/tmp/friends.csv', 'Friends');
CALL CopyFromCSV(';', '/tmp/recommendations.csv', 'Recommendations');
CALL CopyFromCSV(';', '/tmp/xp.csv', 'XP');
CALL CopyFromCSV(';', '/tmp/time_tracking.csv', 'TimeTracking');


-- sudo mkdir /var/tmp/postgres_exports
-- sudo chown postgres:staff /var/tmp/postgres_exports
-- sudo chmod 777 /var/tmp/postgres_exports
-- touch export.csv
-- sudo chown postgres export.csv
-- sudo chmod 777 export.csv
-- Создаем дирекорию и файл устанавливаем ему права и владельца postgres чтоб не было ошибки Permission denied
CALL export_data('peers', '/var/tmp/postgres_exports/export.csv', ';');

-- Добавляем данные для 2 задания
INSERT INTO Peers
VALUES ('celestac', '1999-03-04');
INSERT INTO Peers
VALUES ('ivettepe', '2000-01-05');

-- Добавляем данные для 3 задания

-- 1) Эта функция предназначена для вставки тестовых данных в таблицы Peers и TransferredPoints.
-- Функция вставит псевдонимы пиров и даты их рождения в таблицу Peers, а также тестовые данные
-- о передаче поинтов в таблицу TransferredPoints.
CREATE OR REPLACE FUNCTION insert_data_for_ex_1()
    RETURNS VOID AS
$$

BEGIN
    -- Вставляем данные о пирах в таблицу Peers
    INSERT INTO peers VALUES ('first', '1996-05-01');
    INSERT INTO peers VALUES ('second', '1997-04-02');
    INSERT INTO peers VALUES ('third', '1998-03-03');
    INSERT INTO peers VALUES ('fourth', '1999-03-04');
    INSERT INTO peers VALUES ('fifth', '2000-01-05');

    -- Вставляем данные о передаче поинтов в таблицу TransferredPoints
    INSERT INTO transferredpoints
    VALUES ((SELECT COALESCE(max(id), 0) + 1 FROM transferredpoints), 'first', 'second', 1);
    INSERT INTO transferredpoints
    VALUES ((SELECT COALESCE(max(id), 0) + 1 FROM transferredpoints), 'first', 'third', 13);
    INSERT INTO transferredpoints
    VALUES ((SELECT COALESCE(max(id), 0) + 1 FROM transferredpoints), 'second', 'first', 2);
    INSERT INTO transferredpoints
    VALUES ((SELECT COALESCE(max(id), 0) + 1 FROM transferredpoints), 'second', 'third', 3);
    INSERT INTO transferredpoints
    VALUES ((SELECT COALESCE(max(id), 0) + 1 FROM transferredpoints), 'third', 'first', 3);
    INSERT INTO transferredpoints
    VALUES ((SELECT COALESCE(max(id), 0) + 1 FROM transferredpoints), 'third', 'second', 2);
    INSERT INTO transferredpoints
    VALUES ((SELECT COALESCE(max(id), 0) + 1 FROM transferredpoints), 'fourth', 'second', 2);
    INSERT INTO transferredpoints
    VALUES ((SELECT COALESCE(max(id), 0) + 1 FROM transferredpoints), 'fifth', 'second', 1);
    INSERT INTO transferredpoints
    VALUES ((SELECT COALESCE(max(id), 0) + 1 FROM transferredpoints), 'fifth', 'third', 1);
    INSERT INTO transferredpoints
    VALUES ((SELECT COALESCE(max(id), 0) + 1 FROM transferredpoints), 'first', 'fifth', 1);

END;
$$ LANGUAGE plpgsql;

-- Вызываем функцию для вставки тестовых данных
SELECT *
FROM insert_data_for_ex_1();

-- 2) Вставляем тестовые данные в таблицы
-- checks и xp для тестирования запроса get_success_task_peer_data

-- Вызываем хранимую процедуру add_p2p_check для добавления записей в таблицу P2P
CALL add_p2p_check('first', 'second', 'CPP1', '0', '18:30:00');
CALL add_p2p_check('first', 'second', 'CPP1', '1', '18:30:00');
CALL add_p2p_check('first', 'second', 'CPP1', '0', '18:30:00');
CALL add_p2p_check('first', 'second', 'CPP1', '1', '18:30:00');
CALL add_p2p_check('second', 'third', 'CPP2', '0', '18:30:00');
CALL add_p2p_check('second', 'third', 'CPP2', '1', '18:30:00');
CALL add_p2p_check('second', 'third', 'CPP3', '0', '18:30:00');
CALL add_p2p_check('second', 'third', 'CPP3', '1', '18:30:00');
CALL add_p2p_check('third', 'first', 'CPP4', '0', '18:30:00');
CALL add_p2p_check('third', 'first', 'CPP4', '1', '18:30:00');
CALL add_p2p_check('third', 'first', 'CPP5', '0', '18:30:00');
CALL add_p2p_check('third', 'first', 'CPP5', '1', '18:30:00');

-- Добавляем новую запись в таблицу XP с указанным идентификатором проверки и количеством XP
INSERT INTO xp
VALUES ((SELECT COALESCE(max(id) + 1, 1) FROM xp), (SELECT max(id) FROM Checks WHERE peer = 'first' AND task = 'CPP1'),
        100);
INSERT INTO xp
VALUES ((SELECT COALESCE(max(id) + 1, 1) FROM xp), (SELECT max(id) FROM Checks WHERE peer = 'first' AND task = 'CPP1'),
        123);
INSERT INTO xp
VALUES ((SELECT COALESCE(max(id) + 1, 1) FROM xp), (SELECT max(id) FROM Checks WHERE peer = 'second' AND task = 'CPP2'),
        100);
INSERT INTO xp
VALUES ((SELECT COALESCE(max(id) + 1, 1) FROM xp), (SELECT max(id) FROM Checks WHERE peer = 'second' AND task = 'CPP3'),
        100);
INSERT INTO xp
VALUES ((SELECT COALESCE(max(id) + 1, 1) FROM xp), (SELECT max(id) FROM Checks WHERE peer = 'third' AND task = 'CPP4'),
        100);
INSERT INTO xp
VALUES ((SELECT COALESCE(max(id) + 1, 1) FROM xp), (SELECT max(id) FROM Checks WHERE peer = 'third' AND task = 'CPP5'),
        100);


-- 3) Процедура insert_data_exercise_3 выполняет вставку тестовых данных
-- в таблицу timetracking для тестирования запросов.
CREATE OR REPLACE FUNCTION insert_data_exercise_3() RETURNS VOID AS
$$
BEGIN
    INSERT INTO timetracking
    VALUES (COALESCE((SELECT max(id) + 1 FROM timetracking), 1), 'first', '2022-12-05', '06:00:00', '1');
    INSERT INTO timetracking
    VALUES (COALESCE((SELECT max(id) + 1 FROM timetracking), 1), 'first', '2022-12-05', '07:00:00', '2');
    INSERT INTO timetracking
    VALUES (COALESCE((SELECT max(id) + 1 FROM timetracking), 1), 'first', '2022-12-05', '08:00:00', '1');
    INSERT INTO timetracking
    VALUES (COALESCE((SELECT max(id) + 1 FROM timetracking), 1), 'first', '2022-12-05', '09:00:00', '2');
    INSERT INTO timetracking
    VALUES (COALESCE((SELECT max(id) + 1 FROM timetracking), 1), 'second', '2022-12-05', '06:00:00', '1');
    INSERT INTO timetracking
    VALUES (COALESCE((SELECT max(id) + 1 FROM timetracking), 1), 'second', '2022-12-05', '17:00:00', '2');
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM insert_data_exercise_3();

-- 6) Процедура insert_data_exercise_6 выполняет вставку тестовых данных
-- в таблицу checks для тестирования запросов.
CREATE OR REPLACE FUNCTION insert_data_exercise_6() RETURNS VOID AS
$$
INSERT INTO checks
VALUES (COALESCE((SELECT max(id) + 1 FROM checks), 0), 'first', 'A1', '2024-01-23');
INSERT INTO checks
VALUES (COALESCE((SELECT max(id) + 1 FROM checks), 0), 'fourth', 'A1', '2024-01-23');
INSERT INTO checks
VALUES (COALESCE((SELECT max(id) + 1 FROM checks), 0), 'fifth', 'A1', '2024-01-23');
INSERT INTO checks
VALUES (COALESCE((SELECT max(id) + 1 FROM checks), 0), 'third', 'A2', '2024-01-23');
INSERT INTO checks
VALUES (COALESCE((SELECT max(id) + 1 FROM checks), 0), 'fourth', 'C3', '2024-01-24');
INSERT INTO checks
VALUES (COALESCE((SELECT max(id) + 1 FROM checks), 0), 'second', 'A2', '2024-01-24');
INSERT INTO checks
VALUES (COALESCE((SELECT max(id) + 1 FROM checks), 0), 'third', 'A2', '2024-01-24');
INSERT INTO checks
VALUES (COALESCE((SELECT max(id) + 1 FROM checks), 0), 'first', 'A1', '2024-01-24');
INSERT INTO checks
VALUES (COALESCE((SELECT max(id) + 1 FROM checks), 0), 'first', 'A3', '2024-01-25');
$$ LANGUAGE sql;

SELECT insert_data_exercise_6();

-- 7) Процедура insert_data_exercise_7 выполняет вставку тестовых данных
-- в таблицу checks для тестирования запросов.
CREATE OR REPLACE FUNCTION insert_data_exercise_7() RETURNS void AS
$$
BEGIN
    CALL add_p2p_check('first', 'second', 'SQL1', '0', '18:30:00');
    CALL add_p2p_check('first', 'second', 'SQL1', '1', '18:30:00');
    CALL add_p2p_check('first', 'second', 'SQL2', '0', '18:30:00');
    CALL add_p2p_check('first', 'second', 'SQL2', '1', '18:30:00');
    CALL add_p2p_check('first', 'second', 'SQL3', '0', '18:30:00');
    CALL add_p2p_check('first', 'second', 'SQL3', '1', '18:30:00');
    CALL add_p2p_check('second', 'third', 'SQL1', '0', '18:30:00');
    CALL add_p2p_check('second', 'third', 'SQL1', '1', '18:30:00');
    CALL add_p2p_check('second', 'third', 'SQL2', '0', '18:30:00');
    CALL add_p2p_check('second', 'third', 'SQL2', '1', '18:30:00');
    CALL add_p2p_check('third', 'first', 'SQL1', '0', '18:30:00');
    CALL add_p2p_check('third', 'first', 'SQL1', '1', '18:30:00');
    CALL add_p2p_check('third', 'first', 'SQL2', '0', '18:30:00');
    CALL add_p2p_check('third', 'first', 'SQL2', '1', '18:30:00');
    CALL add_p2p_check('third', 'first', 'SQL3', '0', '18:30:00');
    CALL add_p2p_check('third', 'first', 'SQL3', '1', '18:30:00');
    CALL add_p2p_check('third', 'first', 'SQL3', '0', '18:30:00');
    CALL add_p2p_check('third', 'first', 'SQL3', '1', '18:30:00');
    CALL add_p2p_check('fourth', 'first', 'SQL1', '0', '18:30:00');
    CALL add_p2p_check('fourth', 'first', 'SQL1', '1', '18:30:00');
    CALL add_p2p_check('fourth', 'first', 'SQL2', '0', '18:30:00');
    CALL add_p2p_check('fourth', 'first', 'SQL2', '1', '18:30:00');
    CALL add_p2p_check('fourth', 'first', 'SQL3', '0', '18:30:00');
    CALL add_p2p_check('fourth', 'first', 'SQL3', '1', '18:30:00');
END
$$ LANGUAGE plpgsql;

SELECT insert_data_exercise_7();

-- 8) Процедура insert_data_exercise_8 выполняет вставку тестовых данных
-- в таблицы peers, friends и recommendations для тестирования запросов.
CREATE OR REPLACE FUNCTION insert_data_exercise_8() RETURNS void AS
$$
INSERT INTO peers
VALUES ('sixth', '2000-01-05');
INSERT INTO peers
VALUES ('seventh', '1999-02-15');
INSERT INTO friends
VALUES (COALESCE((SELECT max(id) + 1 FROM friends), 1), 'first', 'second');
INSERT INTO friends
VALUES ((SELECT max(id) + 1 FROM friends), 'first', 'third');
INSERT INTO friends
VALUES ((SELECT max(id) + 1 FROM friends), 'first', 'fourth');
INSERT INTO friends
VALUES ((SELECT max(id) + 1 FROM friends), 'first', 'fifth');
INSERT INTO friends
VALUES ((SELECT max(id) + 1 FROM friends), 'first', 'seventh');
INSERT INTO friends
VALUES ((SELECT max(id) + 1 FROM friends), 'first', 'sixth');
INSERT INTO friends
VALUES ((SELECT max(id) + 1 FROM friends), 'second', 'third');
INSERT INTO friends
VALUES ((SELECT max(id) + 1 FROM friends), 'second', 'fourth');
INSERT INTO friends
VALUES ((SELECT max(id) + 1 FROM friends), 'second', 'sixth');
INSERT INTO friends
VALUES ((SELECT max(id) + 1 FROM friends), 'second', 'fifth');
INSERT INTO recommendations
VALUES (COALESCE((SELECT max(id) + 1 FROM recommendations), 1), 'second', 'third');
INSERT INTO recommendations
VALUES (COALESCE((SELECT max(id) + 1 FROM recommendations), 1), 'fifth', 'third');
INSERT INTO recommendations
VALUES (COALESCE((SELECT max(id) + 1 FROM recommendations), 1), 'seventh', 'fifth');
INSERT INTO recommendations
VALUES (COALESCE((SELECT max(id) + 1 FROM recommendations), 1), 'first', 'fifth');
INSERT INTO recommendations
VALUES (COALESCE((SELECT max(id) + 1 FROM recommendations), 1), 'third', 'fifth');
INSERT INTO recommendations
VALUES (COALESCE((SELECT max(id) + 1 FROM recommendations), 1), 'fourth', 'seventh');
$$ LANGUAGE sql;

SELECT insert_data_exercise_8();

-- 9) Процедура insert_data_exercise_9 выполняет вставку тестовых данных
-- в таблицу checks для тестирования запросов.
CREATE OR REPLACE FUNCTION insert_data_exercise_9() RETURNS void AS
$$
INSERT INTO Tasks
VALUES ('AA1', 'C1', 100),
       ('BB1', 'AA1', 100);
    CALL add_p2p_check('first', 'second', 'AA1', '0', '18:30:00');
    CALL add_p2p_check('first', 'second', 'AA1', '1', '18:30:00');
    CALL add_p2p_check('first', 'second', 'BB1', '0', '18:30:00');
    CALL add_p2p_check('first', 'second', 'BB1', '1', '18:30:00');
    CALL add_p2p_check('fifth', 'second', 'BB1', '0', '18:30:00');
    CALL add_p2p_check('fifth', 'second', 'BB1', '1', '18:30:00');
    CALL add_p2p_check('fifth', 'second', 'AA1', '0', '18:30:00');
    CALL add_p2p_check('fifth', 'second', 'AA1', '1', '18:30:00');
    CALL add_p2p_check('second', 'third', 'AA1', '0', '18:30:00');
    CALL add_p2p_check('second', 'third', 'AA1', '1', '18:30:00');
    CALL add_p2p_check('third', 'sixth', 'BB1', '0', '18:30:00');
    CALL add_p2p_check('third', 'sixth', 'BB1', '1', '18:30:00');
    $$
    LANGUAGE sql;

SELECT insert_data_exercise_9();


-- 11) Процедура insert_data_exercise_11 выполняет вставку тестовых данных
-- в таблицу checks для тестирования запросов.
CREATE OR REPLACE FUNCTION insert_data_exercise_11() RETURNS void AS
$$
BEGIN
    INSERT INTO Tasks
    VALUES ('II1', 'C1', 100),
           ('JJ1', 'C1', 100),
           ('KK1', 'C1', 100)
    ON CONFLICT (Title) DO NOTHING;
    CALL add_p2p_check('first', 'second', 'II1', '0', '18:30:00');
    CALL add_p2p_check('first', 'second', 'II1', '1', '18:30:00');
    CALL add_p2p_check('first', 'second', 'JJ1', '0', '18:30:00');
    CALL add_p2p_check('first', 'second', 'JJ1', '1', '18:30:00');
    CALL add_p2p_check('first', 'second', 'KK1', '0', '18:30:00');
    CALL add_p2p_check('first', 'second', 'KK1', '1', '18:30:00');
    CALL add_p2p_check('second', 'third', 'II1', '0', '18:30:00');
    CALL add_p2p_check('second', 'third', 'II1', '1', '18:30:00');
    CALL add_p2p_check('second', 'third', 'JJ1', '0', '18:30:00');
    CALL add_p2p_check('second', 'third', 'JJ1', '1', '18:30:00');
    CALL add_p2p_check('third', 'sixth', 'II1', '0', '18:30:00');
    CALL add_p2p_check('third', 'sixth', 'II1', '1', '18:30:00');
    CALL add_p2p_check('third', 'sixth', 'JJ1', '0', '18:30:00');
    CALL add_p2p_check('third', 'sixth', 'JJ1', '1', '18:30:00');
END
$$ LANGUAGE PLPGSQL;

SELECT insert_data_exercise_11();