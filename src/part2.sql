-- Создание процедуры add_p2p_check для добавения P2P проверки
CREATE OR REPLACE PROCEDURE add_p2p_check(
    checked_peer_name VARCHAR(40), -- Имя проверяемого участника
    checking_peer_name VARCHAR(40), -- Имя проверяющего участника
    task_title VARCHAR(40), -- Название задачи
    p2p_status_in REVIEW_STATUS, -- Статус проверки (ENUM: '0 - Start', '1 - Success', '2 - Failure')
    check_time TIME -- Время проведения проверки
)
    LANGUAGE PLPGSQL AS
$$
DECLARE
    existing_p2p_check_count INTEGER; -- Переменная для хранения количества незавершенных проверок
    existing_check_id        INTEGER; -- Переменная для хранения ID созданной или выбранной проверки
BEGIN
    -- Подсчет количества незавершенных проверок в таблице P2P
    SELECT COUNT(*)
    INTO existing_p2p_check_count
    FROM P2P
    WHERE "check" IN (SELECT id
                      FROM Checks
                      WHERE peer = checked_peer_name
                        AND task = task_title
                        AND "date" = CURRENT_DATE);

    -- Сброс значения последовательности (для гарантии уникальности ID)
    EXECUTE 'SELECT setval(''checks_id_seq'', (SELECT MAX(id) FROM Checks))';
    EXECUTE 'SELECT setval(''p2p_id_seq'', (SELECT MAX(id) FROM p2p))';

    -- Проверка на четность количества незавершенных проверок
    IF (existing_p2p_check_count % 2 = 0 OR existing_p2p_check_count IS NULL) THEN
        -- Четное количество: проверка, что входящий статус '0'
        IF p2p_status_in = '0' THEN
            -- Создание новой проверки в таблице Checks и соответствующей записи в таблице P2P
            INSERT INTO Checks(peer, task, "date")
            VALUES (checked_peer_name, task_title, CURRENT_DATE)
            RETURNING id INTO existing_check_id;

            -- Вставка записи в таблицу P2P
            INSERT INTO P2P("check", checking_peer, "state", time)
            VALUES (existing_check_id, checking_peer_name, p2p_status_in, check_time);
        ELSE
            RAISE EXCEPTION 'Ошибка: Все существующие проверки уже завершены.';
        END IF;
    ELSE
        -- Нечетное количество: проверка наличия незавершенной проверки
        IF p2p_status_in = '0' THEN
            RAISE EXCEPTION 'Нельзя начать новую проверку. Существует незавершенная проверка.';
        ELSE
            -- Создание завершающей записи для незавершенной проверки в таблице P2P
            INSERT INTO P2P("check", checking_peer, "state", time)
            VALUES ((SELECT MAX("check")
                     FROM P2P
                     WHERE "check" IN (SELECT id
                                       FROM Checks
                                       WHERE peer = checked_peer_name
                                         AND task = task_title
                                         AND "date" = CURRENT_DATE)
                       AND "state" = '0'),
                    checking_peer_name,
                    p2p_status_in,
                    check_time);
        END IF;
    END IF;
END;
$$;

-- Процедура add_verter_check предназначена для добавления проверки от пользователей вертера
CREATE OR REPLACE PROCEDURE add_verter_check(
    checked_peer_name VARCHAR(40), -- имя проверяемого участника
    task_title VARCHAR(40), -- название задания
    verter_status REVIEW_STATUS, -- статус проверки Вертером
    check_time TIME -- время проведения проверки
)
    LANGUAGE PLPGSQL AS
$$
DECLARE
    last_successful_check_id INTEGER; -- Переменная для хранения ID последней успешной P2P проверки
    verter_check_count       INTEGER; -- Переменная для хранения количества существующих проверок Вертера
BEGIN
    -- Поиск ID последней успешной P2P проверки для указанного участника и задания
    SELECT MAX("check")
    INTO last_successful_check_id
    FROM P2P
    WHERE "check" IN (SELECT id
                      FROM Checks
                      WHERE peer = checked_peer_name
                        AND task = task_title)
      AND state = '1';

    -- Проверка наличия подходящей успешной P2P проверки
    IF last_successful_check_id IS NULL THEN
        RAISE EXCEPTION 'Не найдено подходящих проверок.';
    ELSE
        -- Сброс значения последовательности (для гарантии уникальности ID)
        EXECUTE 'SELECT setval(''verter_id_seq'', (SELECT MAX(id) FROM verter))';

        -- Подсчет количества существующих проверок Вертера для найденной успешной P2P проверки
        SELECT COUNT(*)
        INTO verter_check_count
        FROM Verter
        WHERE "check" = last_successful_check_id;

        -- Логика добавления проверки Вертера в таблицу Verter
        IF verter_check_count = 0 THEN
            IF verter_status = '0' THEN
                -- Добавление новой проверки Вертера
                INSERT INTO Verter("check", state, time)
                VALUES (last_successful_check_id, verter_status, check_time);
            ELSE
                RAISE EXCEPTION 'Не существует начатой проверки для указанных параметров.';
            END IF;
        ELSIF verter_check_count = 1 THEN
            IF verter_status != '0' THEN
                -- Добавление завершающей записи для существующей проверки Вертера
                INSERT INTO Verter("check", state, time)
                VALUES (last_successful_check_id, verter_status, check_time);
            ELSE
                RAISE EXCEPTION 'Существует незавершенная проверка Вертером';
            END IF;
        ELSE
            RAISE EXCEPTION 'Вертер уже завершил последнюю доступную проверку';
        END IF;
    END IF;
END;
$$;

-- Функция update_transferred_points проверяет, что добавленная запись в
-- P2P имеет статус "начало" (state = '0'), и, если это так, то обновляет
-- соответствующую запись в таблице TransferredPoints, увеличивая количество
-- очков на 1 для данной пары checking_peer и checked_peer.
CREATE OR REPLACE FUNCTION update_transferred_points()
    RETURNS TRIGGER AS
$$
BEGIN
    -- Проверка, что статус "начало"
    IF NEW.state = '0' THEN
        -- Проверяем, существует ли запись в таблице TransferredPoints для данной пары checking_peer и checked_peer
        PERFORM 1
        FROM TransferredPoints
        WHERE checking_peer = NEW.checking_peer
          AND checked_peer = (SELECT peer FROM Checks WHERE id = NEW.check);

        -- Если записи не существует, то создаем новую запись в таблице TransferredPoints
        IF NOT FOUND THEN
            INSERT INTO TransferredPoints (id, checking_peer, checked_peer, points_amount)
            VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM TransferredPoints), NEW.checking_peer,
                    (SELECT peer FROM Checks WHERE id = NEW.check), 1);
        ELSE
            -- Если запись существует, то обновляем количество очков
            UPDATE TransferredPoints
            SET points_amount = points_amount + 1
            WHERE checking_peer = NEW.checking_peer
              AND checked_peer = (SELECT peer FROM Checks WHERE id = NEW.check);
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггер p2p_status_start_trigger создается для таблицы P2P и привязывается
-- к событию AFTER INSERT, что означает, что триггер будет срабатывать после
-- добавления новой записи в таблицу P2P. Каждый раз, когда происходит вставка
-- записи в таблицу P2P, триггер вызывает функцию update_transferred_points.
CREATE OR REPLACE TRIGGER p2p_status_start_trigger
    AFTER INSERT
    ON P2P
    FOR EACH ROW
EXECUTE FUNCTION update_transferred_points();

-- Функция check_xp_insert возвращает тип TRIGGER. Предназначена для проверки корректности
-- добавления записей в таблицу XP перед их фактическим добавлением. Она содержит несколько
-- блоков кода, включая определение локальных переменных, основную логику проверки и
-- инструкции для исключения в случае некорректной записи.
CREATE OR REPLACE FUNCTION check_xp_insert()
    RETURNS TRIGGER AS
$$
DECLARE
    p2p_status    REVIEW_STATUS;
    verter_status REVIEW_STATUS;
    max_xp_amount INTEGER;
BEGIN
    -- Получаем статус P2P этапа для указанной проверки
    SELECT max(state)
    INTO p2p_status
    FROM p2p
    WHERE "check" = NEW."check";

    -- Получаем статус Verter этапа для указанной проверки
    SELECT max(state)
    INTO verter_status
    FROM verter
    WHERE "check" = NEW."check";

    -- Получаем максимальное доступное количество XP для проверяемой задачи
    SELECT max_xp
    INTO max_xp_amount
    FROM Tasks
    WHERE title = (SELECT task FROM Checks WHERE id = NEW."check")
    LIMIT 1;

    -- Проверяем условия корректности записи
    IF NEW.xp_amount <= max_xp_amount
        AND EXISTS (SELECT 1 FROM Checks WHERE id = NEW.check) -- Проверка наличия успешной проверки
        AND NOT EXISTS (SELECT 1 FROM XP WHERE "check" = NEW."check") -- Проверка на отсутствие дублирования
        AND p2p_status = '1'::REVIEW_STATUS -- Успешный P2P этап
        AND
       (verter_status = '1'::REVIEW_STATUS OR verter_status IS NULL) THEN -- Успешный Verter этап или его отсутствие
        RETURN NEW; -- Запись корректна, добавляем её
    ELSE
        RAISE EXCEPTION 'Некорректная запись XP. Проверьте количество XP, успешность P2P и Verter этапов или возможное дублирование.';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Триггер xp_insert_trigger создается для таблицы XP и привязывается
-- к событию BEFORE INSERT, что означает, что триггер будет срабатывать
-- перед добавлением новой записи в таблицу XP. Каждый раз, когда
-- происходит попытка вставки записи в таблицу XP, вызывается check_xp_insert.
CREATE OR REPLACE TRIGGER xp_insert_trigger
    BEFORE INSERT
    ON XP
    FOR EACH ROW
EXECUTE FUNCTION check_xp_insert();

-- Устанавливаем текущее значение последовательности xp_id_seq в соответствии
-- с максимальным значением идентификатора из таблицы XP
SELECT setval('xp_id_seq', (SELECT MAX(id) FROM xp));

-- Вызываем хранимую процедуру add_p2p_check для добавления записей в таблицу P2P
CALL add_p2p_check('ivettepe', 'celestac', 'C2', '0', '18:30:00');
CALL add_p2p_check('ivettepe', 'celestac', 'C2', '1', '18:30:00');
CALL add_p2p_check('ivettepe', 'celestac', 'C2', '2', '18:30:00');

-- Вызываем хранимую процедуру add_verter_check для добавления записей в таблицу Verter
CALL add_verter_check('ivettepe', 'C2', '0', '18:30:00');
CALL add_verter_check('ivettepe', 'C2', '1', '18:30:00');
CALL add_verter_check('ivettepe', 'C2', '2', '18:30:00');

-- Добавляем новую запись в таблицу XP с указанным идентификатором проверки и количеством XP
INSERT INTO XP("check", xp_amount)
VALUES ((SELECT max(id) FROM Checks WHERE peer = 'ivettepe' AND task = 'C2'), 97);