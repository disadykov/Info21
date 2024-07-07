-- 1) Эта функция возвращает таблицу с суммарным количеством переданных пир-поинтов
-- между парами пиров. Для каждой пары пиров, она вычисляет разницу в количестве
-- переданных поинтов, и, если пир - 2 получил больше поинтов, количество будет
-- отрицательным.
CREATE OR REPLACE FUNCTION get_transfer_summary()
    RETURNS TABLE
            (
                Peer1        VARCHAR,
                Peer2        VARCHAR,
                PointsAmount INTEGER
            )
AS
$$
SELECT checking_peer AS Peer1,
       checked_peer  AS Peer2,
       points_amount AS PointsAmount
FROM transferredpoints
WHERE (checking_peer, checked_peer) NOT IN (SELECT checked_peer, checking_peer FROM transferredpoints)
UNION ALL
SELECT t1.checking_peer                                 AS Peer1,
       t1.checked_peer                                  AS Peer2,
       t1.points_amount - COALESCE(t2.points_amount, 0) AS PointsAmount
FROM transferredpoints t1
         LEFT JOIN transferredpoints t2 ON t1.checking_peer = t2.checked_peer AND t1.checked_peer = t2.checking_peer
WHERE t1.points_amount - COALESCE(t2.points_amount, 0) < 0;
$$ LANGUAGE sql;

-- Вызываем функцию и выбираем данные для определенных пользователей
SELECT *
FROM get_transfer_summary()
WHERE Peer1 IN ('first', 'second', 'third', 'fourth', 'fifth', NULL)
  AND Peer2 IN ('first', 'second', 'third', 'fourth', 'fifth', NULL);

-- Вызываем функцию и выбираем все данные
SELECT *
FROM get_transfer_summary();


-- 2) Функция exercise_2 возвращает таблицу с данными о пользователе,
-- выполненной задаче и количестве полученных XP. В таблицу включать
-- только задания, успешно прошедшие проверку (определять по таблице Checks).
-- Одна задача может быть успешно выполнена несколько раз. В таком случае в
-- таблицу включать все успешные проверки.
CREATE OR REPLACE FUNCTION get_success_task_peer_data()
    RETURNS TABLE
            (
                peer VARCHAR,
                task VARCHAR,
                xp   INTEGER
            )
AS
$$
SELECT peer, task, xp_amount AS xp -- Выбираем данные о пользователе, задаче и XP
FROM checks AS c
         JOIN xp ON xp."check" = c.id; -- Объединяем таблицы checks и xp по полю "check"
$$ LANGUAGE sql;

-- Вызываем функцию и выбираем данные для определенных пользователей
SELECT *
FROM get_success_task_peer_data()
WHERE peer IN ('first', 'second', 'third', 'fourth', 'fifth', NULL);

-- Вызываем функцию и выбираем все данные
SELECT *
FROM get_success_task_peer_data();


-- 3) Функция get_day_no_left_campus определяет пиров, которые не покидали кампус в указанный день
-- Параметры функции: day - день, для которого нужно определить пиров
CREATE OR REPLACE FUNCTION get_day_no_left_campus(day date)
    RETURNS TABLE
            (
                peer VARCHAR
            )
AS
$$
SELECT peer
FROM timetracking
WHERE state = 2
  AND date = day
GROUP BY peer
HAVING count(state) = 1;
$$ LANGUAGE sql;

SELECT *
FROM get_day_no_left_campus('2022-12-05');


-- 4) Функция change_in_count_prp подсчитывает изменение в количестве пир поинтов
-- каждого пира по таблице TransferredPoints. Результат выводится отсортированным
-- по изменению числа поинтов.
CREATE OR REPLACE FUNCTION change_in_count_prp()
    RETURNS TABLE
            (
                peer         VARCHAR,
                PointsChange INTEGER
            )
AS
$$
WITH cte_checked AS (SELECT checked_peer, sum(points_amount) AS gain FROM transferredpoints GROUP BY checked_peer),
     cte_checking AS (SELECT checking_peer, sum(points_amount) AS spend FROM transferredpoints GROUP BY checking_peer)
SELECT checking_peer AS Peer, COALESCE(spend, 0) - COALESCE(gain, 0) AS PointsChange
FROM cte_checked AS t1
         FULL JOIN cte_checking AS t2 ON checking_peer = checked_peer;
$$ LANGUAGE sql;

-- Вызываем функцию и выбираем данные для определенных пользователей
SELECT *
FROM change_in_count_prp()
WHERE peer IN ('first', 'second', 'third', 'fourth', 'fifth', NULL)
ORDER BY peer;

-- Вызываем функцию и выбираем все данные
SELECT *
FROM change_in_count_prp()
ORDER BY peer;


-- 5) Функция change_in_count_prp_from_part3_1 подсчитывает изменение в количестве пир поинтов
-- каждого пира по таблице по таблице, возвращаемой первой функцией из Part 3. Результат выводится отсортированным
-- по изменению числа поинтов.
CREATE OR REPLACE FUNCTION change_in_count_prp_from_part3_1()
    RETURNS TABLE
            (
                peer         VARCHAR,
                PointsChange INTEGER
            )
AS
$$
WITH cte_peer1 AS (SELECT peer1, sum(pointsamount) AS gain FROM get_transfer_summary() GROUP BY peer1),
     cte_peer2 AS (SELECT peer2, sum(pointsamount) AS spend FROM get_transfer_summary() GROUP BY peer2)
SELECT peer1 AS Peer, COALESCE(gain, 0) - COALESCE(spend, 0) AS PointsChange
FROM cte_peer1 AS t1
         FULL JOIN cte_peer2 AS t2 ON peer1 = peer2
WHERE peer1 IS NOT NULL
UNION
SELECT peer2 AS Peer, COALESCE(gain, 0) - COALESCE(spend, 0) AS PointsChange
FROM cte_peer1 AS t1
         FULL JOIN cte_peer2 AS t2 ON peer1 = peer2
WHERE peer2 IS NOT NULL;
$$ LANGUAGE sql;

-- Вызываем функцию и выбираем данные для определенных пользователей
SELECT *
FROM change_in_count_prp_from_part3_1()
WHERE peer IN ('first', 'second', 'third', 'fourth', 'fifth', NULL)
ORDER BY peer;

-- Вызываем функцию и выбираем все данные
SELECT *
FROM change_in_count_prp_from_part3_1()
ORDER BY peer;


-- 6) Функция most_delivered_projects возвращает список задач,
-- которые были выполнены чаще всего для каждого дня.
CREATE OR REPLACE FUNCTION most_delivered_projects()
    RETURNS TABLE
            (
                day  DATE,
                task TEXT
            )
AS
$$
WITH cte_count AS (SELECT date, task, count(id) AS task_count
                   FROM Checks
                   GROUP BY date, task),
     cte_max AS (SELECT task, date, RANK() OVER (PARTITION BY date ORDER BY task_count DESC) AS rnk
                 FROM cte_count)
SELECT date, task
FROM cte_max
WHERE rnk = 1
ORDER BY date;
$$ LANGUAGE sql;

-- Вызываем функцию и выбираем данные для определенных пользователей
SELECT *
FROM most_delivered_projects()
WHERE day IN ('2024-01-23', '2024-01-24', '2024-01-25');

-- Вызываем функцию и выбираем все данные
SELECT *
FROM most_delivered_projects();


-- 7) Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания
-- Параметры процедуры: название блока, например "CPP".
-- Результат вывести отсортированным по дате завершения.
-- Формат вывода: ник пира, дата завершения блока (т.е. последнего выполненного задания из этого блока)
CREATE OR REPLACE PROCEDURE all_peers_completed_task_block(block VARCHAR, c1 REFCURSOR DEFAULT 'cursor') AS
$$
BEGIN
    OPEN c1 FOR
        WITH
            -- Отфильтровываем успешно выполненные задания из нужного блока
            -- Нумеруем каждое задание для каждого пира, упорядочивая их по дате завершения
            cte_success AS (SELECT ROW_NUMBER() OVER (PARTITION BY peer, task ORDER BY date DESC) AS num,
                                   checks.id,
                                   peer,
                                   task,
                                   date
                            FROM checks
                                     LEFT JOIN p2p ON p2p."check" = checks.id
                                     LEFT JOIN verter ON verter."check" = checks.id
                            WHERE task ~ (block || '[0-9]+')
                              AND p2p.state = '1'
                              AND (verter.state IS NULL OR verter.state = '1')),
            -- Считаем количество успешно выполненных заданий для каждого пира
            -- и выбираем только последнее успешно выполненное задание для каждого пира
            cte_count AS (SELECT peer, count(distinct task) AS amount
                          FROM cte_success
                          WHERE num = 1
                          GROUP BY peer),
            -- Выбираем последнюю дату выполненного задания для каждого пира
            cte_last_check AS (SELECT peer, max(date) AS date
                               FROM checks
                                        LEFT JOIN p2p ON p2p."check" = checks.id
                                        LEFT JOIN verter ON verter."check" = checks.id
                               WHERE task ~ (SELECT block || '[0-9]+')
                                 AND p2p.state = '1'
                                 AND (verter.state IS NULL OR verter.state = '1')
                               GROUP BY peer)
        SELECT cte_last_check.peer, date
        FROM cte_count
                 JOIN cte_last_check ON cte_last_check.peer = cte_count.peer
        -- Условие: количество успешно выполненных заданий равно общему количеству заданий в блоке
        WHERE amount = (SELECT count(title) FROM tasks WHERE title ~ (SELECT block || '[0-9]+'))
        ORDER BY date;
END
$$ LANGUAGE plpgsql;

-- Вызываем процедуру и выбираем все данные
BEGIN;
CALL all_peers_completed_task_block('SQL');
FETCH ALL FROM "cursor";
END;


-- 8) Определить, к какому пиру стоит идти на проверку каждому обучающемуся
-- Определять нужно исходя из рекомендаций друзей пира, т.е. нужно найти пира,
-- проверяться у которого рекомендует наибольшее число друзей.
-- Формат вывода: ник пира, ник найденного проверяющего
CREATE OR REPLACE FUNCTION best_peer_to_review()
    RETURNS TABLE
            (
                Peer            VARCHAR,
                RecommendedPeer VARCHAR
            )
AS
$$
    -- Объединяем пары друзей в одну таблицу для удобства последующего использования
WITH cte_union AS (SELECT peer1, peer2
                   from friends
                   UNION
                   SELECT peer2, peer1
                   from friends),
     cte_count AS (
         -- Считаем количество рекомендаций для каждой пары пиров
         SELECT peer1, recommended_peer, count(peer) AS count
         FROM cte_union
                  JOIN recommendations ON peer2 = peer AND peer1 != recommended_peer
         GROUP BY peer1, recommended_peer),
     cte_row AS (
         -- Добавляем номер строки с наибольшим количеством рекомендаций для каждого пира
         SELECT peer1,
                recommended_peer,
                count,
                ROW_NUMBER()
                OVER (PARTITION BY peer1 ORDER BY count DESC) AS num
         FROM cte_count)
-- Выводим пары (пир, рекомендованный пир), выбирая только те, у которых номер равен 1 (т.е. с самым большим количеством рекомендаций)
SELECT peer1 AS peer, recommended_peer
FROM cte_row
WHERE num = 1
$$
    LANGUAGE sql;

-- Вызываем функцию и выбираем данные для определенных пользователей
SELECT *
FROM best_peer_to_review()
WHERE Peer IN ('first', 'second', 'third', 'fourth', 'fifth', 'sixth', 'seventh', NULL);

-- Вызываем функцию и выбираем все данные
SELECT *
FROM best_peer_to_review();

-- 9) Определить процент пиров, которые:
-- Приступили только к блоку 1
-- Приступили только к блоку 2
-- Приступили к обоим
-- Не приступили ни к одному
CREATE OR REPLACE FUNCTION peer_percent_block_stat(block1 VARCHAR, block2 VARCHAR)
    RETURNS TABLE
            (
                StartedBlock1      VARCHAR,
                StartedBlock2      VARCHAR,
                StartedBothBlocks  VARCHAR,
                DidntStartAnyBlock VARCHAR
            )
AS
$$
WITH cte_peer_amount AS (SELECT NULLIF(count(nickname), 0) AS count FROM Peers),
     cte_block1 AS (
         -- Выбираем уникальных пиров, выполнивших задания из первого блока
         SELECT DISTINCT peer
         FROM Checks
         WHERE task ~ (block1 || '[0-9]+')),
     cte_block2 AS (
         -- Выбираем уникальных пиров, выполнивших задания из второго блока
         SELECT DISTINCT peer
         FROM Checks
         WHERE task ~ (block2 || '[0-9]+')),
     cte_in_block1 AS (
         -- Считаем количество пиров, которые выполнили только задания из первого блока
         SELECT count(nickname) AS count
         FROM peers
         WHERE nickname IN (SELECT * FROM cte_block1)
           AND nickname NOT IN (SELECT * FROM cte_block2)),
     cte_in_block2 AS (
         -- Считаем количество пиров, которые выполнили только задания из второго блока
         SELECT count(nickname) AS count
         FROM peers
         WHERE nickname IN (SELECT * FROM cte_block2)
           AND nickname NOT IN (SELECT * FROM cte_block1)),
     cte_in_block12 AS (
         -- Считаем количество пиров, выполнивших задания из обоих блоков
         SELECT count(nickname) AS count
         FROM peers
         WHERE nickname IN (SELECT * FROM cte_block2)
           AND nickname IN (SELECT * FROM cte_block1)),
     cte_not_in_block12 AS (
         -- Считаем количество пиров, которые не выполнили ни одного задания из обоих блоков
         SELECT count(nickname) AS count
         FROM peers
         WHERE nickname NOT IN (SELECT * FROM cte_block2)
           AND nickname NOT IN (SELECT * FROM cte_block1))
-- Процентное соотношение для каждой категории пиров
SELECT ROUND((SELECT count FROM cte_in_block1)::numeric / (SELECT count FROM cte_peer_amount)::numeric * 100,
             2)      AS StartedBlock1,
       ROUND((SELECT count FROM cte_in_block2)::numeric / (SELECT count FROM cte_peer_amount)::numeric * 100,
             2)      AS StartedBlock2,
       ROUND((SELECT count FROM cte_in_block12)::numeric / (SELECT count FROM cte_peer_amount)::numeric * 100,
             2)      AS StartedBothBlocks,
       ROUND((SELECT count FROM cte_not_in_block12)::numeric / (SELECT count FROM cte_peer_amount)::numeric *
             100, 2) AS DidntStartAnyBlock;
$$
    LANGUAGE sql;

-- Вызываем функцию и выбираем данные для определенных значений
SELECT *
FROM peer_percent_block_stat('AA', 'BB');


-- 10) Определить процент пиров, которые когда-либо успешно проходили проверку в свой день рождения
-- Также определите процент пиров, которые хоть раз проваливали проверку в свой день рождения.
-- Формат вывода: процент пиров, успешно прошедших проверку в день рождения, процент пиров, проваливших проверку в день рождения
CREATE OR REPLACE FUNCTION peer_percent_task_stat_in_birthday()
    RETURNS TABLE
            (
                SuccessfulChecks   INTEGER,
                UnsuccessfulChecks INTEGER
            )
AS
$$
WITH cte_join AS (
    -- Соединяем таблицы для получения информации о проверках пиров в их день рождения
    SELECT checks.id AS ch_id, p2p.state AS p2p_state, verter.state AS v_state
    FROM Peers
             JOIN Checks ON Peers.nickname = Checks.peer
             LEFT JOIN p2p ON p2p.check = Checks.id
             LEFT JOIN verter ON verter.check = Checks.id
    WHERE EXTRACT(MONTH FROM birthday) = EXTRACT(MONTH FROM date)
      AND EXTRACT(DAY FROM birthday) = EXTRACT(DAY FROM date)),
     cte_bday_count AS (
         -- Считаем количество уникальных проверок в день рождения
         SELECT NULLIF(count(DISTINCT ch_id), 0) FROM cte_join WHERE p2p_state = '1' OR p2p_state = '2'),
     cte_success_count AS (
         -- Считаем количество успешных проверок в день рождения
         SELECT count(DISTINCT ch_id)
         FROM cte_join
         WHERE p2p_state = '1'
           AND (v_state IS NULL OR v_state = '1')),
     cte_failure_count AS (
         -- Считаем количество проваленных проверок в день рождения
         SELECT count(DISTINCT ch_id)
         FROM cte_join
         WHERE p2p_state = '2'
            OR v_state = '2')
-- Вычисляем процент успешных и проваленных проверок в день рождения
SELECT ROUND((SELECT * FROM cte_success_count)::numeric / (SELECT * FROM cte_bday_count)::numeric * 100,
             0) AS SuccessfulChecks,
       ROUND((SELECT * FROM cte_failure_count)::numeric / (SELECT * FROM cte_bday_count)::numeric * 100,
             0) AS UnsuccessfulChecks;
$$
    LANGUAGE sql;

-- Вызываем функцию и выбираем все данные
SELECT *
FROM peer_percent_task_stat_in_birthday();


-- 11) Определить всех пиров, которые сдали заданные задания 1 и 2, но не сдали задание 3
-- Параметры процедуры: названия заданий 1, 2 и 3.
-- Формат вывода: список пиров
CREATE OR REPLACE PROCEDURE peer_stat_made_first_and_second_not_third(task1 VARCHAR, task2 VARCHAR, task3 VARCHAR,
                                                                      c1 REFCURSOR DEFAULT 'cursor') AS
$$
BEGIN
    OPEN c1 FOR
        -- Открываем курсор для возврата результата из хранимой процедуры
        WITH cte_join AS (
            -- Создаем общий подзапрос для объединения таблиц и
            -- получения информации о выполненных заданиях и их состоянии
            SELECT nickname, task, p2p.state AS p2p_state, verter.state AS v_state
            FROM peers
                     LEFT JOIN checks ON checks.peer = peers.nickname
                     LEFT JOIN p2p ON p2p.check = checks.id
                     LEFT JOIN verter ON verter.check = checks.id
            WHERE p2p.state != '0'
               OR verter.state != '0'
            -- Отбираем только те записи, где проверка
            -- была завершена (не в состоянии "0")
        ),
             cte_task1 AS (
                 -- Подзапрос для отбора пиров, которые сдали
                 -- задание 1 с успешным p2p и verter состоянием
                 SELECT nickname
                 FROM cte_join
                 WHERE task = task1
                   AND p2p_state = '1'
                   AND (v_state = '1' OR v_state IS NULL)),
             cte_task2 AS (
                 -- Подзапрос для отбора пиров, которые сдали
                 -- задание 2 с успешным p2p и verter состоянием
                 SELECT nickname
                 FROM cte_join
                 WHERE task = task2
                   AND p2p_state = '1'
                   AND (v_state = '1' OR v_state IS NULL)),
             cte_task3 AS (
                 -- Подзапрос для отбора пиров, которые сдали
                 -- задание 3 с успешным p2p и verter состоянием
                 SELECT nickname
                 FROM cte_join
                 WHERE task = task3
                   AND p2p_state = '1'
                   AND (v_state = '1' OR v_state IS NULL))
        -- Основной запрос для получения списка пиров, которые
        -- сдали задания 1 и 2, но не сдали задание 3
        SELECT DISTINCT nickname
        FROM cte_join
        WHERE nickname IN (SELECT nickname FROM cte_task1)
          AND nickname IN (SELECT nickname FROM cte_task2)
          AND nickname NOT IN (SELECT nickname FROM cte_task3);
END
$$ LANGUAGE plpgsql;

BEGIN;
-- Вызов процедуры с параметрами 'II1', 'JJ1', 'KK1'
CALL peer_stat_made_first_and_second_not_third('II1', 'JJ1', 'KK1');
FETCH ALL FROM "cursor";
-- Извлечение результата из курсора
END;


-- 12) Используя рекурсивное обобщенное табличное выражение, для каждой задачи вывести кол-во предшествующих ей задач
-- То есть сколько задач нужно выполнить, исходя из условий входа, чтобы получить доступ к текущей.
-- Формат вывода: название задачи, количество предшествующих
CREATE OR REPLACE FUNCTION task_prev_count()
    RETURNS TABLE
            (
                Task      VARCHAR,
                PrevCount INTEGER
            )
AS
$$
    -- Создание функции task_predecessors, возвращающей таблицу
    -- с именем задачи и количеством предшествующих задач
WITH RECURSIVE task_recursive(title, parent_task, n) AS (
    -- Базовый случай: выбираем задачи, у которых нет
    -- предшественников (parent_task = 'None' или NULL)
    SELECT title, parent_task, 0
    FROM tasks
    WHERE parent_task = 'None'
       or parent_task IS NULL
    UNION ALL
    -- Рекурсивный шаг: объединяем текущую задачу с ее
    -- предшественниками, увеличивая счетчик предшествующих задач (n + 1)
    SELECT t.title, t.parent_task, n + 1
    FROM tasks t
             JOIN task_recursive tr ON tr.title = t.parent_task)
-- Завершаем функцию, возвращая итоговые результаты
-- из рекурсивного табличного выражения
SELECT title AS Task, n AS PrevCount
FROM task_recursive
$$ LANGUAGE sql;

-- Вызываем функцию task_prev_count для получения результата
SELECT *
FROM task_prev_count();


-- 13) Процедура находит «удачные» для проверок дни. День считается «удачным», если в нем есть хотя бы N идущих подряд успешных проверки
-- Параметры процедуры: количество идущих подряд успешных проверок N. 
-- Временем проверки считать время начала P2P этапа. 
-- Под идущими подряд успешными проверками подразумеваются успешные проверки, между которыми нет неуспешных. 
-- При этом кол-во опыта за каждую из этих проверок должно быть не меньше 80% от максимального. 
-- Формат вывода: список дней.

CREATE OR REPLACE PROCEDURE prc_lucky_day(IN N INT) AS
$$
BEGIN
    -- Удаляем и создаем временную таблицу для результата
    DROP TABLE IF EXISTS part_13_table;
    CREATE TEMPORARY TABLE part_13_table AS

    WITH lucky AS (SELECT *
                   FROM checks c
                            JOIN p2p p ON c.id = p.Check
                            LEFT JOIN verter v ON c.id = v.Check
                            JOIN tasks t ON c.task = t.title
                            JOIN xp x ON c.id = x.Check
                   WHERE (p.state = '1')
                     AND (v.state = '1' OR v.state IS NULL))
    SELECT date
    FROM lucky l
    WHERE l.xp_amount >= l.max_xp * 0.8
    GROUP BY date
    HAVING COUNT(date) >= N;
END;
$$ LANGUAGE plpgsql;

-- Проверка работы процедуры на 3х "удачных" днях
CALL prc_lucky_day(3);
SELECT *
FROM part_13_table;


--14) Функция определеяет пира с наибольшим количеством XP
-- Формат вывода: ник пира, количество XP.

CREATE OR REPLACE FUNCTION max_peer_xp()
    RETURNS TABLE
            (
                peer VARCHAR,
                XP   INTEGER
            )
AS
$$
SELECT peer, sum(xp_amount) AS XP
FROM xp
         JOIN checks c ON xp.Check = c.id
GROUP BY peer
ORDER BY XP DESC
LIMIT 1;
$$ LANGUAGE sql;

-- Вызываем функцию и выводим пира с максимальным хр
SELECT *
FROM max_peer_xp();

--15) Определить пиров, приходивших раньше заданного времени не менее N раз за всё время
-- Параметры процедуры: время, количество раз N. 
-- Формат вывода: список пиров.

CREATE OR REPLACE PROCEDURE check_time(time_ TIME, N INT)
    LANGUAGE plpgsql
AS
$$
BEGIN
    -- Удаляем и создаем временную таблицу для результата
    DROP TABLE IF EXISTS part_15_table;
    CREATE TEMPORARY TABLE part_15_table AS

    with one as (SELECT Peer, Count(*) AS sum_n
                 FROM TimeTracking
                 WHERE time_ > TimeTracking.Time
                   AND TimeTracking.state = '1'
                 GROUP BY Peer)
    SELECT Peer
    FROM one
    WHERE sum_n >= N;
END;
$$;

-- Проверка работы процедуры
CALL check_time('10:00:00', 4);
SELECT *
FROM part_15_table;

-- 16) Определить пиров, выходивших за последние N дней из кампуса больше M раз
-- Параметры процедуры: количество дней N, количество раз M. 
-- Формат вывода: список пиров.

CREATE OR REPLACE PROCEDURE prc_count_out_of_campus(N INT, M INT)
    LANGUAGE plpgsql
AS
$$
BEGIN
    -- Удаляем и создаем временную таблицу для результата 
    DROP TABLE IF EXISTS part_16_table;
    CREATE TEMPORARY TABLE part_16_table AS

    WITH one AS (SELECT Peer, COUNT(*) AS outp, Date
                 FROM TimeTracking
                 WHERE TimeTracking.state = '2'
                 GROUP BY Peer, Date)
    SELECT Peer
    FROM one
    WHERE one.Date >= current_date - N
      AND one.Date <= current_date
      AND one.outp > M;
END;
$$;

-- Проверка работы процедуры
CALL prc_count_out_of_campus(500, 5);
SELECT *
FROM part_16_table;

--17) Определить для каждого месяца процент ранних входов
-- Для каждого месяца посчитай, сколько раз люди, родившиеся в этот месяц, приходили в кампус за всё время (будем называть это общим числом входов). 
-- Для каждого месяца посчитай, сколько раз люди, родившиеся в этот месяц, приходили в кампус раньше 12:00 за всё время (будем называть это числом ранних входов). 
-- Для каждого месяца посчитай процент ранних входов в кампус относительно общего числа входов. 
-- Формат вывода: месяц, процент ранних входов.

CREATE OR REPLACE PROCEDURE prc_early_entry()
    LANGUAGE plpgsql
AS
$$
BEGIN
    -- Удаляем и создаем временную таблицу для результата
    DROP TABLE IF EXISTS part_17_table;
    CREATE TEMPORARY TABLE part_17_table AS

    WITH one AS (SELECT EXTRACT(MONTH FROM Date) AS Month, COUNT(*) AS counts
                 FROM TimeTracking
                          JOIN Peers ON TimeTracking.Peer = Peers.Nickname
                 WHERE TimeTracking.state = '1'
                 GROUP BY Month),
         two AS (SELECT EXTRACT(MONTH FROM Date) as Month, count(*) AS COUNTS
                 FROM TimeTracking
                          JOIN Peers ON Peers.Nickname = TimeTracking.Peer
                 WHERE TimeTracking.Time < '12:00'
                   AND TimeTracking.state = '1'
                 GROUP BY Month)

    SELECT to_char(to_date(one.Month::text, 'MM'), 'Month')    as Months,
           ROUND((sum(two.counts) * 100) / sum(one.counts), 0) AS EarlyEntries
    FROM one
             JOIN two ON one.Month = two.Month
    GROUP BY one.Month
    ORDER BY one.Month;
end;
$$;

-- Проверка работы процедуры
CALL prc_early_entry();
SELECT *
FROM part_17_table;