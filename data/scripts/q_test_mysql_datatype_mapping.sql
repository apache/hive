CREATE TABLE work_calendar
(
    year  INT    NOT NULL,
    month INT    NOT NULL,
    week  INT    NOT NULL,
    days  BIT(7) NOT NULL
);
INSERT INTO work_calendar
VALUES (2022, 4, 1, b'1111100'),
       (2022, 5, 3, b'0011100'),
       (2022, 6, 2, b'0000110');


CREATE TABLE work_attendance
(
    name       CHAR(10) NOT NULL,
    ATTENDANCE BIT(1)   NOT NULL
);
INSERT INTO work_attendance
VALUES ('JACK', 1),
       ('TOM', 0);
