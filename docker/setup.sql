CREATE TABLE Movies(
    title VARCHAR(255) NOT NULL,
    year INT
);

INSERT INTO Movies (title, year) Values ('Jurassic Park', 1993);
INSERT INTO Movies (title, year) Values ('2001: A Space Odyssey', 1968);
INSERT INTO Movies (title, year) Values ('Interstellar', NULL);

CREATE TABLE Birthdays(
    name VARCHAR(255) NOT NULL,
    birthday DATE NOT NULL,
)

INSERT INTO BIRTHDAYS (name, birthday) Values ('Keanu Reeves', '1964-09-02');
INSERT INTO BIRTHDAYS (name, birthday) Values ('Robin Wiliams', '1951-07-21');

CREATE TABLE Sales(
    id INT PRIMARY KEY,
    day DATE,
    time TIME,
    product INT,
    price DECIMAL(10,2)
)

INSERT INTO SALES (id, day, time, product, price) Values (1, '2020-09-09', '00:05:34', 54, 9.99);
INSERT INTO SALES (id, day, time, product, price) Values (2, '2020-09-10', '12:05:32', 54, 9.99);
INSERT INTO SALES (id, day, time, product, price) Values (3, '2020-09-10', '14:05:32', 34, 2.00);

CREATE TABLE IntegerDecimals(
    three DECIMAL(3,0),
    nine DECIMAL(9,0),
    eighteen DECIMAL(18,0),
)

INSERT INTO IntegerDecimals (three, nine, eighteen) Values (123, 123456789, 123456789012345678);
