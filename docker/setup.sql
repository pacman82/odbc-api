CREATE TABLE Movies(
    title VARCHAR(255) NOT NULL,
    year int
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