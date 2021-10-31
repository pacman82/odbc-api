CREATE TABLE Movies
(
    title VARCHAR(255) NOT NULL,
    year INT
);

INSERT INTO Movies
    (title, year)
Values
    ('Jurassic Park', 1993),
    ('2001: A Space Odyssey', 1968),
    ('Interstellar', NULL);
