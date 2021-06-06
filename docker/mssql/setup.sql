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


CREATE TABLE Sales
(
    id INT PRIMARY KEY,
    day DATE,
    time TIME,
    product INT,
    price DECIMAL(10,2)
)

INSERT INTO SALES
    (id, day, time, product, price)
Values
    (1, '2020-09-09', '00:05:34', 54, 9.99),
    (2, '2020-09-10', '12:05:32', 54, 9.99),
    (3, '2020-09-10', '14:05:32', 34, 2.00);


IF EXISTS (SELECT name FROM sysobjects WHERE name = 'TestParam')  
   DROP PROCEDURE TestParam  
GO  

CREATE PROCEDURE TestParam   
@OutParm int OUTPUT   
AS
SELECT @OutParm = @OutParm + 5  
RETURN 99  
GO  