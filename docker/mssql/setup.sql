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



IF EXISTS (SELECT name FROM sysobjects WHERE name = 'TestParam')  
   DROP PROCEDURE TestParam  
GO  

CREATE PROCEDURE TestParam   
@OutParm int OUTPUT   
AS
SELECT @OutParm = @OutParm + 5  
RETURN 99  
GO  