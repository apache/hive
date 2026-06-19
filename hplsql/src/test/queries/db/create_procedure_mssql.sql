USE [default]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [spTest1]
  @lim INT,
  @lim2 INT
AS
  SET NOCOUNT ON
  SET XACT_ABORT ON

  DECLARE @cnt int
  SET @cnt = 0
  SELECT @cnt = COUNT(*) from [default].[src] LIMIT @lim2
  IF @cnt <= 0
    SELECT 'Failed' FROM [src] LIMIT 1
  ELSE 
    BEGIN
      SELECT 'Correct' FROM default.[src] LIMIT 1
    END
GO 

ALTER PROCEDURE spTest2
  @lim INT,
  @lim2 TINYINT,
  @lim3 SMALLINT
AS
BEGIN
  DECLARE @cnt int
  SET @cnt = 0
  SELECT @cnt = COUNT(*) from src LIMIT @lim
  IF @cnt <= 0
    SELECT 'Failed' FROM src LIMIT 1
  ELSE 
    BEGIN
      SELECT 'Correct' FROM src LIMIT 1
    END
END

ALTER PROCEDURE spTest3
AS
  SELECT 'Correct' FROM src LIMIT 1
GO
  
EXEC [spTest1] @lim2 = 3
GO
EXECUTE spTest2 3
GO
EXECUTE spTest3
GO