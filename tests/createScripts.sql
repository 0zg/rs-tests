ALTER procedure [dbo].[_ETL_Autocomplete_return_csv]

@Term nvarchar(255)
as
--oso017 20150421  limiteer resultaten op max 10000 regels
--oso017 20160127  id_bericht vergroot naar bigint (id+datum) 
--oso017 20180103  migratie RIS naar DMAP

DECLARE @ReturnTable as TABLE ( 
      [MEMBER_CAPTION] nvarchar(4000), [UNIQUE_NAME] nvarchar(255), [EntityPath] nvarchar(255)
	 -- , [id_bericht] bigint, datum_melding int,id_melding int,zaaknummer varchar(50),sofinr_num int
	  )

DECLARE @SearchWord nvarchar(255) = isnull(@Term,'####') --isnull(@zoektext,'####')

BEGIN----
--intellisense autocomplet
IF (SELECT count(1) WHERE @SearchWord like '%.%.%') = 1 --intellisense autocomplete table.column.values
	begin --select * FROM ETL_Autocomplete_searchtable WHERE columnname like  substring(@SearchWord, patindex('%.%',@SearchWord)+1, len(@SearchWord)) + '%'
	INSERT INTO @ReturnTable
		SELECT TOP 20
			LEFT(@SearchWord, patindex('%.%',@SearchWord)-1) +'.'+ ColumnName as [UNIQUE_NAME]
			,ColumnName as [MEMBER_CAPTION]
			,'' as [EntityPath]
		FROM [dbo].[ETL_Autocomplete_searchtable]
		WHERE left(MeldingBlok,2) = '(u' 
			AND  columnname like '' + substring(@SearchWord, patindex('%.%',@SearchWord)+1, len(@SearchWord)) + '%' 
			ORDER BY columnname asc
	end

IF (SELECT count(1) WHERE @SearchWord like '%.%') = 1 --intellisense autocomplete table.columns
BEGIN 
	begin
	INSERT INTO @ReturnTable
		SELECT TOP 15 
			columnname as [MEMBER_CAPTION]
			,ColumnName as [UNIQUE_NAME]
			,'' as [EntityPath]
		FROM [dbo].[ETL_Autocomplete_searchtable]
		WHERE left(MeldingBlok,2) = '(i' AND columnname like '' + @SearchWord + '%' 
			ORDER BY columnname asc
	end

	IF (SELECT COUNT(*) FROM @ReturnTable) < 1 --if no columns found, check for column.values pair (column.value)
	BEGIN----
            INSERT INTO @ReturnTable
            SELECT TOP 20
            ColumnName as [MEMBER_CAPTION]
            ,ColumnName as [UNIQUE_NAME]
            ,'' as [EntityPath]

            FROM [dbo].[ETL_Autocomplete_searchtable] with (nolock)
            WHERE ColumnName like '' + @SearchWord + '%'
				ORDER BY [EntityPath] asc, [Value] asc--, datum_melding desc, [id_bericht] desc
    END----
END


--SELECT * FROM @ReturnTable
--end

--else guesswork autocomplete
IF ((SELECT count(1) WHERE @SearchWord like '%.%') != 1 AND (SELECT count(1) WHERE @SearchWord like '%.%.%') != 1)
BEGIN 
INSERT INTO @ReturnTable --exact matches (liefst niet meer dan 15 exacte matches)
SELECT TOP 15
[Value] as [MEMBER_CAPTION]
,ColumnName as [UNIQUE_NAME]
,MeldingBlok as [EntityPath]
FROM [dbo].[ETL_Autocomplete_searchtable] with (nolock)
WHERE [Value] =  @SearchWord 
      ORDER BY [Value] asc, [EntityPath] asc--, datum_melding desc, [id_bericht] desc

--IF (SELECT COUNT(*) FROM @ReturnTable) < 1
BEGIN----
INSERT INTO @ReturnTable --starts with 
SELECT TOP 10
[Value] as [MEMBER_CAPTION]
,ColumnName as [UNIQUE_NAME]
,MeldingBlok as [EntityPath]

FROM [dbo].[ETL_Autocomplete_searchtable] with (nolock)
WHERE [Value] like '' + @SearchWord + '%' AND [Value] != @SearchWord
	ORDER BY left(MeldingBlok,2) asc, len([Value]) asc, [EntityPath] asc, [Value] asc--, datum_melding desc, [id_bericht] desc

      IF (SELECT COUNT(*) FROM @ReturnTable) < 5 --duurt het langst, pas uitvoeren als bijna niks exact matched of start met searchword
      BEGIN----
            INSERT INTO @ReturnTable-- contains (duurt het langst_
            SELECT TOP 10
            [Value] as [MEMBER_CAPTION]
            ,ColumnName as [UNIQUE_NAME]
            ,MeldingBlok as [EntityPath]

            FROM [dbo].[ETL_Autocomplete_searchtable] with (nolock)
            WHERE [Value] like '%' + @SearchWord + '%' 
              AND [Value] NOT LIKE '' + @SearchWord + '%'
				ORDER BY left(MeldingBlok,2) asc, len([Value]) asc, [EntityPath] asc, [Value] asc--, datum_melding desc, [id_bericht] desc
      END----

END----
END
--SELECT * FROM @ReturnTable


SELECT TOP 20 --max top 20
--isnull([MEMBER_CAPTION],'onbekend') as [MEMBER_CAPTION]
MEMBER_CAPTION = CASE WHEN left([EntityPath],2) in ('(u') 
 THEN ''''+ isnull([MEMBER_CAPTION],'onbekend') +'''' + [EntityPath] +'' 
 ELSE ''+ isnull([MEMBER_CAPTION],'onbekend') + [EntityPath] +'' 
 END --label / userfriendly dropdown-caption
, ''+ [UNIQUE_NAME]+'' as [UNIQUE_NAME] --selected value == unique and shorter
,[EntityPath] as [EntityPath]

--,order1 = left(a.[EntityPath],2), order2= len(a.[MEMBER_CAPTION]), order3 = [MEMBER_CAPTION]

FROM @ReturnTable as a
--ORDER BY order1 asc, order2 asc, [EntityPath] asc, order3 asc

 
  
END----
GO


ALTER view [dbo].[ETL_Autocomplete_searchtable]
as

SELECT distinct
[value] = TABLE_NAME
,[columnname] = TABLE_NAME --'table ' + TABLE_TYPE
,MeldingBlok = '.''kenmerken'''
FROM INFORMATION_SCHEMA.TABLES

union all
SELECT distinct
[value] = COLUMN_NAME
,[columnname] = TABLE_NAME + '.' + COLUMN_NAME --'column '
,MeldingBlok = '(in ' + TABLE_NAME+ ')'
FROM INFORMATION_SCHEMA.COLUMNS

union all
SELECT DISTINCT [value]=val, [columnname]=col+'.'+val , MeldingBlok= '(uit ' + col +' in logs)'
FROM   
   (SELECT --entitypath = 'column' 
   [LogEntryId]=try_convert(nvarchar(50),[LogEntryId])
   , [RequestType]=try_convert(nvarchar(50),[RequestType])
   , [Format]=try_convert(nvarchar(50),[Format])
   , [TimeStart]=try_convert(nvarchar(50),[TimeStart],102)
   , [Status]  =try_convert(nvarchar(50),[Status])
   FROM [dbo].[ExecutionLogStorage] with (nolock) ) p
UNPIVOT (val FOR col IN   
      ([LogEntryId], [RequestType], [Format], [TimeStart], [Status] )  
)AS unpvt

GO


