USE [Reports]
GO

/****** Object:  StoredProcedure [dbo].[_ETL_Autocomplete_SSAS_return_csv]    Script Date: 6/15/2023 4:27:25 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


ALTER procedure [dbo].[_ETL_Autocomplete_SSAS_return_csv]
@Term nvarchar(255)
AS
--oso017 20150421  limiteer resultaten op max 10000 regels
--oso017 20160127  id_bericht vergroot naar bigint (id+datum) 
--oso017 20180103  migratie RIS naar DMAP
--oso    20230323  aangepast om ook SSAS tabular dimensiewaarden te querieen
DECLARE @SearchWord nvarchar(255) --'zaandam'--isnull('pl','####') --isnull(@zoektext,'####')
SET NOCOUNT ON; 

DECLARE @ReturnTable as TABLE ( 
      [MEMBER_CAPTION] nvarchar(4000), [UNIQUE_NAME] nvarchar(255), [EntityPath] nvarchar(255)

	  )
DECLARE @MembercaptionTable as TABLE ( [MEMBER_CAPTION] nvarchar(4000), [ilabel] nvarchar(4000) NULL )

BEGIN
--SearchWord extraheren uit Term (gescheiden met seperators)
DECLARE @DabQueryList as TABLE ( [QueryType] varchar(50), [DabQuery] nvarchar(4000) NULL )--nogtypende,klaar_plus,klaar_minus,klaar_groep
declare @sw nvarchar(255) = @Term --'-- zakenjaar.2021 ++ melding wet.WML -- tussen.door ++ melding jaar.2023 ++ data.jaar -- kvknr.123 ++ als.laats'
declare @semicol int = PATINDEX('% :: %', REVERSE(@sw)) --alternatief is sterster (**) puntkomma is bijna onmogelijk omdat dit in CSV_parses en URL_parses kapot gaat
declare @plusplus int = PATINDEX('% ++ %', REVERSE(@sw))
declare @minmin int = PATINDEX('% -- %', REVERSE(@sw))
declare @lastseperator int = (select min(x*(case x when 0 then null else 1 end)) --nonzero
 from ( select @semicol as x union select @plusplus as x union select @minmin as x ) a
)
declare @endoffilters int = CASE WHEN PATINDEX('% :: %', @sw) = 0 THEN len(@sw) - @lastseperator ELSE PATINDEX('% :: %', @sw) END -- als er geen semicol is neem dan laaste seperator
--select @lastseperator

--select @lastseperator
INSERT INTO @DabQueryList
SELECT [QueryType] = 'nogtypende'
 , [DabQuery] = -- @sw, @lastseperator,
   LTRIM(CASE WHEN @lastseperator > 0 THEN RIGHT(@sw, @lastseperator) --get last (un)finished query
    ELSE @sw --Term zonder seperator
   END)

--split filters, eerst plus filters , en dan met min regels erafhalen
INSERT INTO @DabQueryList
SELECT [QueryType] = 'plus', [DabQuery] =
	CASE WHEN patindex('%--%',LTRIM(c.[value])) = 0 AND patindex('%++ %',LTRIM(c.[value])) = 0 --at end
		THEN LTRIM(c.[value]) 
	WHEN patindex('%--%',LTRIM(c.[value])) > 0 AND patindex('%++ %',LTRIM(c.[value])) = 0 --min but no plus
		THEN LEFT(LTRIM(c.[value]),patindex('%--%',LTRIM(c.[value]))-1)
	END
	--,plusplus_first_minmin = patindex('%-- %',LTRIM(c.[value])),plusplus_first_plusplus = patindex('%++ %',LTRIM(c.[value]))
FROM  STRING_SPLIT(replace(LTRIM(RTRIM(LEFT(@sw,@endoffilters))),' ++',';'),';') c --remove last (un)finished query

INSERT INTO @DabQueryList
SELECT [QueryType] = 'minus', [DabQuery] = 
	CASE WHEN patindex('%-- %',LTRIM(b.[value])) = 0 AND patindex('%++%',LTRIM(b.[value])) = 0 --at end
		THEN LTRIM(b.[value]) 
	WHEN patindex('%-- %',LTRIM(b.[value])) > 0 AND patindex('%++%',LTRIM(b.[value])) = 0 --at start
		THEN replace( LTRIM(b.[value]), '-- ','')
	WHEN patindex('%-- %',LTRIM(b.[value])) > 0 AND patindex('%++%',LTRIM(b.[value])) > 0 --at start
		THEN replace(LEFT(LTRIM(b.[value]),patindex('%++%',LTRIM(b.[value]))-1), '-- ','')
	WHEN patindex('%-- %',LTRIM(b.[value])) = 0 AND patindex('%++%',LTRIM(b.[value])) > 0 --between
		THEN replace(LEFT(LTRIM(b.[value]),patindex('%++%',LTRIM(b.[value]))-1), '++','')
	END
	--,minmin_first_minmin = patindex('%-- %',LTRIM(b.[value])),minmin_first_plusplus = patindex('%++ %',LTRIM(b.[value]))
  FROM  STRING_SPLIT(replace(LTRIM(RTRIM(LEFT(@sw,@endoffilters))),' --',';'),';') b --remove last (un)finished query
 
SELECT TOP 1 @SearchWord = [DabQuery] FROM @DabQueryList WHERE DabQuery != ''
--cascading filters van rest van de query
--3 plussen en 3 minnen opslaan
DECLARE @DabQueryPlus01 nvarchar(255) = ''
DECLARE @DabQueryPlus02 nvarchar(255) = ''
DECLARE @DabQueryPlus03 nvarchar(255) = ''

SELECT TOP 1 @DabQueryPlus01 = CASE WHEN patindex('%*',[DabQuery]) > 1 THEN LEFT([DabQuery],len([DabQuery])-1) ELSE [DabQuery] END --remove trailing asterix *
	FROM @DabQueryList WHERE DabQuery != '' AND [QueryType] = 'plus' --remove trailing asterix *

--klaar extraheren searchword - nogtypende
--select @SearchWord

--intellisense waarden zoeken
IF (SELECT count(1) WHERE @SearchWord like '.[a-Z0-9]%') = 1 --intellisense autocomplete waarden zoeken via waardentabel
BEGIN 
	INSERT INTO @ReturnTable --exact matches (liefst niet meer dan 15 exacte matches)
	SELECT TOP 15
	[MEMBER_CAPTION] = wt.[ssas_waarden]
	,[UNIQUE_NAME] = wt.[ssas_kolom] + '.' + wt.[ssas_waarden]
	,[EntityPath] = '(('+wt.[ssas_kolom]+'))'
	FROM [dbo].[Autocomplete_waardetabel] wt with (nolock)
	WHERE wt.[ssas_waarden] =  RIGHT(@SearchWord,len(@SearchWord)-1) --AND [autocomplete_type] not in ('tabel.kolom', 'tabel.kolom.waarde','.waarden')
	  --ORDER BY [MEMBER_CAPTION] asc, [EntityPath] asc--, datum_melding desc, [id_bericht] desc --ordering maakt traag

	INSERT INTO @ReturnTable --starts with 
	SELECT TOP 10
	[MEMBER_CAPTION] = wt.[ssas_waarden]
	,[UNIQUE_NAME] = wt.[ssas_kolom] + '.' + wt.[ssas_waarden]
	,[EntityPath] = '(('+wt.[ssas_kolom]+'))'
	FROM [dbo].[Autocomplete_waardetabel] wt with (nolock)
	WHERE [ssas_waarden] like '' + RIGHT(@SearchWord,len(@SearchWord)-1) + '%' AND [ssas_waarden] != RIGHT(@SearchWord,len(@SearchWord)-1) --AND [autocomplete_type] not in ('folder.kolom','tabel.kolom', 'tabel.kolom.waarde','.waarden')
		--ORDER BY len([ssas_waarden]) asc, [EntityPath] asc, [MEMBER_CAPTION] asc--, datum_melding desc, [id_bericht] desc --ordering maakt traag
	
	IF (SELECT COUNT(*) FROM @ReturnTable) < 10
            INSERT INTO @ReturnTable-- contains (duurt het langst)
            SELECT TOP 10
			[MEMBER_CAPTION] = wt.[ssas_waarden]
			,[UNIQUE_NAME] = wt.[ssas_kolom] + '.' + wt.[ssas_waarden]
			,[EntityPath] = '(('+wt.[ssas_kolom]+'))'
            FROM [dbo].[Autocomplete_waardetabel] wt with (nolock)
            WHERE wt.[ssas_waarden] like '%' + RIGHT(@SearchWord,len(@SearchWord)-1) + '%' 
              AND wt.[ssas_waarden] NOT LIKE '' + RIGHT(@SearchWord,len(@SearchWord)-1) + '%' --AND [autocomplete_type] not in ('folder.kolom', 'tabel.kolom', 'tabel.kolom.waarde','.waarden')
				--ORDER BY len(wt.[ssas_waarden]) asc, [EntityPath] asc, [MEMBER_CAPTION] asc--, datum_melding desc, [id_bericht] desc --ordering maakt traag

END

IF (SELECT count(1) WHERE @SearchWord like '.*[a-Z0-9]%') = 1 --intellisense autocomplete waarden contains expliciet zoeken via waardentabel
BEGIN 

            INSERT INTO @ReturnTable-- contains (duurt het langst_
            SELECT TOP 20
			[MEMBER_CAPTION] = wt.[ssas_waarden]
			,[UNIQUE_NAME] = wt.[ssas_kolom] + '.' + wt.[ssas_waarden]
			,[EntityPath] = '(('+wt.[ssas_kolom]+'))'

            FROM [dbo].[Autocomplete_waardetabel] wt with (nolock)
            WHERE wt.[ssas_waarden] like '%' + RIGHT(@SearchWord,len(@SearchWord)-2) + '%' 
              AND wt.[ssas_waarden] NOT LIKE '' + RIGHT(@SearchWord,len(@SearchWord)-2) + '%' --AND [autocomplete_type] not in ('folder.kolom', 'tabel.kolom', 'tabel.kolom.waarde','.waarden')
				--ORDER BY len(wt.[ssas_waarden]) asc, [EntityPath] asc, [MEMBER_CAPTION] asc--, datum_melding desc, [id_bericht] desc --ordering maakt traag

END
----
----
--intellisense autocomplet
IF (SELECT count(1) WHERE @SearchWord like '%[a-Z0-9].%') = 1 AND (SELECT count(1) WHERE @SearchWord like '.%') != 1 --intellisense autocomplete table.columns
BEGIN 
	begin
	INSERT INTO @ReturnTable
		SELECT TOP 15 
			[MEMBER_CAPTION]
			,[UNIQUE_NAME]
			,[EntityPath]
		FROM [dbo].[ETL_Autocomplete_zoektabel_schema] with (nolock)
		WHERE [autocomplete_type] = 'tabel.kolom' AND [MEMBER_CAPTION] like '' + @SearchWord + '%' 
			ORDER BY [MEMBER_CAPTION] asc
	  INSERT INTO @ReturnTable --folder.kolom variant
		SELECT TOP 15 
			fk.[MEMBER_CAPTION]
			,fk.[UNIQUE_NAME]
			,fk.[EntityPath]
		FROM [dbo].[ETL_Autocomplete_zoektabel_schema] fk with (nolock)
		  LEFT JOIN @ReturnTable rt ON fk.[UNIQUE_NAME] = rt.[UNIQUE_NAME]
		WHERE rt.[UNIQUE_NAME] is null AND fk.[autocomplete_type] = 'folder.kolom' AND fk.[MEMBER_CAPTION] like '' + @SearchWord + '%' 
			ORDER BY fk.[MEMBER_CAPTION] asc
	end 

	IF (SELECT COUNT(*) FROM @ReturnTable) < 1 --if no columns found, check for column and show values top 20 example d1[plaats hfd] >> values
	begin----insert MDX
            INSERT INTO @ReturnTable
            SELECT TOP 20
            [MEMBER_CAPTION]
            ,[UNIQUE_NAME] = [MEMBER_CAPTION]
            ,[EntityPath] = ''
            FROM [dbo].[ETL_Autocomplete_zoektabel_schema] a with (nolock)
            WHERE [MEMBER_CAPTION] = LEFT(@SearchWord, len(@SearchWord)-1) AND [autocomplete_type] = 'kolom'
				ORDER BY [EntityPath] asc, [MEMBER_CAPTION] asc--, datum_melding desc, [id_bericht] desc
    end----

	IF (SELECT COUNT(*) FROM @ReturnTable) = 1 --if no columns found, check for dimension via dax(tabular) and show values(members) top 20 example d1[plaats hfd] >> values
	begin----insert MDX
		DECLARE @ColumnPart nvarchar(255) --'gemeente hfd.' --@ReturnPart
		DECLARE @ColumnDax nvarchar(255)   --'d1[dgemeente hfd]'--isnull('pl','####') --isnull(@zoektext,'####')

		BEGIN --Waarden van welke ene dimensie opvragen ?
            SELECT TOP 1
            @ColumnPart = [MEMBER_CAPTION]
            ,@ColumnDax = [fullyqualified_columnname]
            FROM [dbo].[ETL_Autocomplete_zoektabel_schema] a with (nolock)
            WHERE [MEMBER_CAPTION] = LEFT(@SearchWord, len(@SearchWord)-1) AND [autocomplete_type] = 'kolom'
		END

		--zonder bedrijven aantallen als ilabel en countrows in dropdown --sneller
		--DECLARE @DaxQuery nvarchar(4000)  = N'
		--SELECT  [MEMBER_CAPTION] = try_convert(nvarchar(4000),"' + @ColumnDax + '")  FROM OPENQUERY(SV2000136SSAS
		--, ''EVALUATE 
		--TOPN( 20, ALL( ' + @ColumnDax + ' )) 
		--'')
		--'
		DECLARE @DaxQuery nvarchar(4000)  = N'
		SELECT  [MEMBER_CAPTION] = try_convert(nvarchar(4000),"[value]"), [ilabel] =  try_convert(nvarchar(4000),"[ilabel]")  FROM OPENQUERY(SV2000136SSAS
		, ''EVALUATE 
		UNION(
		  SUMMARIZECOLUMNS(
		  	' + CASE WHEN @DabQueryPlus01 != '' AND @DabQueryPlus01 like '%.[a-Z0-9]%' --plusquery waarde start NIET met ster, doe left
					  THEN 'KEEPFILTERS( FILTER( VALUES( d1[' + LEFT(@DabQueryPlus01, PATINDEX('%.[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
							+ '] ), LEFT( d1[' + LEFT(@DabQueryPlus01, PATINDEX('%.[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
							+ '] , ' + convert(varchar,len(substring(@DabQueryPlus01,PATINDEX('%.[a-Z0-9]%',@DabQueryPlus01)+1,len(@DabQueryPlus01) ))) --lengte zoektekst
							+ ') = "' + substring(@DabQueryPlus01,PATINDEX('%.[a-Z0-9]%',@DabQueryPlus01)+1,len(@DabQueryPlus01) ) --rechterhelft is zoektekst en of query waarde
							+ '" )), '
					 WHEN @DabQueryPlus01 != '' AND @DabQueryPlus01 like '%.*[a-Z0-9]%' --plusquery waarde start WEL met ster, doe search
					  THEN 'KEEPFILTERS( FILTER( VALUES( d1[' 
							+ LEFT(@DabQueryPlus01, PATINDEX('%.*[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
							+ '] ), SEARCH( "' + substring(@DabQueryPlus01,PATINDEX('%.*[a-Z0-9]%',@DabQueryPlus01)+2,len(@DabQueryPlus01) ) --rechter helft is query waarde
							+ '", d1[' + LEFT(@DabQueryPlus01, PATINDEX('%.*[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
							+ '], 1, 0 ) > 0 )), '
					ELSE '' END
			+ '
			"value", "{COUNTROWS}"
			, "ilabel", "((bevat " & COUNTROWS(VALUES( ' + @ColumnDax + ' )) & " unieke waarden))"
		  )
		, ROW ( "value", "kenmerkwaarden van '+@ColumnPart+':#", "ilabel", "&koptekst2" )
		, TOPN(19, SUMMARIZECOLUMNS(
			' + @ColumnDax + '
			' +CASE WHEN @DabQueryPlus01 != '' AND @DabQueryPlus01 like '%.[a-Z0-9]%' --plusquery waarde start NIET met ster, doe left
					  THEN ', KEEPFILTERS( FILTER( VALUES( d1[' + LEFT(@DabQueryPlus01, PATINDEX('%.[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
							+ '] ), LEFT( d1[' + LEFT(@DabQueryPlus01, PATINDEX('%.[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
							+ '] , ' + convert(varchar,len(substring(@DabQueryPlus01,PATINDEX('%.[a-Z0-9]%',@DabQueryPlus01)+1,len(@DabQueryPlus01) ))) --lengte zoektekst
							+ ') = "' + substring(@DabQueryPlus01,PATINDEX('%.[a-Z0-9]%',@DabQueryPlus01)+1,len(@DabQueryPlus01) ) --rechterhelft is zoektekst en of query waarde
							+ '" ))'
					 WHEN @DabQueryPlus01 != '' AND @DabQueryPlus01 like '%.*[a-Z0-9]%' --plusquery waarde start WEL met ster, doe search
					  THEN ', KEEPFILTERS( FILTER( VALUES( d1[' 
							+ LEFT(@DabQueryPlus01, PATINDEX('%.*[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
							+ '] ), SEARCH( "' + substring(@DabQueryPlus01,PATINDEX('%.*[a-Z0-9]%',@DabQueryPlus01)+2,len(@DabQueryPlus01) ) --rechter helft is query waarde
							+ '", d1[' + LEFT(@DabQueryPlus01, PATINDEX('%.*[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
							+ '], 1, 0 ) > 0 ))'
					ELSE '' END
			+ '
			, "ilabel", "((" & [bedrijven] & " bedrijven, " & [meldingen (over bedrijf)] & " meldingen, " & [zaken (op bedrijf)] & " zaken))"
		  ))	
		)	
		'')
		'

		BEGIN --TOP 20 Waarden van 1 dimensie opvragen
		  DELETE FROM @ReturnTable
		BEGIN --TRY 
			INSERT INTO @MembercaptionTable (MEMBER_CAPTION, ilabel)
			 EXECute sp_executesql @DaxQuery
		END-- TRY
		--BEGIN CATCH
		--END CATCH
		END

		INSERT INTO @ReturnTable
		SELECT TOP 20 --max top 20
		--isnull([MEMBER_CAPTION],'onbekend') as [MEMBER_CAPTION]
		[MEMBER_CAPTION] = CASE WHEN MEMBER_CAPTION = '{COUNTROWS}' THEN '{ ' + @ColumnPart + ' }' ELSE MEMBER_CAPTION END
		, [UNIQUE_NAME]  = CASE WHEN MEMBER_CAPTION = '{COUNTROWS}' THEN @ColumnPart ELSE @ColumnPart  + '.' +  MEMBER_CAPTION END
		, ilabel as [EntityPath]
		--, [UNIQUE_NAME_MDX] = '[d1].[' + @ColumnPart  + '].&[' +  MEMBER_CAPTION  + ']'

		FROM @MembercaptionTable as a
		--ORDER BY len(a.[MEMBER_CAPTION]) asc, [MEMBER_CAPTION] asc

    end----

	--als beginwaarden wordt gegeven (voorbeeld [plaats ves.z) dan zoeken op waarden startend met de tekens
	--syntax is like '%.[a-Z0-9]% woord na punt start met letter of nummer
	IF (SELECT count(1) WHERE @SearchWord like '%.[a-Z0-9]%') = 1  --if no columns found, check for dimension via dax(tabular) and show values(members) top 20 example d1[plaats hfd] >> values
	begin----insert MDX
		DECLARE @ColumnPart3 nvarchar(255) --'gemeente hfd.' --@ReturnPart
		DECLARE @ColumnDax3 nvarchar(255)   --'d1[dgemeente hfd]'--isnull('pl','####') --isnull(@zoektext,'####')
		DECLARE @DaxSearchText nvarchar(255), @DaxSearchTextLength nvarchar(10)
		SET @DaxSearchText = replace( substring(@SearchWord,PATINDEX('%.[a-Z0-9]%',@SearchWord)+1,len(@SearchWord) ) ,'*','') --+1 om punt weg the halen, en trailing *
		SET @DaxSearchTextLength = len(@DaxSearchText)

		BEGIN --Waarden van welke ene dimensie opvragen ?
            SELECT TOP 1
            @ColumnPart3 = [MEMBER_CAPTION]
            ,@ColumnDax3 = [fullyqualified_columnname]
            FROM [dbo].[ETL_Autocomplete_zoektabel_schema] a with (nolock)
            WHERE [MEMBER_CAPTION] =  LEFT(@SearchWord, PATINDEX('%.[a-Z0-9]%', @SearchWord)-1) AND [autocomplete_type] = 'kolom'
		END
		--substring(a,PATINDEX('%.[a-Z0-9]%',a)+1,len(a) ) --+1 om punt weg the halen << search string
		--DECLARE @DaxQuery3 nvarchar(4000)  = N'
		--SELECT [MEMBER_CAPTION] = try_convert(nvarchar(4000),"[value]") FROM OPENQUERY(SV2000136SSAS
		--, ''EVALUATE
		--	SELECTCOLUMNS(
		--	SUMMARIZECOLUMNS(
		--		' + @ColumnDax3 + ' ,
		--		TOPN(20, KEEPFILTERS( FILTER( ALL( ' + @ColumnDax3 + ' ), SEARCH( "' + @DaxSearchText + '", ' + @ColumnDax3 + ', 1, 0 ) = 1 )), ' + @ColumnDax3 + ' , ASC  )
		--	) , "value", ' + @ColumnDax3 + '
		--	) ORDER BY [value] ASC 
		--	'')
		--'
		--)
		--$
		-- ,KEEPFILTERS( FILTER( VALUES( d1[opgericht in] ), SEARCH( "2022-Q*", d1[opgericht in], 1, 0 ) > 0 ))
		DECLARE @DaxQuery3 nvarchar(4000)  = N'
		SELECT  [MEMBER_CAPTION] = try_convert(nvarchar(4000),"[value]"), [ilabel] =  try_convert(nvarchar(4000),"[ilabel]")  FROM OPENQUERY(SV2000136SSAS
		, ''EVALUATE 
		UNION(
		  ROW ( "value", "huidige filter:#", "ilabel", "#koptekst1" )
		  , SUMMARIZECOLUMNS(
			KEEPFILTERS( FILTER( VALUES( ' + @ColumnDax3 + ' ), LEFT( ' + @ColumnDax3 + ' , ' + @DaxSearchTextLength+ ' ) = "' + @DaxSearchText + '" ))
		  	' + CASE WHEN @DabQueryPlus01 != '' AND @DabQueryPlus01 like '%.[a-Z0-9]%' --plusquery waarde start NIET met ster, doe left
					  THEN ', KEEPFILTERS( FILTER( VALUES( d1[' + LEFT(@DabQueryPlus01, PATINDEX('%.[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
							+ '] ), LEFT( d1[' + LEFT(@DabQueryPlus01, PATINDEX('%.[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
							+ '] , ' + convert(varchar,len(substring(@DabQueryPlus01,PATINDEX('%.[a-Z0-9]%',@DabQueryPlus01)+1,len(@DabQueryPlus01) ))) --lengte zoektekst
							+ ') = "' + substring(@DabQueryPlus01,PATINDEX('%.[a-Z0-9]%',@DabQueryPlus01)+1,len(@DabQueryPlus01) ) --rechterhelft is zoektekst en of query waarde
							+ '" )) '
					 WHEN @DabQueryPlus01 != '' AND @DabQueryPlus01 like '%.*[a-Z0-9]%' --plusquery waarde start WEL met ster, doe search
					  THEN ', KEEPFILTERS( FILTER( VALUES( d1[' 
							+ LEFT(@DabQueryPlus01, PATINDEX('%.*[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
							+ '] ), SEARCH( "' + substring(@DabQueryPlus01,PATINDEX('%.*[a-Z0-9]%',@DabQueryPlus01)+2,len(@DabQueryPlus01) ) --rechter helft is query waarde
							+ '", d1[' + LEFT(@DabQueryPlus01, PATINDEX('%.*[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
							+ '], 1, 0 ) > 0 )) '
					ELSE '' END
			+ '
			, "value", " "
			, "ilabel", "$((" & [bedrijven] & " bedrijven, " & [meldingen (over bedrijf)] & " meldingen, " & [zaken (op bedrijf)] & " zaken))"
		  )
		, ROW ( "value", "kenmerkwaarden van '+@ColumnPart3+':#", "ilabel", "&koptekst2" )
		, TOPN( 19, SUMMARIZECOLUMNS(
				' + @ColumnDax3 + '
				, KEEPFILTERS(FILTER( VALUES( ' + @ColumnDax3 + ' ), LEFT( ' + @ColumnDax3 + ' , ' + @DaxSearchTextLength+ ' ) = "' + @DaxSearchText + '" ))
		  		' + CASE WHEN @DabQueryPlus01 != '' AND @DabQueryPlus01 like '%.[a-Z0-9]%' --plusquery waarde start NIET met ster, doe left
						  THEN ', KEEPFILTERS( FILTER( VALUES( d1[' + LEFT(@DabQueryPlus01, PATINDEX('%.[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
								+ '] ), LEFT( d1[' + LEFT(@DabQueryPlus01, PATINDEX('%.[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
								+ '] , ' + convert(varchar,len(substring(@DabQueryPlus01,PATINDEX('%.[a-Z0-9]%',@DabQueryPlus01)+1,len(@DabQueryPlus01) ))) --lengte zoektekst
								+ ') = "' + substring(@DabQueryPlus01,PATINDEX('%.[a-Z0-9]%',@DabQueryPlus01)+1,len(@DabQueryPlus01) ) --rechterhelft is zoektekst en of query waarde
								+ '" )) '
						 WHEN @DabQueryPlus01 != '' AND @DabQueryPlus01 like '%.*[a-Z0-9]%' --plusquery waarde start WEL met ster, doe search
						  THEN ', KEEPFILTERS( FILTER( VALUES( d1[' 
								+ LEFT(@DabQueryPlus01, PATINDEX('%.*[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
								+ '] ), SEARCH( "' + substring(@DabQueryPlus01,PATINDEX('%.*[a-Z0-9]%',@DabQueryPlus01)+2,len(@DabQueryPlus01) ) --rechter helft is query waarde
								+ '", d1[' + LEFT(@DabQueryPlus01, PATINDEX('%.*[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
								+ '], 1, 0 ) > 0 )) '
						ELSE '' END
				+ '
				, "ilabel", "((" & [bedrijven] & " bedrijven, " & [meldingen (over bedrijf)] & " meldingen, " & [zaken (op bedrijf)] & " zaken))"
				) 
		  )	
		)
		'')
		'

		BEGIN --TOP 20 Waarden van 1 dimensie opvragen
		  DELETE FROM @ReturnTable
		BEGIN --TRY 
			INSERT INTO @MembercaptionTable (MEMBER_CAPTION, ilabel)
			 EXECute sp_executesql @DaxQuery3
		END-- TRY
		--BEGIN CATCH
		--END CATCH
		END
		DECLARE @DaxMaar2Waarden int = 0
		SELECT @DaxMaar2Waarden = count(*) FROM @MembercaptionTable

		INSERT INTO @ReturnTable
		SELECT TOP 20 --max top 20
		--isnull([MEMBER_CAPTION],'onbekend') as [MEMBER_CAPTION]
		[MEMBER_CAPTION] = CASE WHEN MEMBER_CAPTION = ' ' THEN '{ ' + replace(@ColumnPart3 + '.' + @DaxSearchText + '*', '**', '*') + ' }' ELSE MEMBER_CAPTION END
		, [UNIQUE_NAME]  = CASE WHEN MEMBER_CAPTION = ' ' THEN replace(@ColumnPart3 + '.' + @DaxSearchText + '*', '**', '*') ELSE @ColumnPart3  + '.' +  MEMBER_CAPTION END
		,ilabel as [EntityPath]
		--, [UNIQUE_NAME_MDX] = '[d1].[' + @ColumnPart  + '].&[' +  MEMBER_CAPTION  + ']'

		FROM @MembercaptionTable as a
		WHERE [MEMBER_CAPTION] != CASE WHEN @DaxMaar2Waarden > 2 THEN '_-_-_' ELSE ' ' END
		ORDER BY ilabel, len(a.[MEMBER_CAPTION]) asc, [MEMBER_CAPTION] asc

    end----

	--zoeken IN tekst (dus niet alleen beginwaarde)
	--syntaxt 
	IF (SELECT count(1) WHERE @SearchWord like '%.*[a-Z0-9]%') = 1  --if no columns found, check for dimension via dax(tabular) and show values(members) top 20 example d1[plaats hfd] >> values
	begin----insert MDX
		DECLARE @ColumnPart4 nvarchar(255) --'gemeente hfd.' --@ReturnPart
		DECLARE @ColumnDax4 nvarchar(255)   --'d1[dgemeente hfd]'--isnull('pl','####') --isnull(@zoektext,'####')
		DECLARE @DaxSearchText2 nvarchar(255)
		SET @DaxSearchText2 = substring(@SearchWord,PATINDEX('%.*[a-Z0-9]%',@SearchWord)+2,len(@SearchWord) ) --+2 om punt EN ster weg the halen

		BEGIN --Waarden van welke ene dimensie opvragen ?
            SELECT TOP 1
            @ColumnPart4 = [MEMBER_CAPTION]
            ,@ColumnDax4 = [fullyqualified_columnname]
            FROM [dbo].[ETL_Autocomplete_zoektabel_schema] a with (nolock)
            WHERE [MEMBER_CAPTION] =  LEFT(@SearchWord, PATINDEX('%.*[a-Z0-9]%', @SearchWord)-1) AND [autocomplete_type] = 'kolom'
		END
		--substring(a,PATINDEX('%.[a-Z0-9]%',a)+1,len(a) ) --+1 om punt weg the halen << search string
		--DECLARE @DaxQuery4 nvarchar(4000)  = N'
		--SELECT [MEMBER_CAPTION] = try_convert(nvarchar(4000),"[value]") FROM OPENQUERY(SV2000136SSAS
		--, ''EVALUATE
		--	SELECTCOLUMNS(
		--	SUMMARIZECOLUMNS(
		--		' + @ColumnDax4 + ' ,
		--		TOPN(20, KEEPFILTERS( FILTER( ALL( ' + @ColumnDax4 + ' ), SEARCH( "' + @DaxSearchText2 + '", ' + @ColumnDax4 + ', 1, 0 ) >= 1 )), ' + @ColumnDax4 + ' , ASC  )
		--	) , "value", ' + @ColumnDax4 + '
		--	) ORDER BY [value] ASC 
		--	'')
		--'
		DECLARE @DaxQuery4 nvarchar(4000)  = N'
		SELECT  [MEMBER_CAPTION] = try_convert(nvarchar(4000),"[value]"), [ilabel] =  try_convert(nvarchar(4000),"[ilabel]")  FROM OPENQUERY(SV2000136SSAS
		, ''EVALUATE
		UNION(
		  ROW ( "value", "huidige filter:#", "ilabel", "#koptekst1" )
		  , SUMMARIZECOLUMNS(
			KEEPFILTERS( FILTER( VALUES(' + @ColumnDax4 + '), SEARCH( "' + @DaxSearchText2 + '", ' + @ColumnDax4 + ', 1, 0 ) >= 1 ))
		  	' + CASE WHEN @DabQueryPlus01 != '' AND @DabQueryPlus01 like '%.[a-Z0-9]%' --plusquery waarde start NIET met ster, doe left
					  THEN ', KEEPFILTERS( FILTER( VALUES( d1[' + LEFT(@DabQueryPlus01, PATINDEX('%.[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
							+ '] ), LEFT( d1[' + LEFT(@DabQueryPlus01, PATINDEX('%.[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
							+ '] , ' + convert(varchar,len(substring(@DabQueryPlus01,PATINDEX('%.[a-Z0-9]%',@DabQueryPlus01)+1,len(@DabQueryPlus01) ))) --lengte zoektekst
							+ ') = "' + substring(@DabQueryPlus01,PATINDEX('%.[a-Z0-9]%',@DabQueryPlus01)+1,len(@DabQueryPlus01) ) --rechterhelft is zoektekst en of query waarde
							+ '" )) '
					 WHEN @DabQueryPlus01 != '' AND @DabQueryPlus01 like '%.*[a-Z0-9]%' --plusquery waarde start WEL met ster, doe search
					  THEN ', KEEPFILTERS( FILTER( VALUES( d1[' 
							+ LEFT(@DabQueryPlus01, PATINDEX('%.*[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
							+ '] ), SEARCH( "' + substring(@DabQueryPlus01,PATINDEX('%.*[a-Z0-9]%',@DabQueryPlus01)+2,len(@DabQueryPlus01) ) --rechter helft is query waarde
							+ '", d1[' + LEFT(@DabQueryPlus01, PATINDEX('%.*[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
							+ '], 1, 0 ) > 0 )) '
					ELSE '' END
			+ '
			, "value", " "
			, "ilabel", "$((" & [bedrijven] & " bedrijven, " & [meldingen (over bedrijf)] & " meldingen, " & [zaken (op bedrijf)] & " zaken))"
		  )
		  , ROW ( "value", "kenmerkwaarden van '+@ColumnPart4+':#", "ilabel", "&koptekst2" )
		  , TOPN( 19, SUMMARIZECOLUMNS(
			 ' + @ColumnDax4 + ' 
			 , KEEPFILTERS(FILTER( VALUES( ' + @ColumnDax4 + ' ), SEARCH( "' + @DaxSearchText2 + '", ' + @ColumnDax4 + ', 1, 0 ) >= 1 ))
		  	 ' + CASE WHEN @DabQueryPlus01 != '' AND @DabQueryPlus01 like '%.[a-Z0-9]%' --plusquery waarde start NIET met ster, doe left
						  THEN ', KEEPFILTERS( FILTER( VALUES( d1[' + LEFT(@DabQueryPlus01, PATINDEX('%.[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
								+ '] ), LEFT( d1[' + LEFT(@DabQueryPlus01, PATINDEX('%.[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
								+ '] , ' + convert(varchar,len(substring(@DabQueryPlus01,PATINDEX('%.[a-Z0-9]%',@DabQueryPlus01)+1,len(@DabQueryPlus01) ))) --lengte zoektekst
								+ ') = "' + substring(@DabQueryPlus01,PATINDEX('%.[a-Z0-9]%',@DabQueryPlus01)+1,len(@DabQueryPlus01) ) --rechterhelft is zoektekst en of query waarde
								+ '" )) '
						 WHEN @DabQueryPlus01 != '' AND @DabQueryPlus01 like '%.*[a-Z0-9]%' --plusquery waarde start WEL met ster, doe search
						  THEN ', KEEPFILTERS( FILTER( VALUES( d1[' 
								+ LEFT(@DabQueryPlus01, PATINDEX('%.*[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
								+ '] ), SEARCH( "' + substring(@DabQueryPlus01,PATINDEX('%.*[a-Z0-9]%',@DabQueryPlus01)+2,len(@DabQueryPlus01) ) --rechter helft is query waarde
								+ '", d1[' + LEFT(@DabQueryPlus01, PATINDEX('%.*[a-Z0-9]%', @DabQueryPlus01)-1) --linker helft query is kenmerk
								+ '], 1, 0 ) > 0 )) '
						ELSE '' END
			+ '
			, "ilabel", "((" & [bedrijven] & " bedrijven, " & [meldingen (over bedrijf)] & " meldingen, " & [zaken (op bedrijf)] & " zaken))"
			)
		   )
		)
		'')
		'

		BEGIN --TOP 20 Waarden van 1 dimensie opvragen
		  DELETE FROM @ReturnTable
		BEGIN --TRY 
			INSERT INTO @MembercaptionTable (MEMBER_CAPTION, ilabel)
			 EXECute sp_executesql @DaxQuery4
		END-- TRY
		--BEGIN CATCH
		--END CATCH
		END

		INSERT INTO @ReturnTable
		SELECT TOP 20 --max top 20
		--isnull([MEMBER_CAPTION],'onbekend') as [MEMBER_CAPTION]
		[MEMBER_CAPTION] = CASE WHEN MEMBER_CAPTION = ' ' THEN '{ ' + replace(@ColumnPart4 + '.*' + @DaxSearchText2 + '*', '**', '*') + ' }' ELSE MEMBER_CAPTION END
		, [UNIQUE_NAME]  = CASE WHEN MEMBER_CAPTION = ' ' THEN replace(@ColumnPart4 + '.*' + @DaxSearchText2 + '*', '**', '*') ELSE @ColumnPart4  + '.' +  MEMBER_CAPTION END
		,ilabel as [EntityPath]
		--, [UNIQUE_NAME_MDX] = '[d1].[' + @ColumnPart  + '].&[' +  MEMBER_CAPTION  + ']'

		FROM @MembercaptionTable as a
		ORDER BY ilabel, len(a.[MEMBER_CAPTION]) asc, [MEMBER_CAPTION] asc



    end----

	--zoeken IN tekst twee keer (dus niet alleen beginwaarde)
	--syntaxt 
	
END

----
--intellisense value searcher 2 (via tabel.kolom.waarde syntax (hierboven is via kolom.waarde syntax)
IF (SELECT count(1) WHERE @SearchWord like '%[a-Z0-9].%.%') = 1 AND (SELECT count(1) WHERE @SearchWord like '.%') != 1 --intellisense autocomplete table.columns.value syntax
BEGIN 
	IF (SELECT COUNT(*) FROM @ReturnTable) < 1 --if no columns found, check for unique column and next step show values top 20 example d1[plaats hfd] >> values
	begin--- deze eerste stap is nodig zodat bovenaan de lijst de getoonde unieke dimensienaam staat
            INSERT INTO @ReturnTable
            SELECT TOP 20
            [MEMBER_CAPTION]
            ,[UNIQUE_NAME] =  LEFT([MEMBER_CAPTION], len([MEMBER_CAPTION])-1) --trailing punt weghalen in resultaat
            ,[EntityPath] = ''
            FROM [dbo].[ETL_Autocomplete_zoektabel_schema] a with (nolock)
            WHERE [MEMBER_CAPTION] = @SearchWord AND [autocomplete_type] = 'tabel.kolom.waarde'
				ORDER BY [EntityPath] asc, [MEMBER_CAPTION] asc--, datum_melding desc, [id_bericht] desc
    end----

	IF (SELECT COUNT(*) FROM @ReturnTable) = 1 --TOON top 20 waarden van dimensie, if return table is still empty, check if this is a table.columns.value syntax (retunr top 20 waarden)
		begin----insert MDX
		DECLARE @ColumnPart2 nvarchar(255) --'d1.gemeente hfd.' --@ReturnPart
		DECLARE @ColumnDax2 nvarchar(255)   --'d1[dgemeente hfd]'--isnull('pl','####') --isnull(@zoektext,'####')

		BEGIN --Waarden van welke ene dimensie opvragen ?
            SELECT TOP 1
            @ColumnPart2 = LEFT([MEMBER_CAPTION], len([MEMBER_CAPTION])-1) --trailing punt weghalen in resultaat
            ,@ColumnDax2 = [fullyqualified_columnname]
            FROM [dbo].[ETL_Autocomplete_zoektabel_schema] a with (nolock)
            WHERE [MEMBER_CAPTION] = @SearchWord AND [autocomplete_type] = 'tabel.kolom.waarde'
		END

		DECLARE @DaxQuery2 nvarchar(4000)  = N'
		SELECT [MEMBER_CAPTION] = try_convert(nvarchar(4000),"[value]") FROM OPENQUERY(SV2000136SSAS
		, ''EVALUATE 
		SELECTCOLUMNS(
		TOPN( 20, ALL( ' + @ColumnDax2 + ' ) ) 
		,"value", ' + @ColumnDax2 + '
		)
		'')
		'
		BEGIN --TOP 20 Waarden van 1 dimensie opvragen
		  DELETE FROM @ReturnTable
		BEGIN --TRY 
			INSERT INTO @MembercaptionTable (MEMBER_CAPTION)
			 EXECute sp_executesql @DaxQuery2
		END-- TRY
		--BEGIN CATCH
		--END CATCH
		END

		INSERT INTO @ReturnTable
		SELECT TOP 20 --max top 20
		--isnull([MEMBER_CAPTION],'onbekend') as [MEMBER_CAPTION]
		MEMBER_CAPTION --= @ColumnPart2 + '.' +  MEMBER_CAPTION
		, [UNIQUE_NAME] = @ColumnPart2  + '.' +  MEMBER_CAPTION--CASE WHEN len(MEMBER_CAPTION) > 40 THEN LEFT(MEMBER_CAPTION,40)+'*' ELSE MEMBER_CAPTION END --selected value == unique and shorter
		,'' as [EntityPath]
		--, [UNIQUE_NAME_MDX] = '[d1].[' + @ColumnPart  + '].&[' +  MEMBER_CAPTION  + ']'

		--,order1 = left(a.[EntityPath],2), order2= len(a.[MEMBER_CAPTION]), order3 = [MEMBER_CAPTION]

		FROM @MembercaptionTable as a
		--ORDER BY order1 asc, order2 asc, [EntityPath] asc, order3 asc

    end----
END

----
--else guesswork autocomplete Metadata searcher
IF ((SELECT count(1) WHERE @SearchWord like '%[a-Z0-9].%') != 1 AND (SELECT count(1) WHERE @SearchWord like '%[a-Z0-9].%.%') != 1 AND (SELECT count(1) WHERE @SearchWord like '.%') != 1)
BEGIN 
INSERT INTO @ReturnTable --exact matches (liefst niet meer dan 15 exacte matches)
SELECT TOP 15
[MEMBER_CAPTION]
,[UNIQUE_NAME]
,[EntityPath]
FROM [dbo].[ETL_Autocomplete_zoektabel_schema] with (nolock)
WHERE [MEMBER_CAPTION] =  @SearchWord AND [autocomplete_type] not in ('tabel.kolom', 'tabel.kolom.waarde','.waarden')
      ORDER BY [MEMBER_CAPTION] asc, [EntityPath] asc--, datum_melding desc, [id_bericht] desc

--IF (SELECT COUNT(*) FROM @ReturnTable) < 1
BEGIN----
INSERT INTO @ReturnTable --starts with 
SELECT TOP 10
[MEMBER_CAPTION]
,[UNIQUE_NAME]
,[EntityPath]

FROM [dbo].[ETL_Autocomplete_zoektabel_schema] with (nolock)
WHERE [MEMBER_CAPTION] like '' + @SearchWord + '%' AND [MEMBER_CAPTION] != @SearchWord AND [autocomplete_type] not in ('folder.kolom','tabel.kolom', 'tabel.kolom.waarde','.waarden')
	ORDER BY left([EntityPath],2) asc, len([MEMBER_CAPTION]) asc, [EntityPath] asc, [MEMBER_CAPTION] asc--, datum_melding desc, [id_bericht] desc

      IF (SELECT COUNT(*) FROM @ReturnTable) < 10 --duurt het langst, pas uitvoeren als bijna niks exact matched of start met searchword
      BEGIN----
            INSERT INTO @ReturnTable-- contains (duurt het langst_
            SELECT TOP 10
            [MEMBER_CAPTION]
            ,[UNIQUE_NAME]
            ,[EntityPath]

            FROM [dbo].[ETL_Autocomplete_zoektabel_schema] with (nolock)
            WHERE [MEMBER_CAPTION] like '%' + @SearchWord + '%' 
              AND [MEMBER_CAPTION] NOT LIKE '' + @SearchWord + '%' AND [autocomplete_type] not in ('folder.kolom', 'tabel.kolom', 'tabel.kolom.waarde','.waarden')
				ORDER BY left([EntityPath],2) asc, len([MEMBER_CAPTION]) asc, [EntityPath] asc, [MEMBER_CAPTION] asc--, datum_melding desc, [id_bericht] desc
      END----

	  --Folder.kolom varianten
	  IF (SELECT COUNT(*) FROM @ReturnTable) < 10
		INSERT INTO @ReturnTable --starts with 
		SELECT TOP 10
		fk.[MEMBER_CAPTION]
		,fk.[UNIQUE_NAME]
		,fk.[EntityPath]

		FROM [dbo].[ETL_Autocomplete_zoektabel_schema] fk with (nolock)
			LEFT JOIN @ReturnTable rt ON fk.UNIQUE_NAME = rt.UNIQUE_NAME --voorkom dubbelle selecties
		WHERE rt.UNIQUE_NAME is null AND fk.[MEMBER_CAPTION] like '' + @SearchWord + '%' AND fk.[MEMBER_CAPTION] != @SearchWord AND [autocomplete_type] in ('folder.kolom')
		ORDER BY left(fk.[EntityPath],2) asc, len(fk.[MEMBER_CAPTION]) asc, [EntityPath] asc, [MEMBER_CAPTION] asc--, datum_melding desc, [id_bericht] desc
	  --Folder.kolom varianten
	  IF (SELECT COUNT(*) FROM @ReturnTable) < 10
	    INSERT INTO @ReturnTable-- contains (duurt het langst_
            SELECT TOP 10
            fk.[MEMBER_CAPTION]
            ,fk.[UNIQUE_NAME]
            ,fk.[EntityPath]

            FROM [dbo].[ETL_Autocomplete_zoektabel_schema] fk with (nolock)
				LEFT JOIN @ReturnTable rt ON fk.UNIQUE_NAME = rt.UNIQUE_NAME --voorkom dubbelle selecties
            WHERE rt.UNIQUE_NAME is null AND fk.[MEMBER_CAPTION] like '%' + @SearchWord + '%' 
              AND fk.[MEMBER_CAPTION] NOT LIKE '' + @SearchWord + '%' AND [autocomplete_type]  in ('folder.kolom')
				ORDER BY left(fk.[EntityPath],2) asc, len(fk.[MEMBER_CAPTION]) asc, fk.[EntityPath] asc, fk.[MEMBER_CAPTION] asc--, datum_melding desc, [id_bericht] desc

END----
END

--SELECT * FROM @ReturnTable

--reuturn gehele query plus waarde, autocomplete plus prettifier van hele query
SELECT TOP 20 --max top 20
--isnull([MEMBER_CAPTION],'onbekend') as [MEMBER_CAPTION]
MEMBER_CAPTION = replace(replace( CASE WHEN left([EntityPath],2) in ('(u') 
 THEN ''''+ isnull([MEMBER_CAPTION],'onbekend') +'''' + [EntityPath] +'' 
 ELSE ''+ isnull([MEMBER_CAPTION],'onbekend') + [EntityPath] +'' 
 END ,'"',''),';','') --value-seperator helper, opgevangen in JS --label / userfriendly dropdown-caption

 , [UNIQUE_NAME] =  CASE WHEN @lastseperator > 0 THEN LEFT(@sw, (len(@sw)-@lastseperator) ) + ' ' + [UNIQUE_NAME]
	ELSE ''+ [UNIQUE_NAME] +''  END --complete query teruggeven aan autocomplete

,[EntityPath] as [EntityPath]

--,order1 = left(a.[EntityPath],2), order2= len(a.[MEMBER_CAPTION]), order3 = [MEMBER_CAPTION]
FROM @ReturnTable as a
--ORDER BY order2 asc, [EntityPath] asc, order3 asc
RETURN;
 
END----




GO


