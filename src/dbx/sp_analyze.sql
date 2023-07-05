DROP PROCEDURE IF EXISTS sp_analyze;

@delimiter %%%;

CREATE PROCEDURE sp_analyze(this_schema varchar(255),this_table varchar(255))
NOT DETERMINISTIC
READS SQL DATA
BEGIN
	DECLARE @Query VARCHAR(1000)
	DECLARE finished INTEGER DEFAULT 0;
	DECLARE irow_counts int DEFAULT -1;
	DECLARE itable_comment varchar(1024) DEFAULT "";
	DECLARE ifield_comment varchar(1024) DEFAULT "";
	DECLARE col_name varchar(250) DEFAULT "";

	DECLARE isdata VARCHAR(250) DEFAULT "";
	DECLARE idistv VARCHAR(250) DEFAULT -1;
	DECLARE iindexs VARCHAR(250) DEFAULT "";

	DECLARE my_columns cursor  FOR
	SELECT column_name,column_comment
	FROM information_schema.columns
	WHERE table_schema=this_schema and table_name=this_table
	ORDER BY ordinal_position;
	
	-- declare NOT FOUND handler
	DECLARE CONTINUE HANDLER 
        FOR NOT FOUND SET finished = 1;

	SELECT T.TABLE_ROWS
		,T.TABLE_COMMENT INTO irow_counts,itable_comment
	FROM INFORMATION_SCHEMA.TABLES T
	WHERE table_schema=this_schema and table_name=this_table;

	call sp_inserthdr(this_schema,this_table ,irow_counts,itable_comment);

	OPEN my_columns;
	getcoldetal: LOOP
		FETCH my_columns INTO col_name,ifield_comment;

		IF finished = 1 THEN 
			LEAVE getcoldetal;
		END IF;
		
		SET @query='SELECT count(DISTINCT col_name) INTO @idistv FROM this_schema.this_table;'
		EXECUTE(@query)
			
		SET @query='SELECT concat(''[ '',col_name ,'' ][ '', coalesce(LEAD(col_name,1) OVER (ORDER BY col_name),'''') '
				+ ','' ][ '', coalesce(LEAD(col_name,2) OVER (ORDER BY col_name),'''')  ,'' ]'') as sample_data '
				+ '	FROM (SELECT DISTINCT col_name FROM this_schema.this_table) dist_qry ORDER BY col_name limit 1'
		EXECUTE(@query)
				
			SELECT
				indexes INTO iindexs
				FROM 
				(				
				SELECT
					concat(coalesce(INDEX_NAME,'')
					,'; ',coalesce(LEAD(INDEX_NAME,1) OVER (ORDER BY INDEX_NAME))
					,'; ',coalesce(LEAD(INDEX_NAME,2) OVER (ORDER BY INDEX_NAME))
					,'; ',coalesce(LEAD(INDEX_NAME,3) OVER (ORDER BY INDEX_NAME))
					,'; ',coalesce(LEAD(INDEX_NAME,4) OVER (ORDER BY INDEX_NAME))
					) as indexes
					FROM INFORMATION_SCHEMA.STATISTICS
					WHERE TABLE_SCHEMA= this_schema 
									and TABLE_NAME = this_table
									and COLUMN_NAME = col_name
					) indexqry ;
		
		call sp_insertmetrics(this_schema,this_table,col_name,isdata,idistv,iindexs,ifield_comment);

	END LOOP getcoldetal;		


END;

%%%
@delimiter ; 
%%%
call sp_analyze('world','city');
