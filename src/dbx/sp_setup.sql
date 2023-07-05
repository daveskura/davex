    
DROP PROCEDURE IF EXISTS sp_xsetup;
DROP PROCEDURE IF EXISTS sp_inserthdr;
DROP PROCEDURE IF EXISTS sp_insertmetrics;

@delimiter %%%;

CREATE PROCEDURE sp_insertmetrics(ischema varchar(250),itable varchar(250),ifield varchar(250),isample_data varchar(1024),idistinct_values int,tindexes varchar(1024),ifield_comments varchar(1024))
NOT DETERMINISTIC
READS SQL DATA
BEGIN    
    DECLARE metrics_tbls_exists_cnt int DEFAULT -1;
    
	SELECT COUNT(*) INTO metrics_tbls_exists_cnt
	FROM INFORMATION_SCHEMA.columns
    WHERE table_Schema = ischema and table_name = itable and column_name = ifield;

	IF metrics_tbls_exists_cnt > 0 THEN
        DELETE FROM X.metrics
        WHERE schema_name = ischema and table_name = itable and field_name = ifield;

        INSERT INTO X.metrics(schema_name,table_name,field_name,sample_data,distinct_values,indexes,field_comments) VALUES
        (ischema,itable,ifield,isample_data,idistinct_values,tindexes,ifield_comments);
        
    END IF;

END;

-- call sp_insertmetrics('world','city','name','cho',1,'n/a','no field comment')

%%%

CREATE PROCEDURE sp_inserthdr(ischema varchar(250),itable varchar(250),row_counts int,comments varchar(1024))
NOT DETERMINISTIC
READS SQL DATA
BEGIN    
    DECLARE metrics_tbls_exists_cnt int DEFAULT -1;
    
	SELECT COUNT(*) INTO metrics_tbls_exists_cnt
	FROM INFORMATION_SCHEMA.TABLES
    WHERE table_Schema = ischema and table_name = itable;

	IF metrics_tbls_exists_cnt > 0 THEN
        DELETE FROM X.metrics_header
        WHERE schema_name = ischema and table_name = itable;

        INSERT INTO X.metrics_header(schema_name,table_name,row_counts,table_comments) VALUES
        (ischema,itable,row_counts,comments);
        
    END IF;

END;



%%%


CREATE PROCEDURE sp_xsetup()
NOT DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE LOG varchar(2048) DEFAULT '';
	DECLARE schema_exists_cnt int DEFAULT -1;
	DECLARE table_exists_cnt int DEFAULT -1;
    SET LOG = 'Begin: ';
    
	SELECT COUNT(*) INTO schema_exists_cnt
	FROM INFORMATION_SCHEMA.SCHEMATA
	WHERE SCHEMA_NAME = 'X';

	IF schema_exists_cnt > 0 THEN
        SET LOG = concat(LOG,'Schema X exists.  Investigate schema X before proceeding.');
		
	ELSE
        SET LOG = concat(LOG,'Creating schema X;');
		CREATE SCHEMA X;
        SET LOG = concat(LOG,'Creating Tables X.metrics_header');
		CREATE TABLE X.metrics_header (
            schema_name varchar(250),
            table_name varchar(250),
            row_counts int,
            table_comments varchar(1024)
        );
        SET LOG = concat(LOG,', X.metrics;');
		CREATE TABLE X.metrics (
            schema_name varchar(250),
            table_name varchar(250),
            field_name varchar(250),
            sample_data varchar(1024),
            distinct_values int,
            indexes varchar(1024),
            field_comments varchar(1024)
        );

END IF;

    SELECT LOG;
    
END;


%%%
@delimiter ; 
%%%


