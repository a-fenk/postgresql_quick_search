class SQLGeneratorService:
    def __init__(
            self,
            table: str,
            table_schema: str = 'public',
            extension_schema: str = 'ext',
    ):
        self.__extension_schema = extension_schema
        self.__table_schema = table_schema
        self.__table = table

    def get_sql_commands_in_order(self) -> list[str]:
        return [
            self.add_extension_schema(),
            self.create_intarray_extension(),
            self.create_uuid_ossp_extension(),
            self.drop_table(),
            self.create_table(),
            self.create_table_index(),
            self.truncate_table(),
            self.create_function(),
            # self.alter_function(),
        ]

    def add_extension_schema(self) -> str:
        return f"""    
-- Add schema for extension
CREATE SCHEMA IF NOT EXISTS {self.__extension_schema}
"""

    def create_intarray_extension(self) -> str:
        return f"""
-- Add intarray extension
CREATE EXTENSION IF NOT EXISTS intarray
    SCHEMA {self.__extension_schema}
    VERSION "1.2";
"""

    def create_uuid_ossp_extension(self) -> str:
        return f"""
-- Add uuid-ossp extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp"
"""

    def drop_table(self) -> str:
        return f"""
DROP TABLE IF EXISTS {self.__table_schema}.{self.__table};
"""

    def create_table(self) -> str:
        return f"""
-- Create table of candidates, with parameters
CREATE TABLE {self.__table_schema}.{self.__table}(
    id uuid DEFAULT uuid_generate_v4 (),
    parameter_array integer[],		-- each element: (parameter_id << 16) + (parameter_value << 0)
    parameter_json jsonb,			-- "keyName": [<array of key values>], e.g. "stack": [1,5,20]
    CONSTRAINT {self.__table}_pkey PRIMARY KEY (id)
) WITH (
    autovacuum_enabled = TRUE,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 256,
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 256
)
TABLESPACE pg_default;
"""

    def create_table_index(self) -> str:
        return f"""
CREATE INDEX IF NOT EXISTS {self.__table}_json_index
ON {self.__table_schema}.{self.__table} USING gin
(parameter_json jsonb_path_ops)
TABLESPACE pg_default;
"""

    def truncate_table(self) -> str:
        return f"""
TRUNCATE {self.__table_schema}.{self.__table};
"""

    def create_function(self) -> str:
        return f"""
CREATE OR REPLACE FUNCTION {self.__table_schema}.{self.__table}(
	ljinput jsonb DEFAULT '{{\"apiVersion\": \"0.0.1\"}}'::jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$

-- Select the best items based on input conditions
--	=> {{ 
--      \"ids\":  []  = array of prefiltered ids
--      \"mandatory\": {{ \"<key>\": [numeric values] }}  = all mandatory parameters
--      \"strict\":  [ {{ \"id\", \"value\", \"weight\" }} ]  = strict parameters, with weights (float value)
--      \"bonus\":   [ {{ \"id\", \"value\", \"weight\" }} ]  = flexible parameters, with bonus equal to difference * weight (float value)
--      \"penalty\": [ {{ \"id\", \"value\", \"weight\" }} ]  = flexible parameters, with penalty equal to difference * weight (float value)
--      \"page\": page ID
--      \"onPage\": items per page
--      \"minRating\": minimal rating for items

--  }}

--      <= "data": [ { "id", "rating" }, "total": int ]

DECLARE
	-- Exception diagnostics
    lcExceptionContext text;
    lcExceptionDetail text;
    lcExceptionHint text;
    lcExceptionMessage text;
    lcExceptionState text;

    -- Constants
    PARAM_VALUE_BITMASK		integer := 15;

    PARAM_ID_OFFSET         integer := 16;      -- bit offset for parameter ID
    PARAM_MAX_VALUE         integer := 65536;   -- maximal parameter value
	
	-- Input parameters
    lnLimit                 integer := COALESCE(ljInput->>'onPage', '64')::integer;
    lnOffset                integer := (COALESCE(ljInput->>'page', '1')::integer - 1) * lnLimit;
    lnMinRating             integer := COALESCE(ljInput->>'minRating', '0')::integer;
    
    ljMandatory             jsonpath;           -- jsonpath for mandatory keys
    
    ids                   	uuid[];             -- array of ids
    
    laStrictValue           integer[];          -- array of strict parameters: (id << PARAM_ID_OFFSET) + value
    laStrictWeight          float[];            -- array of weights for strict parameters

    laBonusValue            integer[];          -- array of bonus parameters: (id << PARAM_ID_OFFSET) + value
    laBonusWeight           float[];            -- array of weights for bonus parameters
    
    laPenaltyValue          integer[];          -- array of penalty parameters: (id << PARAM_ID_OFFSET) + value
    laPenaltyWeight         float[];            -- array of weights for penalty parameters
    
	ljResult                jsonb;
	total					int;
        
begin
	IF jsonb_typeof(ljInput->'ids') IS NOT DISTINCT FROM 'array' then
	
        SELECT
            array_agg(
                value::uuid
            )
        INTO ids
		FROM jsonb_array_elements_text(ljInput->'ids')
		;
    END IF;
	
        
    -- Calculate jsonpath for mandatory parameters
    ljMandatory := CASE
        WHEN jsonb_typeof(ljInput->'mandatory') IS DISTINCT FROM 'object' 
        THEN NULL::jsonpath
        ELSE (
            SELECT
                string_agg(
                    CASE
                        WHEN jsonb_typeof(value) = 'array' THEN
                                concat(
                                    '(',
                                    (
                                        SELECT
                                            string_agg(concat('$.', key, ' == ', param_value), ' && ')
                                        FROM jsonb_array_elements_text(value) param_value
                                        WHERE param_value ~ '^[0-9]{{1,6}}$'
                                            AND param_value::integer < PARAM_MAX_VALUE
                                    ),
                                    ')'
                                )
                        WHEN jsonb_typeof(value) = 'number'
                                AND value::text ~ '^[0-9]{{1,6}}$'
                                AND value::text::integer < PARAM_MAX_VALUE THEN 
                            concat('$.', key, ' == ', value::text)
                        WHEN jsonb_typeof(value) = 'string'
                                AND (value #>> '{{}}') ~ '^[0-9]{{1,6}}$'
                                AND (value #>> '{{}}')::integer < PARAM_MAX_VALUE THEN
                            concat('$.', key, ' == ', (value #>> '{{}}'))
                        ELSE '0 == 0'
                    END,
                    ' && '
                )
            FROM jsonb_each(ljInput->'mandatory')
        )::jsonpath
    END;
    
    -- Calculate array of strict parameters
    IF jsonb_typeof(ljInput->'strict') IS NOT DISTINCT FROM 'array' THEN
    
        SELECT
            array_agg(
                (
	             	((value->>'id')::integer << PARAM_ID_OFFSET)
	                +
	                (value->>'value')::integer
                )
                order by (value->>'id')::integer << PARAM_ID_OFFSET
            ),
            
            array_agg(
                (value->>'weight')::integer
                order by (value->>'id')::integer << PARAM_ID_OFFSET
            )
        INTO laStrictValue, laStrictWeight
        FROM jsonb_array_elements(ljInput->'strict')
        WHERE (value->>'value')::integer < PARAM_MAX_VALUE
        ;
    
    END IF;
    
    
    
    -- Calculate array of bonus parameters
    IF jsonb_typeof(ljInput->'bonus') IS NOT DISTINCT FROM 'array' THEN
    
        SELECT
            array_agg(
                (
	             	((value->>'id')::integer << PARAM_ID_OFFSET)
	                +
	                (value->>'value')::integer
                )
                order by (value->>'id')::integer << PARAM_ID_OFFSET
            ),
            
            array_agg(
                (value->>'weight')::integer
                order by (value->>'id')::integer << PARAM_ID_OFFSET
            )
        INTO laBonusValue, laBonusWeight
        FROM jsonb_array_elements(ljInput->'bonus')
        WHERE (value->>'value')::integer < PARAM_MAX_VALUE
        ;
    
    END IF;
    
    
    
    -- Calculate array of penalty parameters
    IF jsonb_typeof(ljInput->'penalty') IS NOT DISTINCT FROM 'array' THEN
    
        SELECT
            array_agg(
                (
	             	((value->>'id')::integer << PARAM_ID_OFFSET)
	                +
	                (value->>'value')::integer
                )
                order by (value->>'id')::integer << PARAM_ID_OFFSET
            ),
            
            array_agg(
                (value->>'weight')::integer
                order by (value->>'id')::integer << PARAM_ID_OFFSET
            )
        INTO laPenaltyValue, laPenaltyWeight
        FROM jsonb_array_elements(ljInput->'penalty')
        WHERE (value->>'value')::integer < PARAM_MAX_VALUE
        ;
    
    END IF;
    
    -- Build response
    WITH 
    -- Select all items based on mandatory parameters, with \"strict\" array intersection
    "items_list" AS MATERIALIZED (
        SELECT
            id,
            parameter_array,
            (laStrictValue OPERATOR(ext.&) \"{self.__table}\".parameter_array) AS strict_intersect
        FROM {self.__table_schema}.{self.__table}
        WHERE ("{self.__table}".id = any(ids) OR ids IS NULL) AND (
	        ljMandatory IS NULL OR "{self.__table}".parameter_json @@ ljMandatory
	    )
    ),
    
    -- Calculate rating for each item based on parameter_array
    "items_rating" AS MATERIALIZED (
        SELECT
            "items_list".id,
            
            COALESCE(
            (   -- Weighted strict vector
                SELECT
                    SUM(laStrictWeight[strict_elem.i]::integer)
                FROM unnest("items_list".strict_intersect) WITH ORDINALITY strict_elem(id, i)
            ), 0)
            +
            COALESCE(
            (   -- Weighted bonus vector
                SELECT
                    SUM(
                        laBonusWeight[bonus.i]::integer
                        * GREATEST((element_id & PARAM_VALUE_BITMASK) - (bonus.id & PARAM_VALUE_BITMASK), 0)
                    )
                FROM unnest(laBonusValue) WITH ORDINALITY bonus(id, i)
                INNER JOIN unnest("items_list".parameter_array) element_id ON
                    element_id >> PARAM_ID_OFFSET = bonus.id >> PARAM_ID_OFFSET
            ), 0)
            -
            COALESCE(
            (   -- Weighted penalty vector
                SELECT
                    SUM(
                        laPenaltyWeight[penalty.i]::integer
                        * GREATEST((penalty.id & PARAM_VALUE_BITMASK) - (element_id & PARAM_VALUE_BITMASK), 0)
                    )
                FROM unnest(laPenaltyValue) WITH ORDINALITY penalty(id, i)
                INNER JOIN unnest("items_list".parameter_array) element_id ON
                    element_id >> PARAM_ID_OFFSET = penalty.id >> PARAM_ID_OFFSET
            ), 0)
            AS rating
        FROM "items_list"
        WHERE COALESCE(
            (   -- Weighted strict vector
                SELECT
                    SUM(laStrictWeight[strict_elem.i]::integer)
                FROM unnest("items_list".strict_intersect) WITH ORDINALITY strict_elem(id, i)
            ), 0)
            +
            COALESCE(
            (   -- Weighted bonus vector
                SELECT
                    SUM(
                        laBonusWeight[bonus.i]::integer
                        * GREATEST((element_id & PARAM_VALUE_BITMASK) - (bonus.id & PARAM_VALUE_BITMASK), 0)
                    )
                FROM unnest(laBonusValue) WITH ORDINALITY bonus(id, i)
                INNER JOIN unnest("items_list".parameter_array) element_id ON
                    element_id >> PARAM_ID_OFFSET = bonus.id >> PARAM_ID_OFFSET
            ), 0)
            -
            COALESCE(
            (   -- Weighted penalty vector
                SELECT
                    SUM(
                        laPenaltyWeight[penalty.i]::integer
                        * GREATEST((penalty.id & PARAM_VALUE_BITMASK) - (element_id & PARAM_VALUE_BITMASK), 0)
                    )
                FROM unnest(laPenaltyValue) WITH ORDINALITY penalty(id, i)
                INNER JOIN unnest("items_list".parameter_array) element_id ON
                    element_id >> PARAM_ID_OFFSET = penalty.id >> PARAM_ID_OFFSET
            ), 0) >= lnMinRating
        ORDER BY rating DESC
        LIMIT lnLimit
        OFFSET lnOffset
    ),
    
    -- calculating total results counter
    "total" as materialized (
   		SELECT
           count("items_rating".id)
        FROM "items_rating"
    )
    
    SELECT
        jsonb_agg(
            jsonb_build_object(
                'id', "items_rating".id,
                'rating', "items_rating".rating
            )
        ),
        "total".count
    INTO ljResult, total
    FROM "items_rating", "total"
    group by "total".count;
   	
    
    
        RETURN jsonb_build_object(
        'data', ljResult,
        'total', total
        );
    
    
EXCEPTION
    WHEN others THEN
        GET STACKED DIAGNOSTICS
            lcExceptionContext = PG_EXCEPTION_CONTEXT,
            lcExceptionDetail  = PG_EXCEPTION_DETAIL,
            lcExceptionHint    = PG_EXCEPTION_HINT,
            lcExceptionMessage = MESSAGE_TEXT,
            lcExceptionState   = RETURNED_SQLSTATE;
 
        RETURN jsonb_build_object(
            'error_context', lcExceptionContext,
            'error_detail', lcExceptionDetail,
            'error_hint', lcExceptionHint,
            'error_message', lcExceptionMessage,
            'error_state', lcExceptionState
        );
END;
$BODY$;
"""

#     def alter_function(self):
#         return f"""
# ALTER FUNCTION {self.__table_schema}.{self.__table}(jsonb)
# OWNER TO postgres;
# """
