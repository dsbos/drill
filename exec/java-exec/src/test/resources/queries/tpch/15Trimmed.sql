-- ???? TRIMMED DOWN FOR DEBUGGING

--  T0¦¦l_orderkey<INT(REQUIRED)>
--  T0¦¦l_partkey<INT(REQUIRED)>
--  T0¦¦l_suppkey<INT(REQUIRED)>
--  T0¦¦l_linenumber<INT(REQUIRED)>
--  T0¦¦l_quantity<FLOAT8(REQUIRED)>
--  T0¦¦l_extendedprice<FLOAT8(REQUIRED)>
--  T0¦¦l_discount<FLOAT8(REQUIRED)>
--  T0¦¦l_tax<FLOAT8(REQUIRED)>
--  T0¦¦l_returnflag<VARCHAR(REQUIRED)>
--  T0¦¦l_linestatus<VARCHAR(REQUIRED)>
--  T0¦¦l_shipdate<DATE(REQUIRED)>
--  T0¦¦l_commitdate<DATE(REQUIRED)>
--  T0¦¦l_receiptdate<DATE(REQUIRED)>
--  T0¦¦l_shipinstruct<VARCHAR(REQUIRED)>
--  T0¦¦l_shipmode<VARCHAR(REQUIRED)>
--  T0¦¦l_comment<VARCHAR(REQUIRED)>

select
    max(total_revenue)
from (
    select
        sum(12345) as total_revenue
    from
        (
        SELECT l_suppkey, l_extendedprice
        FROM cp.`tpch/lineitem.parquet`
        WHERE 93 = l_suppkey AND FALSE
        -- trying to add LIMIT here yields UnsupportedOperationException
        )
    group by
        l_suppkey
);


