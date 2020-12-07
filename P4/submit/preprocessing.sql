create index o_key2 
on orders(o_custkey, o_orderdate);

create index o_key3 
on orders(o_orderdate); 

create index  c_key 
on customer(c_mktsegment, c_custkey);

create index l_key 
on lineitem(l_orderkey,l_shipdate);

create index l_key2 
on lineitem(l_orderkey, l_commitdate, l_receiptdate);

create index p_key1
on part(p_size, p_type, p_partkey);

create index s_key1
on supplier(s_suppkey, s_nationkey);