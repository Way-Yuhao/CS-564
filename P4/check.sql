select
	name
from
	sqlite_master 
where
	type == 'index';

PRAGMA index_info(sqlite_autoindex_PARTSUPP_1);
PRAGMA index_info(sqlite_autoindex_LINEITEM_1);