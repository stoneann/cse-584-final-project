SET SESSION grouped_execution = true;
SET SESSION recoverable_grouped_execution = true;

SELECT t1.join_id, t2.id, t1.id
FROM "10_db".l_table t1
JOIN "10_db".r_table t2
ON t1.join_id = t2.join_id;
