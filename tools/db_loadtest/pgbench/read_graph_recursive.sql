\set root_uid random(:min_instance_uid, :max_instance_uid)

WITH RECURSIVE walk AS (
    SELECT
        gi.uid,
        0::int AS depth,
        ARRAY[gi.uid]::bigint[] AS path
    FROM generic_instance gi
    WHERE gi.uid = :root_uid
      AND gi.is_deleted = false

    UNION ALL

    SELECT
        child.uid,
        walk.depth + 1,
        walk.path || child.uid
    FROM walk
    JOIN generic_instance_lineage lin
      ON lin.parent_instance_uid = walk.uid
    JOIN generic_instance child
      ON child.uid = lin.child_instance_uid
    WHERE lin.is_deleted = false
      AND child.is_deleted = false
      AND walk.depth < :max_depth
      AND NOT (child.uid = ANY(walk.path))
)
SELECT
    COUNT(*) AS nodes_visited,
    COALESCE(MAX(depth), 0) AS max_depth_reached
FROM walk;
