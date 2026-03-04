\set root_uuid random(:min_instance_uuid, :max_instance_uuid)

WITH RECURSIVE walk AS (
    SELECT
        gi.uuid,
        0::int AS depth,
        ARRAY[gi.uuid]::bigint[] AS path
    FROM generic_instance gi
    WHERE gi.uuid = :root_uuid
      AND gi.is_deleted = false

    UNION ALL

    SELECT
        child.uuid,
        walk.depth + 1,
        walk.path || child.uuid
    FROM walk
    JOIN generic_instance_lineage lin
      ON lin.parent_instance_uuid = walk.uuid
    JOIN generic_instance child
      ON child.uuid = lin.child_instance_uuid
    WHERE lin.is_deleted = false
      AND child.is_deleted = false
      AND walk.depth < :max_depth
      AND NOT (child.uuid = ANY(walk.path))
)
SELECT
    COUNT(*) AS nodes_visited,
    COALESCE(MAX(depth), 0) AS max_depth_reached
FROM walk;
