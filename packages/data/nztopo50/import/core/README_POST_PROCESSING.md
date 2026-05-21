# Road Checks

When roads have been loaded it is useful to check what may have been missed. Typically due to the
LAMPS export and LDS import not aligning on FIDs or Null FIDs.

## Records in lookup but not in base

select count(_) from (
SELECT r._
FROM release66.road_line r
LEFT JOIN lookups.road_lkp l
ON r.t50_fid = l.t50_fid
WHERE l.t50_fid IS NULL);

## Records in lookup but not in base

select _ from (
SELECT l._
FROM lookups.road_lkp l
LEFT JOIN release66.road_line r
ON l.t50_fid = r.t50_fid
WHERE r.t50_fid IS NULL) where t50_fid is not null;
